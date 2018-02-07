{-# LANGUAGE CPP           #-}
{-# LANGUAGE TupleSections #-}

-- |
-- Module      : Database.RocksDB.Base
-- Copyright   : (c) 2012-2013 The leveldb-haskell Authors
--               (c) 2014 The rocksdb-haskell Authors
-- License     : BSD3
-- Maintainer  : mail@agrafix.net
-- Stability   : experimental
-- Portability : non-portable
--
-- RocksDB Haskell binding.
--
-- The API closely follows the C-API of RocksDB.
-- For more information, see: <http://agrafix.net>

module Database.RocksDB.Base
    ( -- * Exported Types
      DB
    , BatchOp (..)
    , Comparator (..)
    , Compression (..)
    , Options (..)
    , ReadOptions (..)
    , Snapshot
    , WriteBatch
    , WriteOptions (..)
    , Range

    -- * Defaults
    , defaultOptions
    , defaultReadOptions
    , defaultWriteOptions

    -- * Basic Database Manipulations
    , open
    , openBracket
    , createColumnFamily
    , close
    , put
    , putBinaryVal
    , putBinary
    , putCF
    , delete
    , write
    , get
    , getBinary
    , getBinaryVal
    , getCF
    , withSnapshot
    , withSnapshotBracket
    , createSnapshot
    , releaseSnapshot

    -- * Filter Policy / Bloom Filter
    , FilterPolicy (..)
    , BloomFilter
    , createBloomFilter
    , releaseBloomFilter
    , bloomFilter

    -- * Administrative Functions
    , Property (..), getProperty
    , destroy
    , repair
    , approximateSize

    -- * Iteration
    , module Database.RocksDB.Iterator

    -- * Column Families
    , columnFamilyDescriptor
    ) where

import           Control.Applicative          ((<$>))
import           Control.Exception            (bracket, bracketOnError, finally)
import           Control.Monad                (liftM, when)
import           Control.Concurrent.MVar

import           Control.Monad.IO.Class       (MonadIO (liftIO))
import           Control.Monad.Trans.Resource (MonadResource (..), ReleaseKey, allocate,
                                               release)
import           Control.Monad.Trans.Except   (ExceptT, throwE, runExceptT)
import           Control.Monad.Trans.Class    (lift)
import           Data.Binary                  (Binary)
import qualified Data.Binary                  as Binary
import           Data.ByteString              (ByteString)
import           Data.ByteString.Internal     (ByteString (..))
import qualified Data.ByteString.Lazy         as BSL
import           Foreign
import           Foreign.C.String             (CString, withCString, newCString)
import           System.Directory             (createDirectoryIfMissing)

import           Database.RocksDB.C
import           Database.RocksDB.Internal
import           Database.RocksDB.Iterator
import           Database.RocksDB.Types

import qualified Data.HashMap.Strict          as HM
import qualified Data.ByteString              as BS
import qualified Data.ByteString.Unsafe       as BU

import qualified GHC.Foreign                  as GHC
import qualified GHC.IO.Encoding              as GHC

-- | Create a 'BloomFilter'
bloomFilter :: MonadResource m => Int -> m BloomFilter
bloomFilter i =
    snd <$> allocate (createBloomFilter i)
                      releaseBloomFilter

-- | Open a database
--
-- The returned handle will automatically be released when the enclosing
-- 'runResourceT' terminates.
openBracket :: MonadResource m => FilePath -> Options -> [ColumnFamilyDescriptor] -> m (ReleaseKey, DB)
openBracket path opts cfs = allocate (open path opts cfs) close
{-# INLINE openBracket #-}

-- | Run an action with a snapshot of the database.
--
-- The snapshot will be released when the action terminates or throws an
-- exception. Note that this function is provided for convenience and does not
-- prevent the 'Snapshot' handle to escape. It will, however, be invalid after
-- this function returns and should not be used anymore.
withSnapshotBracket :: MonadResource m => DB -> (Snapshot -> m a) -> m a
withSnapshotBracket db f = do
    (rk, snap) <- createSnapshotBracket db
    res <- f snap
    release rk
    return res

-- | Create a snapshot of the database.
--
-- The returned 'Snapshot' will be released automatically when the enclosing
-- 'runResourceT' terminates. It is recommended to use 'createSnapshot'' instead
-- and release the resource manually as soon as possible.
-- Can be released early.
createSnapshotBracket :: MonadResource m => DB -> m (ReleaseKey, Snapshot)
createSnapshotBracket db = allocate (createSnapshot db) (releaseSnapshot db)

-- | Open a database.
--
-- The returned handle should be released with 'close'.
open :: MonadIO m => FilePath -> Options -> [ColumnFamilyDescriptor] -> m DB
open path opts cfDescriptors = liftIO $ bracketOnError initialize finalize mkDB
    where
# ifdef mingw32_HOST_OS
        initialize =
            (, ()) <$> mkOpts opts
        finalize (opts', ()) =
            freeOpts opts'
# else
        initialize = do
            opts' <- mkOpts opts
            -- With LC_ALL=C, two things happen:
            --   * rocksdb can't open a database with unicode in path;
            --   * rocksdb can't create a folder properly.
            -- So, we create the folder by ourselves, and for thart we
            -- need to set the encoding we're going to use. On Linux
            -- it's almost always UTC-8.
            oldenc <- GHC.getFileSystemEncoding
            when (createIfMissing opts) $
                GHC.setFileSystemEncoding GHC.utf8
            cfArgs' <- mkColumnFamilyArgs cfDescriptors
            pure (opts', cfArgs', oldenc)
        finalize (opts', cfArgs', oldenc) = do
            freeOpts opts'
            freeColumnFamilyArgs cfArgs'
            GHC.setFileSystemEncoding oldenc
# endif
        mkDB (opts'@(Options' opts_ptr _ _), args@(ColumnFamilyArgs cfNames cfOpts cfPtrs cfLen), _) = do
            when (createIfMissing opts) $
                createDirectoryIfMissing True path
            withFilePath path $ \path_ptr -> do
                dbHandle <- throwIfErr "open" $
                  c_rocksdb_open_column_families
                    opts_ptr
                    path_ptr
                    (intToCInt cfLen)
                    cfNames
                    cfOpts
                    cfPtrs

                cfs <- getColumnFamilyHandles args
                isOpen <- newMVar True
                return $ DB dbHandle opts' cfs isOpen

-- | Close a database.
--
-- The handle will be invalid after calling this action and should no
-- longer be used.
close :: MonadIO m => DB -> m ()
close (DB db_ptr opts_ptr cfs openMV) = liftIO $ do
    isOpen <- takeMVar openMV
    when isOpen $ do
        mapM_ (c_rocksdb_column_family_handle_destroy . _cfPtr) . HM.elems . _cfHandles $ cfs
        c_rocksdb_close db_ptr `finally` freeOpts opts_ptr
    putMVar openMV False

createColumnFamily :: MonadIO m => DB -> ColumnFamilyDescriptor -> m DB
createColumnFamily db@(DB db_ptr dbOpts (ColumnFamilies' cfs) isOpen) (ColumnFamilyDescriptor name opts) =
    withDB db . liftIO . bracketOnError initialize finalize $ \(opts'@(Options' opts_ptr _ _), name') -> do
        cfHandle <- throwIfErr "create_cf" $ c_rocksdb_create_column_family db_ptr opts_ptr name'
        let cf = ColumnFamily' cfHandle name opts'
        return db { _dbColumnFamilies = ColumnFamilies' $ HM.insert name cf cfs }
  where
    initialize = do
      opts' <- mkOpts opts
      name' <- newCString name
      return (opts', name')
    finalize (opts', name') = do
      freeOpts opts'
      free name'

-- | Run an action with a 'Snapshot' of the database.
withSnapshot :: MonadIO m => DB -> (Snapshot -> IO a) -> m a
withSnapshot db act = liftIO $
    bracket (createSnapshot db) (releaseSnapshot db) act

-- | Create a snapshot of the database.
--
-- The returned 'Snapshot' should be released with 'releaseSnapshot'.
createSnapshot :: MonadIO m => DB -> m Snapshot
createSnapshot db@(DB db_ptr _ _ _) = withDB db . liftIO $
    Snapshot <$> c_rocksdb_create_snapshot db_ptr

-- | Release a snapshot.
--
-- The handle will be invalid after calling this action and should no
-- longer be used.
releaseSnapshot :: MonadIO m => DB -> Snapshot -> m ()
releaseSnapshot db@(DB db_ptr _ _ _) (Snapshot snap) = withDB db . liftIO $
    c_rocksdb_release_snapshot db_ptr snap

-- | Get a DB property.
getProperty :: MonadIO m => DB -> Property -> m (Maybe ByteString)
getProperty db@(DB db_ptr _ _ _) p = withDB db . liftIO $
    withCString (prop p) $ \prop_ptr -> do
        val_ptr <- c_rocksdb_property_value db_ptr prop_ptr
        if val_ptr == nullPtr
            then return Nothing
            else do res <- Just <$> BS.packCString val_ptr
                    freeCString val_ptr
                    return res
    where
        prop (NumFilesAtLevel i) = "rocksdb.num-files-at-level" ++ show i
        prop Stats               = "rocksdb.stats"
        prop SSTables            = "rocksdb.sstables"

-- | Destroy the given RocksDB database.
destroy :: MonadIO m => FilePath -> Options -> m ()
destroy path opts = liftIO $ bracket (mkOpts opts) freeOpts destroy'
    where
        destroy' (Options' opts_ptr _ _) =
            withFilePath path $ \path_ptr ->
                throwIfErr "destroy" $ c_rocksdb_destroy_db opts_ptr path_ptr

-- | Repair the given RocksDB database.
repair :: MonadIO m => FilePath -> Options -> m ()
repair path opts = liftIO $ bracket (mkOpts opts) freeOpts repair'
    where
        repair' (Options' opts_ptr _ _) =
            withFilePath path $ \path_ptr ->
                throwIfErr "repair" $ c_rocksdb_repair_db opts_ptr path_ptr


-- TODO: support [Range], like C API does
type Range  = (ByteString, ByteString)

-- | Inspect the approximate sizes of the different levels.
approximateSize :: MonadIO m => DB -> Range -> m Int64
approximateSize db@(DB db_ptr _ _ _) (from, to) = withDB db . liftIO $
    BU.unsafeUseAsCStringLen from $ \(from_ptr, flen) ->
    BU.unsafeUseAsCStringLen to   $ \(to_ptr, tlen)   ->
    withArray [from_ptr]          $ \from_ptrs        ->
    withArray [intToCSize flen]   $ \flen_ptrs        ->
    withArray [to_ptr]            $ \to_ptrs          ->
    withArray [intToCSize tlen]   $ \tlen_ptrs        ->
    allocaArray 1                 $ \size_ptrs        -> do
        c_rocksdb_approximate_sizes db_ptr 1
                                    from_ptrs flen_ptrs
                                    to_ptrs tlen_ptrs
                                    size_ptrs
        liftM head $ peekArray 1 size_ptrs >>= mapM toInt64

    where
        toInt64 = return . fromIntegral

putBinaryVal :: (MonadIO m, Binary v) => DB -> WriteOptions -> ByteString -> v -> m ()
putBinaryVal db wopts key val = put db wopts key (binaryToBS val)

putBinary :: (MonadIO m, Binary k, Binary v) => DB -> WriteOptions -> k -> v -> m ()
putBinary db wopts key val = put db wopts (binaryToBS key) (binaryToBS val)

-- | Write a key/value pair to the default column family.
put :: MonadIO m => DB -> WriteOptions -> ByteString -> ByteString -> m ()
put db@(DB db_ptr _ _ _) opts key value = withDB db . liftIO . withCWriteOpts opts $ \opts_ptr ->
    BU.unsafeUseAsCStringLen key   $ \(key_ptr, klen) ->
    BU.unsafeUseAsCStringLen value $ \(val_ptr, vlen) ->
        throwIfErr "put"
            $ c_rocksdb_put db_ptr opts_ptr
                            key_ptr (intToCSize klen)
                            val_ptr (intToCSize vlen)

-- | Write a key/value pair to a non-default column family.
putCF :: MonadIO m => DB -> String -> WriteOptions -> ByteString -> ByteString -> m (Either RocksDBError ())
putCF db@(DB db_ptr _ _ _) cf opts key val = withDB db . liftIO . runExceptT $ do
    ColumnFamily' cf_ptr _ _ <- lookupCF db cf
    lift . BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
        BU.unsafeUseAsCStringLen val $ \(val_ptr, vlen) ->
            withCWriteOpts opts $ \opts_ptr -> do
      throwIfErr "put"
        $ c_rocksdb_put_cf db_ptr opts_ptr cf_ptr
          key_ptr (intToCSize klen)
          val_ptr (intToCSize vlen)


getBinaryVal :: (Binary v, MonadIO m) => DB -> ReadOptions -> ByteString -> m (Maybe v)
getBinaryVal db ropts key  = fmap bsToBinary <$> get db ropts key

getBinary :: (MonadIO m, Binary k, Binary v) => DB -> ReadOptions -> k -> m (Maybe v)
getBinary db ropts key = fmap bsToBinary <$> get db ropts (binaryToBS key)

-- | Read a value by key.
get :: MonadIO m => DB -> ReadOptions -> ByteString -> m (Maybe ByteString)
get db@(DB db_ptr _ _ _) opts key = withDB db . liftIO $ withCReadOpts opts $ \opts_ptr ->
    BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
    alloca                       $ \vlen_ptr -> do
        val_ptr <- throwIfErr "get" $
            c_rocksdb_get db_ptr opts_ptr key_ptr (intToCSize klen) vlen_ptr
        vlen <- peek vlen_ptr
        if val_ptr == nullPtr
            then return Nothing
            else do
                res' <- Just <$> BS.packCStringLen (val_ptr, cSizeToInt vlen)
                freeCString val_ptr
                return res'

getCF :: MonadIO m
      => DB
      -> String
      -> ReadOptions
      -> ByteString
      -> m (Either RocksDBError (Maybe ByteString))
getCF db@(DB db_ptr _ _ _) cf opts key = withDB db . liftIO . runExceptT $ do
    ColumnFamily' cf_ptr _ _ <- lookupCF db cf
    lift . BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
      withCReadOpts opts $ \opts_ptr ->
                             alloca $ \vlen_ptr -> do
      val_ptr <- throwIfErr "get" $
                 c_rocksdb_get_cf db_ptr opts_ptr cf_ptr key_ptr (intToCSize klen) vlen_ptr
      vlen <- peek vlen_ptr
      if val_ptr == nullPtr
        then return Nothing
        else do
            res' <- Just <$> BS.packCStringLen (val_ptr, cSizeToInt vlen)
            freeCString val_ptr
            return res'

-- | Delete a key/value pair.
delete :: MonadIO m => DB -> WriteOptions -> ByteString -> m ()
delete db@(DB db_ptr _ _ _) opts key = withDB db . liftIO . withCWriteOpts opts $ \opts_ptr ->
    BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
        throwIfErr "delete"
            $ c_rocksdb_delete db_ptr opts_ptr key_ptr (intToCSize klen)

-- | Perform a batch mutation.
write :: MonadIO m => DB -> WriteOptions -> WriteBatch -> m (Either RocksDBError ())
write db@(DB db_ptr _ _ _) opts batch = withDB db . liftIO $ withCWriteOpts opts $ \opts_ptr ->
    bracket c_rocksdb_writebatch_create c_rocksdb_writebatch_destroy $ \batch_ptr -> runExceptT $ do

    mapM_ (batchAdd batch_ptr) batch

    liftIO . throwIfErr "write" $ c_rocksdb_write db_ptr opts_ptr batch_ptr

    -- ensure @ByteString@s (and respective shared @CStringLen@s) aren't GC'ed
    -- until here
    mapM_ (liftIO . touch) batch

    where
        batchAdd batch_ptr (Put cfName key val) = do
          ColumnFamily' cf_ptr _ _ <- lookupCF db cfName
          liftIO . BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
              BU.unsafeUseAsCStringLen val $ \(val_ptr, vlen) ->
                c_rocksdb_writebatch_put_cf
                  batch_ptr
                  cf_ptr
                  key_ptr (intToCSize klen)
                  val_ptr (intToCSize vlen)

        batchAdd batch_ptr (Del cfName key) = do
          ColumnFamily' cf_ptr _ _ <- lookupCF db cfName
          liftIO . BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
            c_rocksdb_writebatch_delete_cf batch_ptr cf_ptr key_ptr (intToCSize klen)

        touch (Put _ (PS p _ _) (PS p' _ _)) = do
            touchForeignPtr p
            touchForeignPtr p'

        touch (Del _ (PS p _ _)) = touchForeignPtr p

createBloomFilter :: MonadIO m => Int -> m BloomFilter
createBloomFilter i = do
    let i' = fromInteger . toInteger $ i
    fp_ptr <- liftIO $ c_rocksdb_filterpolicy_create_bloom i'
    return $ BloomFilter fp_ptr

releaseBloomFilter :: MonadIO m => BloomFilter -> m ()
releaseBloomFilter (BloomFilter fp) = liftIO $ c_rocksdb_filterpolicy_destroy fp

binaryToBS :: Binary v => v -> ByteString
binaryToBS x = BSL.toStrict (Binary.encode x)

bsToBinary :: Binary v => ByteString -> v
bsToBinary x = Binary.decode (BSL.fromStrict x)

-- | Marshal a 'FilePath' (Haskell string) into a `NUL` terminated C string using
-- temporary storage.
-- On Linux, UTF-8 is almost always the encoding used.
-- When on Windows, UTF-8 can also be used, although the default for those devices is
-- UTF-16. For a more detailed explanation, please refer to
-- https://msdn.microsoft.com/en-us/library/windows/desktop/dd374081(v=vs.85).aspx.
withFilePath :: FilePath -> (CString -> IO a) -> IO a
withFilePath = GHC.withCString GHC.utf8

lookupCF :: MonadIO m => DB -> String -> ExceptT RocksDBError m ColumnFamily'
lookupCF db@(DB _ _ cfs _) cf = withDB db
                              . maybe (throwE $ NoSuchColumnFamily cf) return
                              . HM.lookup cf
                              . _cfHandles
                              $ cfs
