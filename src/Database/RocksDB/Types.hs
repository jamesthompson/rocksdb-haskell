-- |
-- Module      : Database.RocksDB.Types
-- Copyright   : (c) 2012-2013 The leveldb-haskell Authors
--               (c) 2014 The rocksdb-haskell Authors
-- License     : BSD3
-- Maintainer  : mail@agrafix.net
-- Stability   : experimental
-- Portability : non-portable
--

module Database.RocksDB.Types
    ( BatchOp (..)
    , BloomFilter (..)
    , SliceTransform(..)
    , Comparator (..)
    , Compression (..)
    , ColumnFamilyDescriptor (..)
    , FilterPolicy (..)
    , Options (..)
    , Property (..)
    , ReadOptions (..)
    , RocksDBError (..)
    , Snapshot (..)
    , WriteBatch
    , WriteOptions (..)

    , defaultOptions
    , defaultReadOptions
    , defaultWriteOptions
    , columnFamilyDescriptor
    )
where

import           Data.ByteString    (ByteString)
import           Data.Default
import           Foreign
import           Control.Exception

import           Database.RocksDB.C


-- | Snapshot handle
newtype Snapshot = Snapshot SnapshotPtr deriving (Eq)

-- | Compression setting
data Compression
    = NoCompression
    | SnappyCompression
    | ZlibCompression
    deriving (Eq, Show)

-- | User-defined comparator
newtype Comparator = Comparator (ByteString -> ByteString -> Ordering)

-- | User-defined filter policy
data FilterPolicy = FilterPolicy
    { fpName       :: String
    , createFilter :: [ByteString] -> ByteString
    , keyMayMatch  :: ByteString -> ByteString -> Bool
    }

-- | Represents the built-in Bloom Filter
newtype BloomFilter = BloomFilter FilterPolicyPtr

-- | User-defined Slice Transform function
data SliceTransform
  = SliceTransformFun
    { stName      :: String
    , stInDomain  :: ByteString -> Bool
    , stInRange   :: ByteString -> Bool
    , stTransform :: ByteString -> ByteString
    }
  | FixedPrefixSliceTransform Int
  | NoOpSliceTransform

-- | Options when opening a database
data Options = Options
    { comparator      :: !(Maybe Comparator)
      -- ^ Comparator used to defined the order of keys in the table.
      --
      -- If 'Nothing', the default comparator is used, which uses lexicographic
      -- bytes-wise ordering.
      --
      -- NOTE: the client must ensure that the comparator supplied here has the
      -- same name and orders keys /exactly/ the same as the comparator provided
      -- to previous open calls on the same DB.
      --
      -- Default: Nothing
    , compression     :: !Compression
      -- ^ Compress blocks using the specified compression algorithm.
      --
      -- This parameter can be changed dynamically.
      --
      -- Default: 'EnableCompression'
    , createIfMissing :: !Bool
      -- ^ If true, the database will be created if it is missing.
      --
      -- Default: False
    , errorIfExists   :: !Bool
      -- ^ It true, an error is raised if the database already exists.
      --
      -- Default: False
    , maxOpenFiles    :: !Int
      -- ^ Number of open files that can be used by the DB.
      --
      -- You may need to increase this if your database has a large working set
      -- (budget one open file per 2MB of working set).
      --
      -- Default: 1000
    , paranoidChecks  :: !Bool
      -- ^ If true, the implementation will do aggressive checking of the data
      -- it is processing and will stop early if it detects any errors.
      --
      -- This may have unforeseen ramifications: for example, a corruption of
      -- one DB entry may cause a large number of entries to become unreadable
      -- or for the entire DB to become unopenable.
      --
      -- Default: False
    , writeBufferSize :: !Int
      -- ^ Amount of data to build up in memory (backed by an unsorted log on
      -- disk) before converting to a sorted on-disk file.
      --
      -- Larger values increase performance, especially during bulk loads. Up to
      -- to write buffers may be held in memory at the same time, so you may
      -- with to adjust this parameter to control memory usage. Also, a larger
      -- write buffer will result in a longer recovery time the next time the
      -- database is opened.
      --
      -- Default: 4MB
    , prefixExtractor :: !(Maybe SliceTransform)
      -- ^ If non-'Nothing', use the specified function to determine the
      -- prefixes for keys.  These prefixes will be placed in the filter.
      -- Depending on the workload, this can reduce the number of read-IOP
      -- cost for scans when a prefix is passed via ReadOptions to
      -- 'newIterator'. For prefix filtering to work properly,
      -- "prefix_extractor" and "comparator" must be such that the following
      -- properties hold:
      --
      -- 1) key.starts_with(prefix(key))
      -- 2) Compare(prefix(key), key) <= 0.
      -- 3) If Compare(k1, k2) <= 0, then Compare(prefix(k1), prefix(k2)) <= 0
      -- 4) prefix(prefix(key)) == prefix(key)
      --
      -- Default: Nothing
    }

defaultOptions :: Options
defaultOptions = Options
    { comparator           = Nothing
    , compression          = SnappyCompression
    , createIfMissing      = False
    , errorIfExists        = False
    , maxOpenFiles         = 1000
    , paranoidChecks       = False
    , writeBufferSize      = 4 `shift` 20
    , prefixExtractor      = Nothing
    }

instance Default Options where
    def = defaultOptions

-- | Options for write operations
data WriteOptions = WriteOptions
    { sync :: !Bool
      -- ^ If true, the write will be flushed from the operating system buffer
      -- cache (by calling WritableFile::Sync()) before the write is considered
      -- complete. If this flag is true, writes will be slower.
      --
      -- If this flag is false, and the machine crashes, some recent writes may
      -- be lost. Note that if it is just the process that crashes (i.e., the
      -- machine does not reboot), no writes will be lost even if sync==false.
      --
      -- In other words, a DB write with sync==false has similar crash semantics
      -- as the "write()" system call. A DB write with sync==true has similar
      -- crash semantics to a "write()" system call followed by "fsync()".
      --
      -- Default: False
    } deriving (Eq, Show)

defaultWriteOptions :: WriteOptions
defaultWriteOptions = WriteOptions { sync = False }

instance Default WriteOptions where
    def = defaultWriteOptions

-- | Options for read operations
data ReadOptions = ReadOptions
    { verifyCheckSums :: !Bool
      -- ^ If true, all data read from underlying storage will be verified
      -- against corresponding checksuyms.
      --
      -- Default: False
    , fillCache       :: !Bool
      -- ^ Should the data read for this iteration be cached in memory? Callers
      -- may with to set this field to false for bulk scans.
      --
      -- Default: True
    , useSnapshot     :: !(Maybe Snapshot)
      -- ^ If 'Just', read as of the supplied snapshot (which must belong to the
      -- DB that is being read and which must not have been released). If
      -- 'Nothing', use an implicit snapshot of the state at the beginning of
      -- this read operation.
      --
      -- Default: Nothing
    , prefixSameAsStart :: !Bool
      -- ^ If 'True', iterators will only iterate over the same prefix as their
      -- start key. Only valid if a 'prefixExtractor' is set
    } deriving (Eq)

defaultReadOptions :: ReadOptions
defaultReadOptions = ReadOptions
    { verifyCheckSums   = False
    , fillCache         = True
    , useSnapshot       = Nothing
    , prefixSameAsStart = False
    }

instance Default ReadOptions where
    def = defaultReadOptions

type WriteBatch = [BatchOp]

-- | Batch operation
data BatchOp = Put String ByteString ByteString | Del String ByteString
    deriving (Eq, Show)

-- | Properties exposed by RocksDB
data Property = NumFilesAtLevel Int | Stats | SSTables
    deriving (Eq, Show)

data ColumnFamilyDescriptor = ColumnFamilyDescriptor
    { columnFamilyName    :: !String
      -- ^ The name of the column family
    , columnFamilyOptions :: !Options}

-- | Create a 'ColumnFamilyDescriptor' with the given name and default options
columnFamilyDescriptor :: String -> ColumnFamilyDescriptor
columnFamilyDescriptor = flip ColumnFamilyDescriptor def

instance Default ColumnFamilyDescriptor where
  def = columnFamilyDescriptor "default"

data RocksDBError = NoSuchColumnFamily String
                  | StringError String
                  deriving Show

instance Exception RocksDBError
