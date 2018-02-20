{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE BinaryLiterals      #-}
{-# LANGUAGE OverloadedStrings   #-}

module Main where

import           Control.Monad.IO.Class       (MonadIO (liftIO))
import           Control.Monad.Trans.Resource (MonadResource, runResourceT)
import           Data.Default                 (def)
import           System.IO.Temp               (withSystemTempDirectory)
import           Control.Exception
import           Control.Concurrent

import           Database.RocksDB             ( iterItems
                                              , iterSeek
                                              , withIterator
                                              , prefixExtractor
                                              , comparator
                                              , prefixSameAsStart
                                              , createColumnFamily
                                              , getCF
                                              , putCF
                                              , Compression (..)
                                              , DB
                                              , compression
                                              , createIfMissing
                                              , get
                                              , open
                                              , close
                                              , put
                                              , columnFamilyDescriptor
                                              , Comparator(..)
                                              )
import           Database.RocksDB.Types       ( SliceTransform(..) )
import qualified Data.ByteString.Char8     as BSC


import           Test.Hspec
import           Test.QuickCheck              (Arbitrary (..), UnicodeString (..),
                                               generate)
import Debug.Trace

initializeDB :: MonadResource m => FilePath -> m DB
initializeDB path = do
    dbWithoutCFs <- open path def { createIfMissing = True , compression = NoCompression } []
    createColumnFamily dbWithoutCFs $ columnFamilyDescriptor "other"

main :: IO ()
main = hspec $ do

  describe "Basic DB Functionality" $ do
    it "should put items into the database and retrieve them" $  do
      runResourceT $ withSystemTempDirectory "rocksdb" $ \path -> do
        db <- initializeDB path
        put db def "zzz" "zzz"
        get db def "zzz"
      `shouldReturn` (Just "zzz")

    it "should put items into column families and retrieve them" $ do
      runResourceT $ withSystemTempDirectory "rocksdb2" $ \path -> do
        db <- initializeDB path
        liftIO $ putStrLn "opened"
        res <- putCF db "other" def "foo" "bar"
        return $ either throw (const ()) res
        _ <- either throw id <$> putCF db "other" def "foo" "bar"
        v <- either throw id <$> getCF db "other" def "foo"
        return v
      `shouldReturn` (Just "bar")

    it "supports both default and non-default column families" $ do
      runResourceT $ withSystemTempDirectory "rocksdb3" $ \path -> do
        db <- initializeDB path
        put db def "foo" "zzz"
        _ <- either throw id <$> putCF db "other" def "foo" "bar"
        Right v1 <- getCF db "other" def "foo"
        v2 <- get db def "foo"
        return (v1, v2)
      `shouldReturn` (Just "bar", Just "zzz")

    it "should put items into a database whose filepath has unicode characters and\
       \ retrieve them" $  do
      runResourceT $ withSystemTempDirectory "rocksdb" $ \path -> do
        unicode <- getUnicodeString <$> liftIO (generate arbitrary)
        db <- initializeDB $ path ++ unicode
        put db def "zzz" "zzz"
        get db def "zzz"
      `shouldReturn` (Just "zzz")

  describe "running in a spark" $ do
    it "allows open/close from a spark" $ do
      done <- newEmptyMVar
      _ <- forkOn 2 . runResourceT . withSystemTempDirectory "rocksdb4" $ \path -> do
        db <- initializeDB path
        put db def "zzz" "zzz"
        _ <- get db def "zzz"
        close db
        liftIO $ putMVar done ()
      takeMVar done

  describe "prefix iteration" $ do
    context "with a constant-length PrefixExtractor" $
      it "supports iterating the prefix" $ do
        runResourceT $ withSystemTempDirectory "rocksdb5" $ \path -> do
            let prefixExtractor = Just $ FixedPrefixSliceTransform 5
                options = def { prefixExtractor }
            db <- open path options { createIfMissing = True, compression = NoCompression } []
            put db def "aaa123" "123"
            put db def "aaa456" "456"
            put db def "bbb789" "789"
            put db def "bbb012" "012"

            withIterator db def { prefixSameAsStart = True } $ \iter -> do
              iterSeek iter "bbb"
              iterItems iter
        `shouldReturn` [("bbb789", "789"), ("bbb012", "012")]

    -- context "with a dynamic PrefixExtractor" $
    --   it "supports iterating the prefix" $ do
    --     runResourceT $ withSystemTempDirectory "rocksdb6" $ \path -> do
    --         let getPrefix = BSC.takeWhile (`elem` ['a'..'z']) . traceShowId
    --             prefixExtractor = Just $
    --               SliceTransformFun { stName      = "lowercase letters"
    --                                 , stInDomain  = const True
    --                                 , stInRange   = const True
    --                                 , stTransform = getPrefix . traceShowId
    --                                 }
    --             comparator = Just . Comparator $ \a b -> getPrefix b `compare` getPrefix a
    --             options = def { prefixExtractor, comparator }
    --         db <- open path options { createIfMissing = True, compression = NoCompression } []
    --         put db def "a123" "123"
    --         put db def "a456" "456"
    --         put db def "aa456" "456"
    --         put db def "aa789" "789"
    --         put db def "b123" "123"
    --         put db def "bb456" "456"
    --         put db def "bbb789" "456"

    --         r1 <- withIterator db def { prefixSameAsStart = True } $ \iter -> do
    --           iterSeek iter "a123"
    --           iterItems iter

    --         r2 <- withIterator db def { prefixSameAsStart = True } $ \iter -> do
    --           iterSeek iter "aa"
    --           iterItems iter

    --         r3 <- withIterator db def { prefixSameAsStart = True } $ \iter -> do
    --           iterSeek iter "bb"
    --           iterItems iter

    --         return (r1, r2, r3)
    --     `shouldReturn` ( [("a123",  "123"), ("a456",  "456")]
    --                    , [("aa456", "456"), ("aa789", "aa789")]
    --                    , [("bb456", "456")]
    --                    )
