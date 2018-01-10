{-# LANGUAGE BinaryLiterals      #-}
{-# LANGUAGE OverloadedStrings   #-}

module Main where

import           Control.Monad.IO.Class       (MonadIO (liftIO))
import           Control.Monad.Trans.Resource (MonadResource, runResourceT)
import           Data.Default                 (def)
import           System.IO.Temp               (withSystemTempDirectory)
import           Control.Exception

import           Database.RocksDB             ( createColumnFamily
                                              , getCF
                                              , putCF
                                              , Compression (..)
                                              , DB
                                              , compression
                                              , createIfMissing
                                              , get
                                              , open
                                              , put
                                              , columnFamilyDescriptor
                                              )



import           Test.Hspec                   (describe, hspec, it, shouldReturn)
import           Test.QuickCheck              (Arbitrary (..), UnicodeString (..),
                                               generate)

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
