module Main where

import Data.Maybe
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.ByteString.Lazy as BL
import Data.ByteString.Builder
import System.IO

import Network.Nats
import Network.Nats.Message

main :: IO ()
main = withNats $ \nats ->
  subscribe nats ">" Nothing $ \MSG{subject, replyTo, payload} -> do
    C8.putStrLn $ toLazyByteString $ mconcat
      [ "PUB "
      , byteString subject
      , " "
      , byteString (fromMaybe "" replyTo)
      , " "
      , word32Dec (fromIntegral $ BL.length payload)
      , "\r\n"
      , lazyByteString payload
      , "\r\n"
      ]
    hFlush stdout
