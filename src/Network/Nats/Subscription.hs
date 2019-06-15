module Network.Nats.Subscription
  ( Sub(..)
  , newSub
  , setUnsubAfter
  , nextMsg
  , addMsg
  ) where

import Data.Atomics
import Data.Atomics.Counter
import Data.IORef
import Control.Concurrent.Chan.Unagi
import Network.Nats.Message (SID, Message)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BS

type Subject = BS.ByteString
type ReplyTo = BS.ByteString

type Callback =
  SID -> Subject -> BL.ByteString -> ReplyTo -> IO ()

data Sub = Sub { subject :: !Subject
               , unsubAfter :: !(Maybe AtomicCounter)
               , cb :: !(Maybe Callback)
               , inChan :: !(InChan Message)
               , outChan :: !(OutChan Message)
               }

newSub :: Subject -> Maybe Callback -> IO Sub
newSub subj callback = do
  (inC, outC) <- newChan
  return Sub { subject = subj
             , unsubAfter = Nothing
             , cb = callback
             , inChan = inC
             , outChan = outC
             }

setUnsubAfter :: IORef Sub -> Int -> IO ()
setUnsubAfter subRef n = do
  unsubAfter' <- newCounter n
  atomicModifyIORefCAS_ subRef (\sub' -> sub'{unsubAfter=Just unsubAfter'})

nextMsg :: Sub -> IO Message
nextMsg sub = readChan (outChan sub)

addMsg :: Sub -> Message -> IO Bool
addMsg Sub{inChan, unsubAfter} msg = do
  writeChan inChan msg
  case unsubAfter of
    Nothing -> return False
    Just unsubAfter' -> do
      remaining <- incrCounter (-1) unsubAfter'
      return (remaining == 0)
