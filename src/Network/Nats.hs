module Network.Nats
  ( withNats
  , pub
  , sub
  , unsub
  , getMsg
  , subscribe
  , publish
  , request
  , nextMessage
  , Nats
  , UnexpectedMessageError
  ) where

import System.IO
import System.Environment (lookupEnv)
import Control.Exception.Safe
import Control.Monad (when, forever)
import Data.Atomics.Counter
import Data.Atomics
import Data.IORef
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as Map
import Data.Map.Strict (Map)
import qualified Network.Socket as S
import Data.UUID
import System.Random (randomIO)
import Control.Concurrent.Chan.Unagi
import Control.Concurrent.Async
import Data.ByteString.Builder
import Data.Maybe (fromMaybe)

import qualified Network.Nats.Message as Msg
import Network.Nats.Message (SID, Message(..))
import Network.Nats.Subscription (Sub, addMsg, newSub, nextMsg, setUnsubAfter)

connectToServer :: String -> Int -> IO Handle
connectToServer host' port' = do
  addr:_ <- S.getAddrInfo (Just hints) (Just host') (Just $ show port')
  bracketOnError (open addr) S.close (connect' addr)
  where
    hints = S.defaultHints { S.addrSocketType = S.Stream }
    open addr =
      S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
    connect' :: S.AddrInfo -> S.Socket -> IO Handle
    connect' addr sock = do
      S.setSocketOption sock S.KeepAlive 1
      S.setSocketOption sock S.NoDelay 1
      S.connect sock (S.addrAddress addr)
      h <- S.socketToHandle sock ReadWriteMode
      hSetBuffering h (BlockBuffering Nothing)
      hSetBinaryMode h True
      return h

data Nats = Nats { h :: Handle
                 , subs :: IORef (Map SID (IORef Sub))
                 , nextSid :: AtomicCounter
                 , inChan :: InChan Builder
                 , outChan :: OutChan Builder
                 }

withNats :: (Nats -> IO a) -> IO a
withNats f = do
  mbNatsHost <- lookupEnv "NATS_HOST"
  let natsHost = fromMaybe "nats" mbNatsHost
  putStrLn $ "Connecting to NATS on " ++ natsHost ++ ":4222"
  bracket (connectToServer natsHost 4222) hClose $ \h -> do
    line <- BS.hGetLine h
    _info <- Msg.decode line
    BS.hPut h "CONNECT {\"verbose\": false, \"pedantic\": false, \"tls_required\": false, \"protocol\": 0}\r\n"
    hFlush h

    (inChan, outChan) <- newChan
    subs <- newIORef (Map.empty :: Map SID (IORef Sub))
    nextSid <- newCounter 0
    let nats = Nats { h = h
                    , subs = subs
                    , nextSid = nextSid
                    , inChan = inChan
                    , outChan = outChan
                    }

    let receive =
          forever $ do
            msg@MSG{sid} <- getMsg nats
            ss <- readIORef subs
            case Map.lookup sid ss of
              Nothing -> return ()
              Just subRef -> do
                sub' <- readIORef subRef
                done <- addMsg sub' msg
                when done (rmsub nats sid)
        send =
          forever $ do
            msg <- readChan outChan
            hPutBuilder h msg
            hFlush h

    res <- race (f nats) (race_ receive send)

    case res of
      Left x -> return x
      Right () -> error "should never happen"


type Subject = BS.ByteString
type ReplyTo = BS.ByteString

pub :: Nats -> Subject -> Maybe ReplyTo -> BL.ByteString -> IO ()
pub Nats{inChan} subject replyTo msg =
  writeChan inChan msg'
  where msg' = mconcat
          [ "PUB "
          , byteString subject
          , " "
          , byteString (fromMaybe "" replyTo)
          , " "
          , word32Dec (fromIntegral $ BL.length msg)
          , "\r\n"
          , lazyByteString msg
          , "\r\n"
          ]

publish :: Nats -> Subject -> BL.ByteString -> IO ()
publish nats subject msg = pub nats subject Nothing msg

request :: Nats -> Subject -> BL.ByteString -> IO BL.ByteString
request nats subject msg = do
  uuid <- randomIO
  let replyTo = BS.concat ["_INBOX.", toASCIIBytes uuid]
  (sid, _) <- sub nats replyTo Nothing
  unsub nats sid (Just 1)
  pub nats subject (Just replyTo) msg
  reply <- nextMessage nats sid
  return (payload reply)

nextMessage :: Nats -> SID -> IO Message
nextMessage Nats{subs} sid = do
  ss <- readIORef subs
  case Map.lookup sid ss of
    Nothing -> throw (UnknownSubscriptionError sid)
    Just subRef -> do
      sub' <- readIORef subRef
      nextMsg sub'

type QueueGroup = BS.ByteString

newSid :: Nats -> IO SID
newSid Nats{nextSid} = do
  sid <- incrCounter 1 nextSid
  return (fromIntegral sid)

sub :: Nats -> Subject -> Maybe QueueGroup -> IO (SID, Sub)
sub nats@Nats{subs, inChan} subject queueGroup = do
  sid <- newSid nats
  sub' <- newSub subject Nothing
  subRef <- newIORef sub'
  atomicModifyIORefCAS_ subs (Map.insert sid subRef)
  let msg' = mconcat
        [ "SUB "
        , byteString subject
        , " "
        , byteString (fromMaybe "" queueGroup)
        , " "
        , word64Dec sid
        , "\r\n"
        ]
  writeChan inChan msg'
  return (sid, sub')

subscribe :: Nats
          -> Subject
          -> Maybe QueueGroup
          -> (Message -> IO ())
          -> IO ()
subscribe nats subject queueGroup cb = do
  (sid, _) <- sub nats subject queueGroup
  forever $ do
    msg <- nextMessage nats sid
    cb msg

rmsub :: Nats -> SID -> IO ()
rmsub Nats{subs} sid = atomicModifyIORefCAS_ subs (Map.delete sid)

newtype UnknownSubscriptionError = UnknownSubscriptionError SID
  deriving (Show, Typeable)
instance Exception UnknownSubscriptionError

unsub :: Nats -> SID -> Maybe Int -> IO ()
unsub nats@Nats{inChan} sid Nothing = do
  writeChan inChan msg'
  rmsub nats sid
  where msg' = mconcat
          [ "UNSUB "
          , word64Dec sid
          , "\r\n"
          ]
unsub Nats{inChan, subs} sid (Just n) = do
  writeChan inChan msg'
  ss <- readIORef subs
  case Map.lookup sid ss of
    Nothing -> throw (UnknownSubscriptionError sid)
    Just subRef -> setUnsubAfter subRef n
  where msg' = mconcat
          [ "UNSUB "
          , word64Dec sid
          , " "
          , word64Dec (fromIntegral n)
          , "\r\n"
          ]


newtype UnexpectedMessageError = UnexpectedMessageError Message
  deriving (Show, Typeable)
instance Exception UnexpectedMessageError

getMsg :: Nats -> IO Message
getMsg nats@Nats{h, inChan} = do
  line <- BS.hGetLine h
  message <- Msg.decode line
  case message of
    PING -> do
      writeChan inChan "PONG\r\n"
      getMsg nats
    PONG -> getMsg nats
    INFO{} -> getMsg nats
    OK -> getMsg nats
    err@(ERR _) -> throw (UnexpectedMessageError err)
    MSG{nBytes} -> do
      payload' <- BL.hGet h nBytes
      "\r\n" <- BS.hGet h 2 -- what can go wrong?
      return message{payload=payload'}
    op -> throw (UnexpectedMessageError op)
