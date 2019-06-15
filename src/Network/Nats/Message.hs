module Network.Nats.Message
  ( SID
  , Message(..)
  , UnknownMessageType
  , decode
  , ParseServerInfoError
  , parseInfo
  , ParseMessageError
  , parseMessage
  ) where

import Control.Exception.Safe
import Data.Aeson ((.:), (.:?), (.!=))
import Data.Word (Word64)
import Data.Text (Text)
import GHC.Generics
import qualified Data.Aeson as AE
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import Data.ByteString.Char8 (ByteString)

-- Subscription id
type SID = Word64

data Message
  = INFO { serverId :: Text
         , version :: Text
         , proto :: Int
         , gitCommit :: Text
         , go :: Text
         , host :: Text
         , port :: Int
         , maxPayload :: Int
         , clientId :: Int
         , authRequired :: Bool
         , tlsRequired :: Bool
         , tlsVerify :: Bool
         , connectionUrls :: [Text]
         }
  | CONNECT { verbose :: Bool
            , pedantic :: Bool
            , tlsRequired :: Bool
            , authToken :: Maybe Text
            , user :: Maybe Text
            , pass :: Maybe Text
            , lang :: Text
            , version :: Text
            , protocol :: Int
            , echo :: Bool
            }
  | PUB { subject :: ByteString
        , replyTo :: Maybe ByteString
        , nBytes :: Int
        , payload :: BL.ByteString
        }
  | SUB { subject :: ByteString
        , queueGroup :: ByteString
        , sid :: SID
        }
  | UNSUB { sid :: SID
          , maxMsgs :: Maybe Int
          }
  | MSG { subject :: !ByteString
        , sid :: !SID
        , replyTo :: !(Maybe ByteString)
        , nBytes :: !Int
        , payload :: BL.ByteString
        }
  | PING
  | PONG
  | OK
  | ERR ByteString
  deriving (Generic, Show)


instance AE.FromJSON Message where
  parseJSON =
    AE.withObject "INFO" $
    \v ->
      INFO
      <$> v .: "server_id"
      <*> v .: "version"
      <*> v .: "proto"
      <*> v .: "git_commit"
      <*> v .: "go"
      <*> v .: "host"
      <*> v .: "port"
      <*> v .: "max_payload"
      <*> v .: "client_id"
      <*> v .:? "auth_required" .!= False
      <*> v .:? "tls_required" .!= False
      <*> v .:? "tls_verify" .!= False
      <*> v .:? "connection_urls" .!= []

newtype UnknownMessageType = UnknownMessageType ByteString
  deriving (Show, Typeable)
instance Exception UnknownMessageType

decode :: MonadThrow m => ByteString -> m Message
decode bytes = decode' mtype (BS.drop 1 mrest)
  where
    (mtype, mrest) = BS.span (\x -> x /= ' ' && x /= '\r') bytes
    decode' :: MonadThrow m => ByteString -> ByteString -> m Message
    decode' "PING" _ = return PING
    decode' "PONG" _ = return PONG
    decode' "+OK" _  = return OK
    decode' "-ERR" msg = return (ERR msg)
    decode' "INFO" msg = parseInfo msg
    decode' "MSG" msg = parseMessage msg
    decode' _ _ = throw (UnknownMessageType bytes)

newtype ParseServerInfoError = ParseServerInfoError String
  deriving (Show, Typeable)
instance Exception ParseServerInfoError

parseInfo :: MonadThrow m => ByteString -> m Message
parseInfo msg =
  case AE.eitherDecodeStrict msg of
    Left e -> throw (ParseServerInfoError e)
    Right info -> return info

data ParseMessageError = ParseMessageError String ByteString
  deriving (Show, Typeable)
instance Exception ParseMessageError

parseMessage :: MonadThrow m => ByteString -> m Message
parseMessage bytes =
  case BS.words bytes of
    [subj, subid, reply, len] -> do
      len' <- parse "message length" len
      subid' <- parse "sid" subid
      return MSG { subject = subj
                 , sid = fromIntegral subid'
                 , replyTo = Just reply
                 , nBytes = len'
                 , payload = undefined
                 }
    [subj, subid, len] -> do
      len' <- parse "message length" len
      subid' <- parse "sid" subid
      return MSG { subject = subj
                 , sid = fromIntegral subid'
                 , replyTo = Nothing
                 , nBytes = len'
                 , payload = undefined
                 }
    _ -> throw $ ParseMessageError "can't tokenize message header" bytes
  where
    cantParse name it =
      throw $ ParseMessageError ("can't parse " ++ name) it
    parse name it =
      case BS.readInt it of
        Nothing -> cantParse name it
        Just (it', "") -> return it'
        Just _ -> cantParse name it
