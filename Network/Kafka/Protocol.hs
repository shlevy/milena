{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE Rank2Types #-}

module Network.Kafka.Protocol where

import Control.Applicative   (Alternative(..))
import Control.Category      (Category(..))
import Control.Lens
import Control.Monad         (replicateM, liftM, liftM2, liftM3, liftM4, liftM5)
import Data.ByteString.Char8 (ByteString)
import Data.ByteString.Lens  (unpackedChars)
import Data.Digest.CRC32
import Data.Int
import Data.Serialize.Get
import Data.Serialize.Put
import GHC.Exts              (IsString(..))
import Numeric.Lens
import Prelude hiding        ((.), id)
import qualified Data.ByteString.Char8 as B
import qualified Network

class Serializable a where
  serialize :: a -> Put

class Deserializable a where
  deserialize :: Get a

data Response = Response { _responseCorrelationId :: CorrelationId, _responseMessage :: ResponseMessage } deriving (Show, Eq)

getResponse :: Int -> Get Response
getResponse l = Response <$> deserialize <*> getResponseMessage (l - 4)

newtype ConsumerMetadataResponse = ConsumerMetadataResp (KafkaError, Broker) deriving (Show, Eq, Deserializable) -- CoordinatorId CoordinatorHost CoordinatorPort
-- newtype ErrorCode = ErrorCode int16
-- newtype CoordinatorId = CoordinatorId int32
-- newtype CoordinatorHost = CoordinatorHost string
-- newtype CoordinatorPort = CoordinatorPort int32

getResponseMessage :: Int -> Get ResponseMessage
getResponseMessage l = liftM MetadataResponse          (isolate l deserialize)
                   <|> liftM OffsetResponse            (isolate l deserialize)
                   <|> liftM ProduceResponse           (isolate l deserialize)
                   <|> liftM OffsetCommitResponse      (isolate l deserialize)
                   <|> liftM OffsetFetchResponse       (isolate l deserialize)
                   <|> liftM ConsumerMetadataResponse  (isolate l deserialize)
                   <|> liftM JoinGroupResponse         (isolate l deserialize)
                   <|> liftM HeartbeatResponse         (isolate l deserialize)
                   -- MUST try FetchResponse last!
                   --
                   -- As an optimization, Kafka might return a partial message
                   -- at the end of a MessageSet, so this will consume the rest
                   -- of the message at the end of the input.
                   --
                   -- Strictly speaking, this might not actually be necessary.
                   -- Parsing a MessageSet is isolated to the byte count that's
                   -- at the beginning of a MessageSet. I don't want to spend
                   -- the time right now to prove that will always be safe, but
                   -- I'd like to at some point.
                   <|> liftM FetchResponse        (isolate l deserialize)

newtype ApiKey = ApiKey Int16 deriving (Show, Eq, Deserializable, Serializable, Num) -- numeric ID for API (i.e. metadata req, produce req, etc.)
newtype ApiVersion = ApiVersion Int16 deriving (Show, Eq, Deserializable, Serializable, Num)
newtype CorrelationId = CorrelationId Int32 deriving (Show, Eq, Deserializable, Serializable, Num, Enum)
newtype ClientId = ClientId KafkaString deriving (Show, Eq, Deserializable, Serializable, IsString)

data RequestMessage = MetadataRequest MetadataRequest
                    | ProduceRequest ProduceRequest
                    | FetchRequest FetchRequest
                    | OffsetRequest OffsetRequest
                    | OffsetCommitRequest OffsetCommitRequest
                    | OffsetFetchRequest OffsetFetchRequest
                    | ConsumerMetadataRequest ConsumerMetadataRequest
                    | JoinGroupRequest JoinGroupRequest
                    | HeartbeatRequest HeartbeatRequest
                    deriving (Show, Eq)

newtype MetadataRequest = MetadataReq [TopicName] deriving (Show, Eq, Serializable, Deserializable)
newtype TopicName = TName { _tName :: KafkaString } deriving (Show, Eq, Ord, Deserializable, Serializable, IsString)

newtype KafkaBytes = KBytes { _kafkaByteString :: ByteString } deriving (Show, Eq, IsString)
newtype KafkaString = KString { _kString :: ByteString } deriving (Show, Eq, Ord, IsString)

newtype ProduceResponse =
  ProduceResp { _produceResponseFields :: [(TopicName, [(Partition, KafkaError, Offset)])] }
  deriving (Show, Eq, Deserializable, Serializable)

newtype OffsetResponse =
  OffsetResp { _offsetResponseFields :: [(TopicName, [PartitionOffsets])] }
  deriving (Show, Eq, Deserializable)

newtype PartitionOffsets =
  PartitionOffsets { _partitionOffsetsFields :: (Partition, KafkaError, [Offset]) }
  deriving (Show, Eq, Deserializable)

newtype JoinGroupResponse =
  JoinGroupResp { _joinGroupResponseFields :: (KafkaError, GroupGenerationId, ConsumerId, [(TopicName, [Partition])]) }
  deriving (Show, Eq, Deserializable)

newtype HeartbeatResponse = HeartbeatResp KafkaError deriving (Show, Eq, Deserializable)

newtype FetchResponse =
  FetchResp { _fetchResponseFields :: [(TopicName, [(Partition, KafkaError, Offset, MessageSet)])] }
  deriving (Show, Eq, Serializable, Deserializable)

newtype MetadataResponse = MetadataResp { _metadataResponseFields :: ([Broker], [TopicMetadata]) } deriving (Show, Eq, Deserializable)
newtype Broker = Broker { _brokerFields :: (NodeId, Host, Port) } deriving (Show, Eq, Ord, Deserializable)
newtype NodeId = NodeId { _nodeId :: Int32 } deriving (Show, Eq, Ord, Deserializable, Num)
newtype Host = Host { _hostKString :: KafkaString } deriving (Show, Eq, Ord, Deserializable, IsString)
newtype Port = Port { _portInt :: Int32 } deriving (Show, Eq, Ord, Deserializable, Num)
newtype TopicMetadata = TopicMetadata { _topicMetadataFields :: (KafkaError, TopicName, [PartitionMetadata]) } deriving (Show, Eq, Deserializable)
newtype PartitionMetadata = PartitionMetadata { _partitionMetadataFields :: (KafkaError, Partition, Leader, Replicas, Isr) } deriving (Show, Eq, Deserializable)
newtype Leader = Leader { _leaderId :: Maybe Int32 } deriving (Show, Eq, Ord)

newtype Replicas = Replicas [Int32] deriving (Show, Eq, Serializable, Deserializable)
newtype Isr = Isr [Int32] deriving (Show, Eq, Deserializable)

newtype OffsetCommitResponse = OffsetCommitResp { _offsetCommitResponseFields :: [(TopicName, [(Partition, KafkaError)])] } deriving (Show, Eq, Deserializable)
newtype OffsetFetchResponse = OffsetFetchResp { _offsetFetchResponseFields :: [(TopicName, [(Partition, Offset, Metadata, KafkaError)])] }  deriving (Show, Eq, Deserializable)

newtype OffsetRequest = OffsetReq (ReplicaId, [(TopicName, [(Partition, Time, MaxNumberOfOffsets)])]) deriving (Show, Eq, Serializable)
newtype Time = Time { _timeInt :: Int64 } deriving (Show, Eq, Serializable, Num, Bounded)
newtype MaxNumberOfOffsets = MaxNumberOfOffsets Int32 deriving (Show, Eq, Serializable, Num)

newtype FetchRequest =
  FetchReq (ReplicaId, MaxWaitTime, MinBytes,
            [(TopicName, [(Partition, Offset, MaxBytes)])])
  deriving (Show, Eq, Deserializable, Serializable)

newtype ReplicaId = ReplicaId Int32 deriving (Show, Eq, Num, Serializable, Deserializable)
newtype MaxWaitTime = MaxWaitTime Int32 deriving (Show, Eq, Num, Serializable, Deserializable)
newtype MinBytes = MinBytes Int32 deriving (Show, Eq, Num, Serializable, Deserializable)
newtype MaxBytes = MaxBytes Int32 deriving (Show, Eq, Num, Serializable, Deserializable)

newtype ProduceRequest =
  ProduceReq (RequiredAcks, Timeout,
              [(TopicName, [(Partition, MessageSet)])])
  deriving (Show, Eq, Serializable)

newtype RequiredAcks = RequiredAcks Int16 deriving (Show, Eq, Serializable, Deserializable, Num)
newtype Timeout = Timeout Int32 deriving (Show, Eq, Serializable, Deserializable, Num)
newtype Partition = Partition Int32 deriving (Show, Ord, Eq, Serializable, Deserializable, Num)

newtype MessageSet = MessageSet { _messageSetMembers :: [MessageSetMember] } deriving (Show, Eq)
data MessageSetMember = MessageSetMember { _setOffset :: Offset, _setMessage :: Message } deriving (Show, Eq)

newtype Offset = Offset Int64 deriving (Show, Eq, Serializable, Deserializable, Num)
newtype Message = Message { _messageFields :: (Crc, MagicByte, Attributes, Key, Value) } deriving (Show, Eq, Deserializable)

newtype Crc = Crc Int32 deriving (Show, Eq, Serializable, Deserializable, Num)
newtype MagicByte = MagicByte Int8 deriving (Show, Eq, Serializable, Deserializable, Num)
newtype Attributes = Attributes Int8 deriving (Show, Eq, Serializable, Deserializable, Num)

newtype Key = Key { _keyBytes :: Maybe KafkaBytes } deriving (Show, Eq)
newtype Value = Value { _valueBytes :: Maybe KafkaBytes } deriving (Show, Eq)

data ResponseMessage = MetadataResponse MetadataResponse
                     | ProduceResponse ProduceResponse
                     | FetchResponse FetchResponse
                     | OffsetResponse OffsetResponse
                     | OffsetCommitResponse OffsetCommitResponse
                     | OffsetFetchResponse OffsetFetchResponse
                     | ConsumerMetadataResponse ConsumerMetadataResponse
                     | JoinGroupResponse JoinGroupResponse
                     | HeartbeatResponse HeartbeatResponse
                     deriving (Show, Eq)

newtype ConsumerMetadataRequest = ConsumerMetadataReq ConsumerGroup deriving (Show, Eq, Serializable)

newtype OffsetCommitRequest = OffsetCommitReq (ConsumerGroup, [(TopicName, [(Partition, Offset, Time, Metadata)])]) deriving (Show, Eq, Serializable)
newtype OffsetFetchRequest = OffsetFetchReq (ConsumerGroup, [(TopicName, [Partition])]) deriving (Show, Eq, Serializable)
newtype Metadata = Metadata KafkaString deriving (Show, Eq, Serializable, Deserializable, IsString)

newtype ConsumerGroup = ConsumerGroup KafkaString deriving (Show, Eq, Serializable, Deserializable, IsString)
newtype ConsumerId = ConsumerId KafkaString deriving (Show, Eq, Serializable, Deserializable, IsString)
newtype JoinGroupRequest = JoinGroupReq (ConsumerGroup, Timeout, [TopicName], ConsumerId, PartitionAssignmentStrategy) deriving (Show, Eq, Serializable)
newtype GroupGenerationId = GroupGenerationId Int32 deriving (Show, Eq, Ord, Serializable, Deserializable, Num)

newtype HeartbeatRequest = HeartbeatReq (ConsumerGroup, GroupGenerationId, ConsumerId) deriving (Show, Eq, Serializable)

data PartitionAssignmentStrategy = RoundRobin | Range deriving (Show, Eq)

partitionStrategy :: PartitionAssignmentStrategy -> KafkaString
partitionStrategy RoundRobin = "roundrobin"
partitionStrategy Range      = "range"

instance Serializable PartitionAssignmentStrategy where
  serialize = serialize . partitionStrategy

errorKafka :: KafkaError -> Int16
errorKafka NoError                                 = 0
errorKafka Unknown                                 = -1
errorKafka OffsetOutOfRange                        = 1
errorKafka InvalidMessage                          = 2
errorKafka UnknownTopicOrPartition                 = 3
errorKafka InvalidMessageSize                      = 4
errorKafka LeaderNotAvailable                      = 5
errorKafka NotLeaderForPartition                   = 6
errorKafka RequestTimedOut                         = 7
errorKafka BrokerNotAvailable                      = 8
errorKafka ReplicaNotAvailable                     = 9
errorKafka MessageSizeTooLarge                     = 10
errorKafka StaleControllerEpochCode                = 11
errorKafka OffsetMetadataTooLargeCode              = 12
errorKafka OffsetsLoadInProgressCode               = 14
errorKafka ConsumerCoordinatorNotAvailableCode     = 15
errorKafka NotCoordinatorForConsumerCode           = 16
errorKafka InvalidTopicException                   = 17
errorKafka RecordListTooLarge                      = 18
errorKafka NotEnoughReplicas                       = 19
errorKafka NotEnoughReplicasAfterAppend            = 20
errorKafka InvalidRequiredAcks                     = 21
errorKafka IllegalGeneration                       = 22
errorKafka InconsistentPartitionAssignmentStrategy = 23
errorKafka UnknownPartitionAssignmentStrategy      = 24
errorKafka UnknownConsumerId                       = 25
errorKafka InvalidSessionTimeout                   = 26

data KafkaError = NoError -- ^ @0@ No error--it worked!
                | Unknown -- ^ @-1@ An unexpected server error
                | OffsetOutOfRange -- ^ @1@ The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
                | InvalidMessage -- ^ @2@ This indicates that a message contents does not match its CRC
                | UnknownTopicOrPartition -- ^ @3@ This request is for a topic or partition that does not exist on this broker.
                | InvalidMessageSize -- ^ @4@ The message has a negative size
                | LeaderNotAvailable -- ^ @5@ This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
                | NotLeaderForPartition -- ^ @6@ This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
                | RequestTimedOut -- ^ @7@ This error is thrown if the request exceeds the user-specified time limit in the request.
                | BrokerNotAvailable -- ^ @8@ This is not a client facing error and is used mostly by tools when a broker is not alive.
                | ReplicaNotAvailable -- ^ @9@ If replica is expected on a broker, but is not.
                | MessageSizeTooLarge -- ^ @10@ The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
                | StaleControllerEpochCode -- ^ @11@ Internal error code for broker-to-broker communication.
                | OffsetMetadataTooLargeCode -- ^ @12@ If you specify a string larger than configured maximum for offset metadata
                | OffsetsLoadInProgressCode -- ^ @14@ The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
                | ConsumerCoordinatorNotAvailableCode -- ^ @15@ The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
                | NotCoordinatorForConsumerCode -- ^ @16@ The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
                | InvalidTopicException -- ^ @17@ The request attempted to perform an operation on an invalid topic.
                | RecordListTooLarge -- ^ @18@ The request included message batch larger than the configured segment size on the server.
                | NotEnoughReplicas -- ^ @19@ Messages are rejected since there are fewer in-sync replicas than required.
                | NotEnoughReplicasAfterAppend -- ^ @20@ Messages are written to the log, but to fewer in-sync replicas than required.
                | InvalidRequiredAcks -- ^ @21@ Produce request specified an invalid value for required acks.
                | IllegalGeneration -- ^ @22@ The specified consumer generation id is not valid.
                | InconsistentPartitionAssignmentStrategy -- ^ @23@ The request partition assignment strategy does not match that of the group.
                | UnknownPartitionAssignmentStrategy -- ^ @24@ The request partition assignment strategy is unknown to the broker.
                | UnknownConsumerId -- ^ @25@ The coordinator is not aware of this consumer.
                | InvalidSessionTimeout -- ^ @26@ The session timeout is not within an acceptable range.
                deriving (Eq, Show)

instance Serializable KafkaError where
  serialize = serialize . errorKafka

instance Deserializable KafkaError where
  deserialize = do
    x <- deserialize :: Get Int16
    case x of
      0    -> return NoError
      (-1) -> return Unknown
      1    -> return OffsetOutOfRange
      2    -> return InvalidMessage
      3    -> return UnknownTopicOrPartition
      4    -> return InvalidMessageSize
      5    -> return LeaderNotAvailable
      6    -> return NotLeaderForPartition
      7    -> return RequestTimedOut
      8    -> return BrokerNotAvailable
      9    -> return ReplicaNotAvailable
      10   -> return MessageSizeTooLarge
      11   -> return StaleControllerEpochCode
      12   -> return OffsetMetadataTooLargeCode
      14   -> return OffsetsLoadInProgressCode
      15   -> return ConsumerCoordinatorNotAvailableCode
      16   -> return NotCoordinatorForConsumerCode
      17   -> return InvalidTopicException
      18   -> return RecordListTooLarge
      19   -> return NotEnoughReplicas
      20   -> return NotEnoughReplicasAfterAppend
      21   -> return InvalidRequiredAcks
      22   -> return IllegalGeneration
      23   -> return InconsistentPartitionAssignmentStrategy
      24   -> return UnknownPartitionAssignmentStrategy
      25   -> return UnknownConsumerId
      26   -> return InvalidSessionTimeout
      _    -> fail $ "invalid error code: " ++ show x

newtype Request = Request (CorrelationId, ClientId, RequestMessage) deriving (Show, Eq)

instance Serializable Request where
  serialize (Request (correlationId, clientId, r)) = do
    serialize (apiKey r)
    serialize (apiVersion r)
    serialize correlationId
    serialize clientId
    serialize r

requestBytes :: Request -> ByteString
requestBytes x = runPut $ do
  putWord32be . fromIntegral $ B.length mr
  putByteString mr
    where mr = runPut $ serialize x

apiVersion :: RequestMessage -> ApiVersion
apiVersion _ = ApiVersion 0 -- everything is at version 0 right now

apiKey :: RequestMessage -> ApiKey
apiKey (ProduceRequest{}) = ApiKey 0
apiKey (FetchRequest{}) = ApiKey 1
apiKey (OffsetRequest{}) = ApiKey 2
apiKey (MetadataRequest{}) = ApiKey 3
apiKey (OffsetCommitRequest{}) = ApiKey 8
apiKey (OffsetFetchRequest{}) = ApiKey 9
apiKey (ConsumerMetadataRequest{}) = ApiKey 10
apiKey (JoinGroupRequest {}) = ApiKey 11
apiKey (HeartbeatRequest {}) = ApiKey 12

instance Serializable RequestMessage where
  serialize (ProduceRequest r) = serialize r
  serialize (FetchRequest r) = serialize r
  serialize (OffsetRequest r) = serialize r
  serialize (MetadataRequest r) = serialize r
  serialize (OffsetCommitRequest r) = serialize r
  serialize (OffsetFetchRequest r) = serialize r
  serialize (ConsumerMetadataRequest r) = serialize r
  serialize (JoinGroupRequest r) = serialize r
  serialize (HeartbeatRequest r) = serialize r

instance Serializable Int64 where serialize = putWord64be . fromIntegral
instance Serializable Int32 where serialize = putWord32be . fromIntegral
instance Serializable Int16 where serialize = putWord16be . fromIntegral
instance Serializable Int8  where serialize = putWord8    . fromIntegral

instance Serializable Key where
  serialize (Key (Just bs)) = serialize bs
  serialize (Key Nothing)   = serialize (-1 :: Int32)

instance Serializable Value where
  serialize (Value (Just bs)) = serialize bs
  serialize (Value Nothing)   = serialize (-1 :: Int32)

instance Serializable KafkaString where
  serialize (KString bs) = do
    let l = fromIntegral (B.length bs) :: Int16
    serialize l
    putByteString bs

instance Serializable MessageSet where
  serialize (MessageSet ms) = do
    let bytes = runPut $ mapM_ serialize ms
        l = fromIntegral (B.length bytes) :: Int32
    serialize l
    putByteString bytes

instance Serializable KafkaBytes where
  serialize (KBytes bs) = do
    let l = fromIntegral (B.length bs) :: Int32
    serialize l
    putByteString bs

instance Serializable MessageSetMember where
  serialize (MessageSetMember offset msg) = do
    serialize offset
    serialize msize
    serialize msg
      where msize = fromIntegral $ B.length $ runPut $ serialize msg :: Int32

instance Serializable Message where
  serialize (Message (_, magic, attrs, k, v)) = do
    let m = runPut $ serialize magic >> serialize attrs >> serialize k >> serialize v
    putWord32be (crc32 m)
    putByteString m

instance (Serializable a) => Serializable [a] where
  serialize xs = do
    let l = fromIntegral (length xs) :: Int32
    serialize l
    mapM_ serialize xs

instance (Serializable a, Serializable b) => Serializable ((,) a b) where
  serialize (x, y) = serialize x >> serialize y
instance (Serializable a, Serializable b, Serializable c) => Serializable ((,,) a b c) where
  serialize (x, y, z) = serialize x >> serialize y >> serialize z
instance (Serializable a, Serializable b, Serializable c, Serializable d) => Serializable ((,,,) a b c d) where
  serialize (w, x, y, z) = serialize w >> serialize x >> serialize y >> serialize z
instance (Serializable a, Serializable b, Serializable c, Serializable d, Serializable e) => Serializable ((,,,,) a b c d e) where
  serialize (v, w, x, y, z) = serialize v >> serialize w >> serialize x >> serialize y >> serialize z

instance Deserializable MessageSet where
  deserialize = do
    l <- deserialize :: Get Int32
    ms <- isolate (fromIntegral l) getMembers
    return $ MessageSet ms
      where getMembers :: Get [MessageSetMember]
            getMembers = do
              wasEmpty <- isEmpty
              if wasEmpty
              then return []
              else liftM2 (:) deserialize getMembers <|> (remaining >>= getBytes >> return [])

instance Deserializable MessageSetMember where
  deserialize = do
    o <- deserialize
    l <- deserialize :: Get Int32
    m <- isolate (fromIntegral l) deserialize
    return $ MessageSetMember o m

instance Deserializable Leader where
  deserialize = do
    x <- deserialize :: Get Int32
    let l = Leader $ if x == -1 then Nothing else Just x
    return l

instance Deserializable KafkaBytes where
  deserialize = do
    l <- deserialize :: Get Int32
    bs <- getByteString $ fromIntegral l
    return $ KBytes bs

instance Deserializable KafkaString where
  deserialize = do
    l <- deserialize :: Get Int16
    bs <- getByteString $ fromIntegral l
    return $ KString bs

instance Deserializable Key where
  deserialize = do
    l <- deserialize :: Get Int32
    case l of
      -1 -> return (Key Nothing)
      _ -> do
        bs <- getByteString $ fromIntegral l
        return $ Key (Just (KBytes bs))

instance Deserializable Value where
  deserialize = do
    l <- deserialize :: Get Int32
    case l of
      -1 -> return (Value Nothing)
      _ -> do
        bs <- getByteString $ fromIntegral l
        return $ Value (Just (KBytes bs))

instance (Deserializable a) => Deserializable [a] where
  deserialize = do
    l <- deserialize :: Get Int32
    replicateM (fromIntegral l) deserialize

instance (Deserializable a, Deserializable b) => Deserializable ((,) a b) where
  deserialize = liftM2 (,) deserialize deserialize
instance (Deserializable a, Deserializable b, Deserializable c) => Deserializable ((,,) a b c) where
  deserialize = liftM3 (,,) deserialize deserialize deserialize
instance (Deserializable a, Deserializable b, Deserializable c, Deserializable d) => Deserializable ((,,,) a b c d) where
  deserialize = liftM4 (,,,) deserialize deserialize deserialize deserialize
instance (Deserializable a, Deserializable b, Deserializable c, Deserializable d, Deserializable e) => Deserializable ((,,,,) a b c d e) where
  deserialize = liftM5 (,,,,) deserialize deserialize deserialize deserialize deserialize

instance Deserializable Int64 where deserialize = liftM fromIntegral getWord64be
instance Deserializable Int32 where deserialize = liftM fromIntegral getWord32be
instance Deserializable Int16 where deserialize = liftM fromIntegral getWord16be
instance Deserializable Int8  where deserialize = liftM fromIntegral getWord8

-- * Generated lenses

makeLenses ''Response

makeLenses ''TopicName

makeLenses ''KafkaBytes
makeLenses ''KafkaString

makeLenses ''ProduceResponse

makeLenses ''OffsetResponse
makeLenses ''PartitionOffsets
makeLenses ''JoinGroupResponse
makeLenses ''OffsetCommitResponse
makeLenses ''OffsetFetchResponse
makeLenses ''FetchResponse

makeLenses ''MetadataResponse
makeLenses ''Broker
makeLenses ''NodeId
makeLenses ''Host
makeLenses ''Port
makeLenses ''TopicMetadata
makeLenses ''PartitionMetadata
makeLenses ''Leader

makeLenses ''Time

makeLenses ''Partition

makeLenses ''MessageSet
makeLenses ''MessageSetMember
makeLenses ''Offset

makeLenses ''Message

makeLenses ''Key
makeLenses ''Value

makePrisms ''ResponseMessage

-- * Composed lenses

keyed :: (Field1 a a b b, Choice p, Applicative f, Eq b) => b -> Optic' p f a a
keyed k = filtered (view $ _1 . to (== k))

joinGroupError :: Lens' JoinGroupResponse KafkaError
joinGroupError = joinGroupResponseFields . _1

joinGroupGenerationId :: Lens' JoinGroupResponse GroupGenerationId
joinGroupGenerationId = joinGroupResponseFields . _2

joinGroupConsumerId :: Lens' JoinGroupResponse ConsumerId
joinGroupConsumerId = joinGroupResponseFields . _3

joinGroupPartitionInfo :: Lens' JoinGroupResponse [(TopicName, [Partition])]
joinGroupPartitionInfo = joinGroupResponseFields . _4

metadataResponseBrokers :: Lens' MetadataResponse [Broker]
metadataResponseBrokers = metadataResponseFields . _1

topicsMetadata :: Lens' MetadataResponse [TopicMetadata]
topicsMetadata = metadataResponseFields . _2

topicMetadataKafkaError :: Lens' TopicMetadata KafkaError
topicMetadataKafkaError = topicMetadataFields . _1

topicMetadataName :: Lens' TopicMetadata TopicName
topicMetadataName = topicMetadataFields . _2

partitionsMetadata :: Lens' TopicMetadata [PartitionMetadata]
partitionsMetadata = topicMetadataFields . _3

partitionId :: Lens' PartitionMetadata Partition
partitionId = partitionMetadataFields . _2

partitionMetadataLeader :: Lens' PartitionMetadata Leader
partitionMetadataLeader = partitionMetadataFields . _3

brokerNode :: Lens' Broker NodeId
brokerNode = brokerFields . _1

brokerHost :: Lens' Broker Host
brokerHost = brokerFields . _2

brokerPort :: Lens' Broker Port
brokerPort = brokerFields . _3

fetchResponseMessages :: Fold FetchResponse MessageSet
fetchResponseMessages = fetchResponseFields . folded . _2 . folded . _4

fetchResponseByTopic :: TopicName -> Fold FetchResponse (Partition, KafkaError, Offset, MessageSet)
fetchResponseByTopic t = fetchResponseFields . folded . keyed t . _2 . folded

messageSetByPartition :: Partition -> Fold (Partition, KafkaError, Offset, MessageSet) MessageSetMember
messageSetByPartition p = keyed p . _4 . messageSetMembers . folded

fetchResponseMessageMembers :: Fold FetchResponse MessageSetMember
fetchResponseMessageMembers = fetchResponseMessages . messageSetMembers . folded

messageValue :: Lens' Message Value
messageValue = messageFields . _5

payload :: Fold Message ByteString
payload = messageValue . valueBytes . folded . kafkaByteString

offsetResponseOffset :: Partition -> Fold OffsetResponse Offset
offsetResponseOffset p = offsetResponseFields . folded . _2 . folded . partitionOffsetsFields . keyed p . _3 . folded

messageSet :: Partition -> TopicName -> Fold FetchResponse MessageSetMember
messageSet p t = fetchResponseByTopic t . messageSetByPartition p

nextOffset :: Lens' MessageSetMember Offset
nextOffset = setOffset . adding 1

findPartitionMetadata :: Applicative f => TopicName -> LensLike' f TopicMetadata [PartitionMetadata]
findPartitionMetadata t = filtered (view $ topicMetadataName . to (== t)) . partitionsMetadata

findPartition :: Partition -> Prism' PartitionMetadata PartitionMetadata
findPartition p = filtered (view $ partitionId . to (== p))

hostString :: Lens' Host String
hostString = hostKString . kString . unpackedChars

portId :: IndexPreservingGetter Port Network.PortID
portId = portInt . to fromIntegral . to Network.PortNumber
