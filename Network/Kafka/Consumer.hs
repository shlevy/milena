{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TupleSections #-}

-- |
-- = General offset tracking Kafka consumer API:
--
--   Sequence of effects for joining a /Consumer Group/ :
--
--
--   (1) 'Metadata' request for topic to any broker in the quorum - returns leaders (broker data) for each partition of the given topic
--
--   2. Initialize coordinator using a 'ConsumerMetadataRequest' - this returns the coordinator broker. This will take some time to instantiate after returning the response.
--
--   3. 'JoinGroupRequest' (dispatch to the coordinator broker) - acquire 'GroupGenerationId', 'ConsumerId', list of [Partition] to poll
--
--   4. 'OffsetFetchRequest' to retrieve all offsets for all assigned partitions. If partition offset = -1, you may desire to 'bootstrapOffsets' by 'offsetCommitRequest' at 0.
--
-- n.b. you __must__ send a heartbeat request for any given @ConsumerId@ within the @Timeout@ specified in the @JoinGroupRequest@
--
-- == /Iterative streaming:/
--
-- Using the Kafka monad.

module Network.Kafka.Consumer where

import Control.Lens
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Either
import Network.Kafka
import Network.Kafka.Protocol
import qualified Data.Map as M

-- | Initialize the consumer (i.e. join group)
initializeConsumer :: PartitionAssignmentStrategy
                   -> Timeout
                   -> [TopicName]
                   -> Kafka ()
initializeConsumer strategy timeout topics = do
  updateMetadatas topics
  consumerMetadataReq >>= updateConsumerMetadata
  (kafkaClientState . stateConsumerTopics) .= topics
  joinGroupReq strategy timeout >>= updateGroupInfo . view responseMessage

-- | Execute a Kafka Request with a given broker
useBroker :: Broker -> Kafka Request -> Kafka Response
useBroker b fn = do
  withBrokerHandle b $ \handle -> (fn >>= doRequest' handle)

-- | Acquire metadata for the state's consumer group
consumerMetadataReq :: Kafka ResponseMessage
consumerMetadataReq = do
  cg  <- use (kafkaClientState . stateConsumerGroup)
  req <- makeRequest $ ConsumerMetadataRequest $ ConsumerMetadataReq cg
  view responseMessage <$> doRequest req

-- | Update state consumer coordinator if valid consumer metadata response - retry until response ok.
updateConsumerMetadata :: ResponseMessage -> Kafka ()
updateConsumerMetadata (ConsumerMetadataResponse
                         (ConsumerMetadataResp (err, b))
                       ) | err == NoError = updateConsumerCoordinator b
                         | otherwise      = consumerMetadataReq >>= updateConsumerMetadata
updateConsumerMetadata _                  = return ()

updateConsumerCoordinator :: Broker -> Kafka ()
updateConsumerCoordinator b = do
  (kafkaClientState . stateConsumerCoordinator) .= Just b
  return ()

-- | Join consumer group with given partition strategy
joinGroupReq :: PartitionAssignmentStrategy
             -> Timeout
             -> Kafka Response
joinGroupReq strategy timeout = do
  cg          <- use (kafkaClientState . stateConsumerGroup)
  topic       <- use (kafkaClientState . stateConsumerTopics)
  cid         <- use (kafkaClientState . stateConsumerId . non "")
  coordinator <- use (kafkaClientState . stateConsumerCoordinator)
  case coordinator of
    (Just b) -> useBroker b $ makeRequest $ JoinGroupRequest $ JoinGroupReq (cg, timeout, topic, cid, strategy)
    Nothing  -> lift $ left KafkaNoConsumerCoordinator

updateGroupInfo :: ResponseMessage -> Kafka ()
updateGroupInfo (JoinGroupResponse r) | noErrorJGR r = updateGroupInfo' r
                                      | otherwise    = return ()
updateGroupInfo _                                    = return ()

noErrorJGR :: JoinGroupResponse -> Bool
noErrorJGR r = r ^. joinGroupError == NoError

-- | Update state consumer group information
updateGroupInfo' :: JoinGroupResponse -> Kafka ()
updateGroupInfo' r = do
  kafkaClientState . stateGroupGenerationId .= (Just $ r ^. joinGroupGenerationId)
  kafkaClientState . stateConsumerPartitions .= (M.fromList $ r ^. joinGroupPartitionInfo)
  kafkaClientState . stateConsumerId .= (Just $ r ^. joinGroupConsumerId)
  return ()

-- | Retrieve offsets for all assigned partitions (by their topics)
fetchOffsets :: Kafka Response
fetchOffsets = do
  cg         <- use (kafkaClientState . stateConsumerGroup)
  partitions <- use (kafkaClientState . stateConsumerPartitions)
  req        <- makeRequest $ OffsetFetchRequest $ OffsetFetchReq (cg, M.toList partitions)
  doRequest req

-- | Update state offsets if successful OffsetFetchResponse received
updateOffsetsInfo :: ResponseMessage -> Kafka ()
updateOffsetsInfo (OffsetFetchResponse r) = updateOffsets r
updateOffsetsInfo _                       = return ()

updateOffsets :: OffsetFetchResponse -> Kafka ()
updateOffsets r = do
  let updates = filterErrorOFI <$> r ^. offsetFetchResponseFields
  kafkaClientState . statePartitionOffsets %= \m -> foldr addOffset m updates
  return ()
    where addOffset t = M.insert (t ^. _1) (t ^. _2)

filterErrorOFI :: (TopicName, [(Partition, Offset, Metadata, KafkaError)])
               -> (TopicName, [(Partition, Offset)])
filterErrorOFI r = (r ^. _1, filterOk  (r ^. _2))
  where filterOk xs = (\t -> (t ^. _1, t ^. _2)) <$> filter notKafkaError xs
        notKafkaError x = (x ^. _4) == NoError

-- | Commit offsets (no KafkaState update)
commitOffsets :: [(TopicName, [(Partition, Offset)])]
              -> Time
              -> Metadata
              -> Kafka Response
commitOffsets offsets time metad = do
  cg  <- use (kafkaClientState . stateConsumerGroup)
  req <- makeRequest $ OffsetCommitRequest $ OffsetCommitReq (cg, addTimeAndMetadata <$> offsets)
  doRequest req
    where addTimeAndMetadata t = (t ^. _1, addTupleVals <$> t ^. _2)
          addTupleVals t = (t ^. _1, t ^. _2, time, metad)

-- | Initialize all partitions at (Offset 0)
bootstrapOffsets :: Kafka Response
bootstrapOffsets = do
  assignedPartitions <- use (kafkaClientState . stateConsumerPartitions)
  let partitionsList = mapZero $ M.toList assignedPartitions
  commitOffsets partitionsList 0 "Init offsets"
    where mapZero xs = pairZero <$> xs
          offsetZero ys = (, Offset 0) <$> ys
          pairZero (topic, partitions) = (topic, offsetZero partitions)
