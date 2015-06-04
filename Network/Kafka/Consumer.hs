{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}
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

import Control.Arrow ((***))
import Control.Lens
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Either
import Data.List           (partition)
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
  offsets <- fetchOffsets -- first time will return offset response with unknown topic or partition ... then bootstrap
  newPartitions <- updateOffsetsInfo $ view responseMessage offsets -- update with ok partitions, return the new partitions
  _ <- bootstrapOffsets 0 "" newPartitions -- bootstrap partitions without an offset at 0
  return ()

-- | Stream from all assigned partitions -> outputs all polled partitions
fetchAllPartitions :: Kafka (M.Map TopicName [(Partition, Offset)], Response)
fetchAllPartitions = do
  wt <- use (kafkaClientState . stateWaitTime)
  ws <- use (kafkaClientState . stateWaitSize)
  bs <- use (kafkaClientState . stateBufferSize)
  allPartitions <- use (kafkaClientState . statePartitionOffsets)
  let addBuffer (p, o) = (p, o, bs)
  let mapBuffers xs = addBuffer <$> xs
  let includingBuffers = (\t -> (t ^. _1, mapBuffers $ t ^. _2)) <$> M.toList allPartitions
  resp <- doRequest =<< (makeRequest $ FetchRequest $ FetchReq (ordinaryConsumerId, wt, ws, includingBuffers))
  return (allPartitions, resp)

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
  coordinator <- use (kafkaClientState . stateConsumerCoordinator)
  case coordinator of
    (Just b) -> useBroker b $ makeRequest $ OffsetFetchRequest $ OffsetFetchReq (cg, M.toList partitions)
    Nothing  -> lift $ left KafkaNoConsumerCoordinator

-- | Update state offsets if successful OffsetFetchResponse received
updateOffsetsInfo :: ResponseMessage -> Kafka [(TopicName, [(Partition, Offset)])]
updateOffsetsInfo (OffsetFetchResponse r) = updateOffsets r
updateOffsetsInfo _                       = return []

-- | Update state's offsets for ok partitions - returning new partitions
updateOffsets :: OffsetFetchResponse -> Kafka [(TopicName, [(Partition, Offset)])]
updateOffsets r = do
  let allOffsets = splitOffsetResponse <$> r ^. offsetFetchResponseFields
  let okPartitions = selectOk <$> allOffsets
  let newPartitions = selectNew <$> allOffsets
  kafkaClientState . statePartitionOffsets %= \m -> foldr addOffset m okPartitions
  return newPartitions
    where addOffset t = M.insert (t ^. _1) (t ^. _2)
          selectOk = (id) *** (view _1)
          selectNew = (id) *** (view _2)

splitOffsetResponse :: (TopicName, [(Partition, Offset, Metadata, KafkaError)])
                    -> (TopicName, ([(Partition, Offset)], [(Partition, Offset)]))
splitOffsetResponse r = (r ^. _1, mapSelect $ filterOk (r ^. _2))
  where filterOk xs = partition notKafkaError xs
        notKafkaError x = (x ^. _4) == NoError
        selectFirstTwo t = (t ^. _1, t ^. _2)
        mapSelect = (fmap selectFirstTwo) *** (fmap selectFirstTwo)

-- | Commit offsets (no KafkaState update)
commitOffsets :: [(TopicName, [(Partition, Offset)])]
              -> Time
              -> Metadata
              -> Kafka Response
commitOffsets offsets time metad = do
  cg          <- use (kafkaClientState . stateConsumerGroup)
  coordinator <- use (kafkaClientState . stateConsumerCoordinator)
  case coordinator of
      (Just b) -> useBroker b $  makeRequest $ OffsetCommitRequest $ OffsetCommitReq (cg, addTimeAndMetadata <$> offsets)
      Nothing  -> lift $ left KafkaNoConsumerCoordinator
    where addTimeAndMetadata t = (t ^. _1, addTupleVals <$> t ^. _2)
          addTupleVals t = (t ^. _1, t ^. _2, time, metad)

-- | Commit input partitions to (Offset 0)
bootstrapOffsets :: Time
                 -> Metadata
                 -> [(TopicName, [(Partition, Offset)])]
                 -> Kafka Response
bootstrapOffsets t m partitionsToSet = do
  let partitionsList = mapZero partitionsToSet
  commitOffsets partitionsList t m
    where mapZero xs = pairZero <$> xs
          offsetZero ys = setZero <$> ys
          pairZero (topic, partitions) = (topic, offsetZero partitions)
          setZero = (id) *** (\_ -> Offset 0)
