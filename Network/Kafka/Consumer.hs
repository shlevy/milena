{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

module Network.Kafka.Consumer where

import Control.Lens
import Network.Kafka
import Network.Kafka.Protocol
import qualified Data.Map as M


consumerMetadataReq :: Kafka Response
consumerMetadataReq = do
  cg  <- use (kafkaClientState . stateConsumerGroup)
  req <- makeRequest $ ConsumerMetadataRequest $ ConsumerMetadataReq cg
  doRequest req

metadataForTopics :: [TopicName] -> Kafka MetadataResponse
metadataForTopics xs = metadata $ MetadataReq xs

joinGroupReq :: PartitionAssignmentStrategy
             -- | Timeout in ms (>=6000 && <= 30000) after current time for lifetime of consumer
             -> Timeout
             -> Kafka Response
joinGroupReq strategy timeout = do
  cg    <- use (kafkaClientState . stateConsumerGroup)
  topic <- use (kafkaClientState . stateConsumerTopics)
  cid   <- use (kafkaClientState . stateConsumerId . non "")
  req   <- makeRequest $ JoinGroupRequest $ JoinGroupReq (cg, timeout, topic, cid, strategy)
  doRequest req

updateGroupInfo :: ResponseMessage -> Kafka ()
updateGroupInfo (JoinGroupResponse r) | noErrorJGR r = updateGroupInfo' r
                                      | otherwise    = return ()
updateGroupInfo _                                    = return ()

noErrorJGR :: JoinGroupResponse -> Bool
noErrorJGR r = r ^. joinGroupError == NoError

updateGroupInfo' :: JoinGroupResponse -> Kafka ()
updateGroupInfo' r = do
  kafkaClientState . stateGroupGenerationId %= \_ -> Just $ r ^. joinGroupGenerationId
  kafkaClientState . stateConsumerPartitions %= \_ -> M.fromList $ r ^. joinGroupPartitionInfo
  kafkaClientState . stateConsumerId %= \_ -> Just $ r ^. joinGroupConsumerId
  return ()

fetchOffsets :: Kafka Response
fetchOffsets = do
  cg         <- use (kafkaClientState . stateConsumerGroup)
  partitions <- use (kafkaClientState . stateConsumerPartitions)
  req        <- makeRequest $ OffsetFetchRequest $ OffsetFetchReq (cg, M.toList partitions)
  doRequest req

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
