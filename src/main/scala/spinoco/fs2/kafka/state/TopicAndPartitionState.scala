package spinoco.fs2.kafka.state


import shapeless.tag._
import spinoco.protocol.kafka.{Broker, PartitionId, TopicName}

/**
  * State of topic and partition
  *
  *
  * @param topic            Name of the topic
  * @param partition        Name of the partition
  * @param leader           Leader of this partition. This is where "publish" and "metadata" requests are sent
  * @param followers        All ISRs (followers) for the topic and partitions. Check the state of broker if it is not in failed state
  *                         before using it.
  */
case class TopicAndPartitionState(
  topic: String @@ TopicName
  , partition: Int @@ PartitionId
  , leader: Option[Int @@ Broker]
  , followers: Seq[Int @@ Broker]
)


