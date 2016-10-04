package spinoco.fs2.kafka.state


import shapeless.tag
import shapeless.tag.@@
import spinoco.fs2.kafka.TopicAndPartition
import spinoco.protocol.kafka.Broker

/**
  * Represents internal state of the Kafka Client
  *
  * @param brokers            All known brokers with their last state
  * @param topics             All known topics and partitions with their last state
  * @param terminated         Yields to true, when the client Stream terminates. Signals to all subscribers and publishers
  *                           that they have to release their resources
  * @param generation        Last known broker generation
  */
case class ClientState[F[_]](
  brokers: Map[Int @@ Broker, BrokerState[F]]
  , topics: Map[TopicAndPartition, TopicAndPartitionState[F]]
  , terminated: Boolean
  , generation: Long @@ BrokerGeneration
)

object ClientState {

  /** provides initial client state **/
  def initial[F[_]]:ClientState[F] = ClientState(
    brokers = Map.empty
    , topics = Map.empty
    , terminated = false
    , generation = tag[BrokerGeneration](0)
  )

}



