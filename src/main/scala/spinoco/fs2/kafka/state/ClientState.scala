package spinoco.fs2.kafka.state



import shapeless.tag.@@
import spinoco.fs2.kafka.TopicAndPartition
import spinoco.protocol.kafka.{Broker, PartitionId, TopicName}

/**
  * Represents internal state of the Kafka Client
  *
  * @param brokers            All known brokers with their last state
  * @param topics             All known topics and partitions with their last state
  *
  */
case class ClientState[F[_]](
  brokers: Map[Int @@ Broker, BrokerState[F]]
  , topics: Map[TopicAndPartition, TopicAndPartitionState]
)  { self =>

  /** updates state of broker **/
  def updateBroker(state: BrokerState[F]): ClientState[F] =
    self.copy(brokers = self.brokers + (state.brokerId -> state))

  /** queries active known leader for given topic **/
  def leaderFor(topicName: String @@ TopicName, partition: Int @@ PartitionId): Option[BrokerState[F]] =
    self.topics.get((topicName, partition)) flatMap { _.leader } flatMap { leaderId => brokers.get(leaderId) }


}

object ClientState {

  /** provides initial client state **/
  def initial[F[_]]:ClientState[F] = ClientState(
    brokers = Map.empty
    , topics = Map.empty
  )


}



