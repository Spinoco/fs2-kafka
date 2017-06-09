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
case class ClientState(
  brokers: Map[Int @@ Broker, BrokerData]
  , topics: Map[TopicAndPartition, TopicAndPartitionState]
)  { self =>

  /** updates state of broker **/
  def updateBroker(state: BrokerData): ClientState =
    self.copy(brokers = self.brokers + (state.brokerId -> state))

  /** queries active known leader for given topic **/
  def leaderFor(topicName: String @@ TopicName, partition: Int @@ PartitionId): Option[BrokerData] =
    self.topics.get((topicName, partition)) flatMap { _.leader } flatMap { leaderId => brokers.get(leaderId) }

  /** invalidates leader if that leader is still the active leader **/
  def invalidateLeader(topicName: String @@ TopicName, partition: Int @@ PartitionId, leader: Int @@ Broker): ClientState = {
    self.topics.get((topicName, partition)) match {
      case None => self
      case Some(tap) =>
        if (! tap.leader.contains(leader)) self
        else self.copy(topics = self.topics + ((topicName, partition) -> tap.copy(leader = None)))

    }
  }


}

object ClientState {

  /** provides initial client state **/
  def initial:ClientState = ClientState(
    brokers = Map.empty
    , topics = Map.empty
  )


}



