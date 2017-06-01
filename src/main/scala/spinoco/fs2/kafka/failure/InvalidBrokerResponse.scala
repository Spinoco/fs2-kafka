package spinoco.fs2.kafka.failure


import shapeless.tag.@@
import spinoco.fs2.kafka.state.BrokerAddress
import spinoco.protocol.kafka._

/**
  * Various errors that are eventually to be processed to user code.
  */


case class InvalidBrokerResponse(address: BrokerAddress, expected: String, request: Request, got: Option[Response]) extends Throwable(s"Broker[$address]: Invalid response to $request, expected: $expected, got: $got")

case class LeaderNotAvailable(topicName: String @@ TopicName, partition: Int @@ PartitionId) extends Throwable(s"Topic: $topicName, partition: $partition has no leader available")

case object NoBrokerAvailable extends Throwable("No Broker available")

case class BrokerRequestFailure(address: BrokerAddress,request: Request, thrown: Throwable) extends Throwable(s"Broker[$address]: Request failed", thrown)

case class BrokerReportedFailure(address: BrokerAddress, request: Request, failure: ErrorType.Value) extends Throwable(s"Broker[$address]: Request reported failure: $failure")
