package spinoco.fs2.kafka.failure


import shapeless.tag.@@
import spinoco.fs2.kafka.network.BrokerAddress
import spinoco.protocol.kafka._

/**
  * Various errors that are eventually to be processed to user code.
  */


/** Broker responded with invalid response **/
case class InvalidBrokerResponse(address: BrokerAddress, expected: String, request: Request, got: Option[Response]) extends Throwable(s"Broker[$address]: Invalid response to $request, expected: $expected, got: $got")

/** Given topic // partition has no leader available **/
case class LeaderNotAvailable(topicName: String @@ TopicName, partition: Int @@ PartitionId) extends Throwable(s"Topic: $topicName, partition: $partition has no leader available")

/** There are no brokers available **/
case object NoBrokerAvailable extends Throwable("No Broker available")

/** Connection with broker has been terminated with given exception **/
case class BrokerRequestFailure(address: BrokerAddress,request: Request, thrown: Throwable) extends Throwable(s"Broker[$address]: Request failed", thrown)

/** for supplied request broker has reported failure when respoding **/
case class BrokerReportedFailure(address: BrokerAddress, request: Request, failure: ErrorType.Value) extends Throwable(s"Broker[$address]: Request reported failure: $failure")

/** for supplied request broker responded with unexpected response **/
case class UnexpectedResponse(address: BrokerAddress, response: ResponseMessage) extends Throwable(s"Unexpected response received from $address: $response")

/** Publish to client that has been terminate already **/
case object ClientTerminated extends Throwable("Kafka client was terminated")
