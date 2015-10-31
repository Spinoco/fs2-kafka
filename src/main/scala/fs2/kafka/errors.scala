package fs2.kafka

/**
 * Kafka Errors
 */
object ErrorCode extends Enumeration{

  /** An unexpected server error **/
  val Unknown = Value(-1)
  /** The requested offset is outside the range of offsets maintained by the server for the given topic/partition. **/
  val OffsetOutOfRange = Value(	1)
  /** This indicates that a message contents does not match its CRC **/
  val InvalidMessage = Value(	2)
  /** This request is for a topic or partition that does not exist on this broker. **/
  val UnknownTopicOrPartition= Value(	3)
  /** The message has a negative size **/
  val InvalidMessageSize= Value(	4)
  /** This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes. **/
  val LeaderNotAvailable= Value(	5)
  /** This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date. **/
  val NotLeaderForPartition= Value(	6)
  /** This error is thrown if the request exceeds the user-specified time limit in the request. **/
  val RequestTimedOut= Value(	7)
  /** This is not a client facing error and is used mostly by tools when a broker is not alive.**/
  val BrokerNotAvailable= Value(	8)
  /** If replica is expected on a broker, but is not (this can be safely ignored). **/
  val ReplicaNotAvailable= Value(	9)
  /** The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum. **/
  val MessageSizeTooLarge	= Value(10)
  /** Internal error code for broker-to-broker communication. **/
  val StaleControllerEpochCode	= Value(11)
  /** If you specify a string larger than configured maximum for offset metadata **/
  val OffsetMetadataTooLargeCode	= Value(12)
  /** The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition). **/
  val OffsetsLoadInProgressCode	= Value(14)
  /** The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created. **/
  val ConsumerCoordinatorNotAvailableCode	= Value(15)
  /** The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for. **/
  val NotCoordinatorForConsumerCode	= Value(16)


}

