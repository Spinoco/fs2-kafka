package fs2.kafka.protocol

import fs2.kafka.OffsetQuery

import scala.concurrent.duration.FiniteDuration


case class Request(
  id: Int
  , clientId: String
  , tpe: RequestType
)


sealed trait RequestType

object RequestType {

  case class TopicMetadata(topics: Set[String]) extends RequestType

  case class Produce(ack: Acknowledgment, timeout: FiniteDuration, partition: Int, messages: Map[String, Map[Int, Seq[Message]]]) extends RequestType

  case class Fetch(replica: Int, timeout: FiniteDuration, minBytes: Int, topics: Map[String, Map[Int, FetchPartition]]) extends RequestType

  case class GetOffset(query:OffsetQuery) extends RequestType

}


case class FetchPartition(offset: Long, maxBytes: Int)

