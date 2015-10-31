package fs2.kafka.protocol

import fs2.kafka.{ErrorCode, OffsetQueryResponse, TopicMetadata}


case class Response(id: Int, tpe: ResponseType)


sealed trait ResponseType

object ResponseType {

  case class TopicMetadataResponse(data: TopicMetadata) extends ResponseType

  case class ProduceResponse(topics: Map[String, Map[Int, Either[ErrorCode.Value, Long]]]) extends ResponseType

  case class FetchResponse(topic: Map[String, Map[Int, Either[ErrorCode.Value, TopicFetchResult]]]) extends ResponseType

  case class OffsetResponse(response: OffsetQueryResponse) extends ResponseType

}


case class TopicFetchResult(highWM: Long, messages: Seq[Message])
