package fs2.kafka


sealed trait PublishMessage

object PublishMessage {

  case class PublishOne(
    topic: String
    , partition: Int
    , content: MessageContent
  ) extends PublishMessage

  case class PublishChunk(
    messages: Seq[PublishOne]
  ) extends PublishMessage

}