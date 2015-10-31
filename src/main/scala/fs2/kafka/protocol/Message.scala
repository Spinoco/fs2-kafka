package fs2.kafka.protocol

import fs2.kafka.MessageContent


case class Message(
  offset: Long
  , crc: Int
  , compression: Option[CompressionCodec]
  , content: Either[Seq[Message], MessageContent]
)


sealed trait CompressionCodec

object CompressionCodec {

  object GZip extends CompressionCodec

  object Snappy extends CompressionCodec

}