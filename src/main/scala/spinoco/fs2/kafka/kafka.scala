package spinoco.fs2


import scodec.bits.ByteVector
import shapeless.tag
import shapeless.tag._
import spinoco.fs2.kafka.network.BrokerAddress
import spinoco.protocol.kafka.{Offset, PartitionId, TopicName}


package object kafka {

  /**
    * Message read from the topic.
    * @param offset     Offset of the message
    * @param key        Key of the message
    * @param message    Message content
    * @param tail       Offset of last message in the topic
    */
  case class TopicMessage(offset: Long @@ Offset, key: ByteVector, message: ByteVector, tail: Long @@ Offset)

  type TopicAndPartition = (String @@ TopicName, Int @@ PartitionId)


  /** types correctly name of the topic **/
  def topic(name: String): String @@ TopicName = tag[TopicName](name)

  /** types correctly id of the partition**/
  def partition(id: Int): Int @@ PartitionId = tag[PartitionId](id)

  /** types the offset in the topic**/
  def offset(offset: Long): Long @@ Offset = tag[Offset](offset)

  /** Starting from this offset will assure that we will read always from very oldest message (head) kept in topic **/
  val HeadOffset = offset(0)

  /** Starting from this offset will assure we starting with most recent messages written to topic (tail) **/
  val TailOffset = offset(Long.MaxValue)

  /** syntax helper to construct broker address **/
  def broker(host: String, port: Int): BrokerAddress = BrokerAddress(host, port)

}
