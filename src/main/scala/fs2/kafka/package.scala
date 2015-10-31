package fs2

import scodec.bits.ByteVector


package object kafka {

  /** message K->V **/
  type MessageContent = (ByteVector, ByteVector)



  /** message received from topic **/
  case class TopicMessage(topic: String, partition: Int, offset: Long, content: MessageContent)




}
