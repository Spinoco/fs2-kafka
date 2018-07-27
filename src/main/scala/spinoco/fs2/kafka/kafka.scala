package spinoco.fs2


import java.nio.channels.AsynchronousChannelGroup

import cats.effect.{ConcurrentEffect, Timer}
import fs2._
import scodec.bits.ByteVector
import shapeless.tag
import shapeless.tag._

import spinoco.fs2.kafka.network.BrokerAddress
import spinoco.protocol.kafka.{Offset, PartitionId, ProtocolVersion, TopicName}


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

  /**
    * Build a stream, that when run will produce single kafka client.
    *
    * Initially client spawns connections to nodes specified in ensemble and queries them for the topology.
    * After topology is known, it then initiates connection to each Kafka Broker listed in topology.
    * That connection is then used to publish messages to topic/partition that given broker is leader of.
    *
    * For the subscription client always initiate separate connections to 'followers'. Only in such case there is
    * no ISR (follower) available client initiate subscribe connection to 'leader'.
    *
    *
    * Client automatically reacts and recovers from any topology changes that may occur in ensemble:
    *   - When the leader is changed, the publish requests goes to newly designated leader.
    *   - When follower dies, or changes its role as leader, then subsequent reads are sent to another follower, if available.
    *
    *
    * @param ensemble     Ensemble to connect to.  Must not be empty.
    * @param protocol     Protocol that will be used for requests. This shall be lowest common protocol supported by all brokers.
    * @param clientName   Name of the client. Name is suffixed for different type of connections to broker:
    *                     - initial-meta-rq : Initial connection to query all available brokers
    *                     - control : Control connection where publish requests and maetadat requests are sent to
    *                     - fetch: Connection where fetch requests are sent to.
    */
  def client[F[_] : Logger : ConcurrentEffect : Timer](
    ensemble: Set[BrokerAddress]
    , protocol: ProtocolVersion.Value
    , clientName: String
  )(implicit AG: AsynchronousChannelGroup):Stream[F,KafkaClient[F]] =
    KafkaClient(ensemble, protocol, clientName)


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
