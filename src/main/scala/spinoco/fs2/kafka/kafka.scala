package spinoco.fs2

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup

import fs2._
import fs2.util.Async
import shapeless.tag._
import spinoco.protocol.kafka.{PartitionId, ProtocolVersion, TopicName}


package object kafka {

  /**
    * Message read from the topic.
    * @param offset     Offset of the message
    * @param key        Key of the message
    * @param message    Message content
    */
  case class TopicMessage(offset:Long,key:Chunk.Bytes,message:Chunk.Bytes)

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
  def client[F[_]](
    ensemble:Set[InetSocketAddress]
    , protocol: ProtocolVersion.Value
    , clientName: String
  )(implicit AG: AsynchronousChannelGroup, F:Async[F], S: Scheduler, L: Logger[F]):Stream[F,KafkaClient[F]] =
    KafkaClient(ensemble, protocol, clientName)




}
