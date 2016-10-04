package spinoco.fs2.kafka.state

import java.util.Date

import shapeless.tag._
import spinoco.protocol.kafka.{Broker, PartitionId, TopicName}

/**
  * State of topic and partition
  *
  *
  * @param topic            Name of the topic
  * @param partition        Name of the partition
  * @param leader           Leader of this partition. This is where "publish" and "metadata" requests are sent
  * @param followers        All ISRs (followers) for the topic and partitions with their state. This also
  *                         includes tailSubscriber but not leader, only case when this includes leader is
  *                         that leader is only ISR for this topic and partition.
  */
case class TopicAndPartitionState[F[_]](
  topic: String @@ TopicName
  , partition: Int @@ PartitionId
  , leader: Option[Int @@ Broker]
  , followers: Map[Int @@ Broker, FollowerState[F]]
)



object FollowerState {


  /**
    * Indicates, that the follower is fully operational and also includes the number of connections active on this follower.
    *
    * @param brokerId       Id of the broker
    * @tparam F
    */
  case class Operational[F[_]](
    brokerId: Int @@ Broker
  ) extends FollowerState[F]

  /**
    * Broker failed to establish one of its connections. We put broker to back off until given time, before trying
    * to use it for any new connections. Established connections may operate normally, if they have not failed yet.
    * @param brokerId         Id of the broker
    * @param backOffUntil     Until when this broker has to be held from any new connection attempts
    * @tparam F
    */
  case class Failed[F[_]](
    brokerId: Int @@ Broker
    , backOffUntil: Long @@ Date
  ) extends FollowerState[F]

}



sealed trait FollowerState[F[_]]