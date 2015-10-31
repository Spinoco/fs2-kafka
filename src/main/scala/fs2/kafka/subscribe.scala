package fs2.kafka

import scala.concurrent.duration.FiniteDuration


case class Subscription(topic: String, partition: Int, offset: SubscriptionOffset, waitOnTail: Option[TailCondition])

case class TailCondition(maxTime: Option[FiniteDuration], minBytes: Option[FiniteDuration])

sealed trait SubscriptionOffset

object SubscriptionOffset {

  case object Head extends SubscriptionOffset

  case object Tail extends SubscriptionOffset

  case class Exact(offset: Long) extends SubscriptionOffset

}
