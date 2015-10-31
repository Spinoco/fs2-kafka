package fs2.kafka.protocol

sealed trait Acknowledgment

object Acknowledgment {

  /** Producer won't get an acknowledgement. Message may be lost any time */
  object FireNForget extends Acknowledgment

  /** Only leader acks. If the leader dies, message may be lost **/
  object Leader extends Acknowledgment

  /** All replicas acknowledges the message, message will be never lost **/
  case class Replicas(count:Option[Int]) extends Acknowledgment

}
