package fs2.kafka

import java.net.Inet4Address
import java.nio.channels.AsynchronousChannelGroup


import scala.concurrent.duration.FiniteDuration
import scalaz.concurrent.{Strategy, Task}
import scalaz.stream.{Process,Sink, Channel}


/**
 * Created by pach on 31/10/15.
 */
sealed trait Client {

  /**
   * Publishes the message (or chunk of messages) to kafka and awaits confirmation
   * @param replicas  if None, only leader confirms the publish. If defined the at least that
   *                  amount if ISRs must confirm receipt of the message, unless the number is larger than number of
   *                  available ISRs
   * @return
   */
  def publishAck(replicas: Option[Int]): Channel[Task, (FiniteDuration, PublishMessage), Process[Task, TopicMessage]] = ???

  /**
   * Publishes the message (or chunk of messages) and awaits no acknowledgement
   */
  def publish: Sink[Task, PublishMessage] = ???

  /**
   * Subscribes to receive messages on topic specified by `Subscription`
   */
  def subscribe: Channel[Task, Subscription, Process[Task, TopicMessage]] = ???


  /** Requests metadata from kafka **/
  def metadata:Channel[Task,Set[String],MetadataResponse] = ???


  /** Queries offsets for topics and their partitions **/
  def offsets:Channel[Task,OffsetQuery,OffsetQueryResponse] = ???

}


object Client {

  /**
   * Creates the client. Client releases all its resources when returned process stops.
   */
  def apply(
    brokers: Seq[Inet4Address]
  )(implicit S: Strategy, AG: AsynchronousChannelGroup): Process[Task, Client] = ???


}





