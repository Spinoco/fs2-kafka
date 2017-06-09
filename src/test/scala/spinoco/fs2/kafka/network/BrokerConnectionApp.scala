package spinoco.fs2.kafka.network

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

import fs2._
import fs2.Stream
import scodec.bits.ByteVector
import shapeless.tag
import spinoco.protocol.kafka.Request.RequiredAcks
import spinoco.protocol.kafka._

import scala.concurrent.duration._

/**
  * Created by pach on 28/08/16.
  */
object BrokerConnectionApp extends App {
  implicit val S = Strategy.fromFixedDaemonPool(10)
  implicit val SCh = Scheduler.fromFixedDaemonPool(10)
  implicit val AG = AsynchronousChannelGroup.withFixedThreadPool(10, new ThreadFactory{
    val idx = new AtomicInteger(0)
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r, s"achg-${idx.incrementAndGet() }")
      t.setDaemon(true)
      t
    }
  })

  def metadata = {
    val source =
      Stream[Task, RequestMessage](
        RequestMessage(
          version = ProtocolVersion.Kafka_0_8
          , correlationId = 1
          , clientId = "manager"
          , request = Request.MetadataRequest(Vector.empty)
        )
      ) ++ time.sleep_[Task](10.seconds)

    source.through(BrokerConnection(
      address = new InetSocketAddress("127.0.0.1", 9092)
    ))
      .evalMap(rcv => Task.delay {
        println(rcv)
      })
      .run.unsafeRun()
  }

  def publish = {
    val source =
      Stream[Task, RequestMessage](
        RequestMessage(
          version = ProtocolVersion.Kafka_0_8
          , correlationId = 2
          , clientId = "publisher"
          , request = Request.ProduceRequest(
            requiredAcks = RequiredAcks.LocalOnly
            , timeout = 10.seconds
            , messages = Vector(
              (tag[TopicName]("test"), Vector(
                (tag[PartitionId](0), Vector(
                  Message.SingleMessage(0l,MessageVersion.V0,None,ByteVector(1,2,3), ByteVector(5,6,7,8))
                ))
              ))
            )
          )
        )
      ) ++ time.sleep_[Task](10.seconds)

    source.through(BrokerConnection(
      address = new InetSocketAddress("127.0.0.1", 9092)
    ))
      .evalMap(rcv => Task.delay {
        println(rcv)
      })
      .run.unsafeRun()

  }

  def fetch = {
    val source =
      Stream[Task, RequestMessage](
        RequestMessage(
          version = ProtocolVersion.Kafka_0_8
          , correlationId = 2
          , clientId = "fetcher"
          , request = Request.FetchRequest(
            replica = tag[Broker](-1)
            , maxWaitTime = 10.seconds
            , minBytes = 0
            , maxBytes = None
            , topics = Vector((tag[TopicName]("test"), Vector(
              (tag[PartitionId](0), tag[Offset](0), 1024*1024)
            )))
          )
        )
      ) ++ time.sleep_[Task](10.seconds)

    source.through(BrokerConnection(
      address = new InetSocketAddress("127.0.0.1", 9092)
    ))
      .evalMap(rcv => Task.delay {
        println(rcv)
      })
      .run.unsafeRun()
  }

  //metadata
  //publish
  fetch
}
