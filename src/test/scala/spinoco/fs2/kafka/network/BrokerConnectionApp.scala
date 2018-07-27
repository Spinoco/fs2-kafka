package spinoco.fs2.kafka.network

import java.net.InetSocketAddress

import fs2._
import cats.effect.IO
import scodec.bits.ByteVector
import shapeless.tag
import spinoco.protocol.kafka.Request.RequiredAcks
import spinoco.protocol.kafka._

import scala.concurrent.duration._

/**
  * Created by pach on 28/08/16.
  */
object BrokerConnectionApp extends App {

  import spinoco.fs2.kafka.Fs2KafkaClientResources._

  def metadata = {
    val source =
      Stream[IO, RequestMessage](
        RequestMessage(
          version = ProtocolVersion.Kafka_0_8
          , correlationId = 1
          , clientId = "manager"
          , request = Request.MetadataRequest(Vector.empty)
        )
      ) ++ Stream.sleep_[IO](10.seconds)

    source.through(BrokerConnection(
      address = new InetSocketAddress("127.0.0.1", 9092)
    ))
    .evalMap(rcv => IO {
      println(rcv)
    })
    .compile.drain.unsafeRunSync()
  }

  def publish = {
    val source =
      Stream[IO, RequestMessage](
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
      ) ++ Stream.sleep_[IO](10.seconds)

    source.through(BrokerConnection(
      address = new InetSocketAddress("127.0.0.1", 9092)
    ))
    .evalMap(rcv => IO {
      println(rcv)
    })
    .compile.drain.unsafeRunSync()

  }

  def fetch = {
    val source =
      Stream[IO, RequestMessage](
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
      ) ++ Stream.sleep_[IO](10.seconds)

    source.through(BrokerConnection(
      address = new InetSocketAddress("127.0.0.1", 9092)
    ))
    .evalMap(rcv => IO {
      println(rcv)
    })
    .compile.drain.unsafeRunSync()
  }

  //metadata
  //publish
  fetch
}
