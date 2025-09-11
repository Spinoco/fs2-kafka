package spinoco.fs2.kafka.network

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.comcast.ip4s.{Host, Port, SocketAddress}
import fs2._
import scodec.bits.ByteVector
import shapeless.tag
import spinoco.fs2.kafka.partition
import spinoco.protocol.kafka.Message.SingleMessage
import spinoco.protocol.kafka.Request._
import spinoco.protocol.kafka.Response._
import spinoco.protocol.kafka._

import java.util.Date
import scala.concurrent.duration._

/**
  * Created by pach on 11/09/16.
  */
class BrokerConnection10Spec extends BrokerConnectionKafkaSpecBase {
  "Kafka 0.10.0" - {
    "Publish and subscribe message" in {
      val result =
        withKafkaSingle{ _ =>
          val createTopic = Stream.exec(createKafkaTopicScript(testTopic1))
          val publishOne = (Stream(
            RequestMessage(
              version = ProtocolVersion.Kafka_0_10
              , correlationId = 1
              , clientId = "test-publisher"
              , request = ProduceRequest(
                requiredAcks = RequiredAcks.LocalOnly
                , timeout = 10.seconds
                , messages = Vector((testTopic1, Vector((part0, Vector(SingleMessage(0L, MessageVersion.V0, None, ByteVector(1, 2, 3), ByteVector(5, 6, 7)))))))
              )
            )
          ) ++ Stream.sleep_[IO](1.minute))
            .through(BrokerConnection.mk(SocketAddress(Host.fromString("172.30.0.11").get, Port.fromInt(9092).get)))
            .take(1).map(Left(_))

          val fetchOne =
            (Stream(RequestMessage(
              version = ProtocolVersion.Kafka_0_10
              , correlationId = 2
              , clientId = "test-subscriber"
              , request = FetchRequest(
                replica = tag[Broker](-1)
                , maxWaitTime = 1.second
                , minBytes = 1
                , maxBytes = None
                , topics = Vector((testTopic1, Vector((part0, tag[Offset](0), 10240))))
              )
            )) ++ Stream.sleep_[IO](1.minute))
              .through(BrokerConnection.mk(SocketAddress(Host.fromString("172.30.0.11").get, Port.fromInt(9092).get)))
              .take(1).map(Right(_))


          createTopic ++ publishOne ++ fetchOne
        }.compile.toVector.unsafeRunSync()

      result shouldBe Vector(
        Left(ResponseMessage(1, ProduceResponse(Vector((testTopic1, Vector((part0, PartitionProduceResult(None, tag[Offset](0), None))))), throttleTime = Some(0.millis))))
        , Right(ResponseMessage(2, FetchResponse(Vector((testTopic1, Vector(PartitionFetchResult(part0, None, tag[Offset](1), Vector(SingleMessage(0, MessageVersion.V1, None, ByteVector(1, 2, 3), ByteVector(5, 6, 7))))))), throttleTime = Some(0.millis))))
      )

    }


    "Fetch metadata for topics" in {
      val result =
        withKafkaSingle { _ =>
          val createTopic1 = Stream.exec(createKafkaTopicScript(testTopic1))
          val createTopic2 = Stream.exec(createKafkaTopicScript(testTopic2))

          val fetchMeta =
            (Stream(RequestMessage(
              version = ProtocolVersion.Kafka_0_8
              , correlationId = 1
              , clientId = "test-subscriber"
              , request = MetadataRequest(Vector())
            )) ++ Stream.sleep_[IO](1.minute))
              .through(BrokerConnection.mk(SocketAddress(Host.fromString("172.30.0.11").get, Port.fromInt(9092).get)))
              .take(1)

          createTopic1 ++ createTopic2 ++ fetchMeta

        }.compile.toVector.unsafeRunSync()

      val metaResponse = result.collect { case ResponseMessage(1, meta:MetadataResponse) => meta }

      metaResponse.size shouldBe 1
      metaResponse.flatMap(_.brokers).size shouldBe 1
      metaResponse.flatMap(_.topics).size shouldBe 2
    }


    "Fetch offsets topics" in {
      val result =
        withKafkaSingle { _ =>
          val createTopic1 = Stream.exec(createKafkaTopicScript(testTopic1))

          val fetchOffsets=
            (Stream(RequestMessage(
              version = ProtocolVersion.Kafka_0_8
              , correlationId = 1
              , clientId = "test-subscriber"
              , request = OffsetsRequest(tag[Broker](-1), Vector((testTopic1, Vector((partition(0), new Date(-1), Some(10))))))
            )) ++ Stream.sleep_[IO](1.minute))
              .through(BrokerConnection.mk(SocketAddress(Host.fromString("172.30.0.11").get, Port.fromInt(9092).get)))
              .take(1)

          createTopic1 ++ fetchOffsets

        }.compile.toVector.unsafeRunSync()

      val offsetResponse = result.collect { case ResponseMessage(1, offset:OffsetResponse) => offset }

      offsetResponse.size shouldBe 1
      offsetResponse.flatMap(_.data) shouldBe Vector(
        (testTopic1, Vector(PartitionOffsetResponse(partition(0), None, new Date(0), Vector(tag[Offset](0)))))
      )
    }


  }
}
