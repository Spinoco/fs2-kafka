package spinoco.fs2.kafka.network

import java.net.InetSocketAddress
import java.util.Date

import cats.effect.IO
import fs2._
import scodec.bits.ByteVector
import shapeless.tag
import spinoco.protocol.kafka.Message.SingleMessage
import spinoco.protocol.kafka.Request._
import spinoco.protocol.kafka.Response._
import spinoco.protocol.kafka._
import spinoco.fs2.kafka.{KafkaRuntimeRelease, partition}

import scala.concurrent.duration._

/**
  * Specification to verify behaviour with Kafka 0.8
  * Note that this required local docker instance to work properly
  */
class BrokerConnection08SPec extends BrokerConnectionKafkaSpecBase {
  "Kafka 0.8.2" - {
    "Publish and subscribe message" in {
      val result =
      withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) { case (zkId, kafkaId) =>
        val createTopic =  Stream.eval_(createKafkaTopic(kafkaId,testTopic1))
        val publishOne = (Stream(
          RequestMessage(
            version = ProtocolVersion.Kafka_0_8
            , correlationId = 1
            , clientId = "test-publisher"
            , request = ProduceRequest(
              requiredAcks = RequiredAcks.LocalOnly
              , timeout = 10.seconds
              , messages = Vector((testTopic1, Vector((part0, Vector(SingleMessage(0l, MessageVersion.V0, None, ByteVector(1,2,3), ByteVector(5,6,7)))))))
            )
          )
        ) ++ S.sleep_[IO](1.minute))
        .through(BrokerConnection(new InetSocketAddress("127.0.0.1",9092)))
        .take(1).map(Left(_))

        val fetchOne =
          (Stream(RequestMessage(
            version = ProtocolVersion.Kafka_0_8
            , correlationId = 2
            , clientId = "test-subscriber"
            , request = FetchRequest(
              replica = tag[Broker](-1)
              , maxWaitTime = 1.second
              , minBytes = 1
              , maxBytes = None
              , topics = Vector((testTopic1, Vector((part0, tag[Offset](0), 10240))))
            )
          )) ++ S.sleep_[IO](1.minute))
          .through(BrokerConnection(new InetSocketAddress("127.0.0.1",9092)))
          .take(1).map(Right(_))


        createTopic ++ publishOne ++ fetchOne
      }.runLog.unsafeRunSync()


      result shouldBe Vector(
        Left(ResponseMessage(1, ProduceResponse(Vector((testTopic1,Vector((part0,PartitionProduceResult(None,tag[Offset](0),None))))), throttleTime = None)))
        , Right(ResponseMessage(2,FetchResponse(Vector((testTopic1, Vector(PartitionFetchResult(part0, None, tag[Offset](1), Vector(SingleMessage(0,MessageVersion.V0, None, ByteVector(1,2,3), ByteVector(5,6,7))))))), throttleTime = None)))
      )
    }

    "Fetch metadata for topics" in {
      val result =
        withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) { case (zkId, kafkaId) =>
          val createTopic1 = Stream.eval_(createKafkaTopic(kafkaId, testTopic1))
          val createTopic2 = Stream.eval_(createKafkaTopic(kafkaId, testTopic2))

          val fetchMeta =
            (Stream(RequestMessage(
              version = ProtocolVersion.Kafka_0_8
              , correlationId = 1
              , clientId = "test-subscriber"
              , request = MetadataRequest(Vector())
            )) ++ S.sleep_[IO](1.minute))
              .through(BrokerConnection(new InetSocketAddress("127.0.0.1",9092)))
              .take(1)

          createTopic1 ++ createTopic2  ++ fetchMeta

        }.runLog.unsafeRunSync()

      val metaResponse = result.collect { case ResponseMessage(1, meta:MetadataResponse) => meta }

      metaResponse.size shouldBe 1
      metaResponse.flatMap(_.brokers).size shouldBe 1
      metaResponse.flatMap(_.topics).size shouldBe 2
    }


    "Fetch offsets topics" in {
      val result =
        withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) { case (zkId, kafkaId) =>
          val createTopic1 = Stream.eval_(createKafkaTopic(kafkaId, testTopic1))

          val fetchOffsets=
            (Stream(RequestMessage(
              version = ProtocolVersion.Kafka_0_8
              , correlationId = 1
              , clientId = "test-subscriber"
              , request = OffsetsRequest(tag[Broker](-1), Vector((testTopic1, Vector((partition(0), new Date(-1), Some(10))))))
            )) ++ S.sleep_[IO](1.minute))
              .through(BrokerConnection(new InetSocketAddress("127.0.0.1",9092)))
              .take(1)

          createTopic1 ++ fetchOffsets

        }.runLog.unsafeRunSync()

      val offsetResponse = result.collect { case ResponseMessage(1, offset:OffsetResponse) => offset }

      offsetResponse.size shouldBe 1
      offsetResponse.flatMap(_.data) shouldBe Vector(
        (testTopic1, Vector(PartitionOffsetResponse(partition(0), None, new Date(0), Vector(tag[Offset](0)))))
      )
    }



  }




}
