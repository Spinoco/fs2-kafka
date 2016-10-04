package spinoco.fs2.kafka.network

import java.net.InetSocketAddress

import fs2._
import scodec.bits.ByteVector
import shapeless.tag
import spinoco.protocol.kafka.Message.SingleMessage
import spinoco.protocol.kafka.Request.{FetchRequest, MetadataRequest, ProduceRequest, RequiredAcks}
import spinoco.protocol.kafka.Response._
import spinoco.protocol.kafka._

import scala.concurrent.duration._

/**
  * Created by pach on 11/09/16.
  */
class BrokerConnection09Spec extends BrokerConnectionKafkaSpecBase {
  "Kafka 0.9.0" - {
    "Publish and subscribe message" in {
      val result =
        withKafka8Singleton(KafkaRuntimeRelease.V_0_9_0_1).flatMap { case (zkId, kafkaId) =>
          val createTopic = Stream.eval_(createKafkaTopic(kafkaId, testTopic1))
          val publishOne = (Stream(
            RequestMessage(
              version = ProtocolVersion.Kafka_0_9
              , correlationId = 1
              , clientId = "test-publisher"
              , request = ProduceRequest(
                requiredAcks = RequiredAcks.LocalOnly
                , timeout = 10.seconds
                , messages = Vector((testTopic1, Vector((part0, Vector(SingleMessage(0l, MessageVersion.V0, None, ByteVector(1, 2, 3), ByteVector(5, 6, 7)))))))
              )
            )
          ) ++ time.sleep(1.minute))
            .through(BrokerConnection(new InetSocketAddress("127.0.0.1", 9092)))
            .take(1).map(Left(_))

          val fetchOne =
            (Stream(RequestMessage(
              version = ProtocolVersion.Kafka_0_9
              , correlationId = 2
              , clientId = "test-subscriber"
              , request = FetchRequest(
                replica = tag[Broker](-1)
                , maxWaitTime = 1.second
                , minBytes = 1
                , topics = Vector((testTopic1, Vector((part0, tag[Offset](0), 10240))))
              )
            )) ++ time.sleep(1.minute))
              .through(BrokerConnection(new InetSocketAddress("127.0.0.1", 9092)))
              .take(1).map(Right(_))


          createTopic ++ publishOne ++ fetchOne
        }.runLog.unsafeRun()

      result shouldBe Vector(
        Left(ResponseMessage(1, ProduceResponse(Vector((testTopic1, Vector((part0, PartitionProduceResult(None, tag[Offset](0), None))))), throttleTime = Some(0.millis))))
        , Right(ResponseMessage(2, FetchResponse(Vector((testTopic1, Vector(PartitionFetchResult(part0, None, tag[Offset](1), Vector(SingleMessage(0, MessageVersion.V0, None, ByteVector(1, 2, 3), ByteVector(5, 6, 7))))))), throttleTime = Some(0.millis))))
      )

    }


    "Fetch metadata for topics" in {
      val result =
        withKafka8Singleton(KafkaRuntimeRelease.V_8_2_0).flatMap { case (zkId, kafkaId) =>
          val createTopic1 = Stream.eval_(createKafkaTopic(kafkaId, testTopic1))
          val createTopic2 = Stream.eval_(createKafkaTopic(kafkaId, testTopic2))

          val fetchMeta =
            (Stream(RequestMessage(
              version = ProtocolVersion.Kafka_0_8
              , correlationId = 1
              , clientId = "test-subscriber"
              , request = MetadataRequest(Vector())
            )) ++ time.sleep(1.minute))
              .through(BrokerConnection(new InetSocketAddress("127.0.0.1",9092)))
              .take(1)

          createTopic1 ++ createTopic2  ++ fetchMeta

        }.runLog.unsafeRun()

      val metaResponse = result.collect { case ResponseMessage(1, meta:MetadataResponse) => meta }

      metaResponse.size shouldBe 1
      metaResponse.flatMap(_.brokers).size shouldBe 1
      metaResponse.flatMap(_.topics).size shouldBe 2
    }
  }
}
