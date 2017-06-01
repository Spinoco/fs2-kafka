package spinoco.fs2.kafka


import fs2._
import scodec.bits.ByteVector
import shapeless.tag
import spinoco.fs2.kafka.network.BrokerConnection
import spinoco.fs2.kafka.state.BrokerAddress
import spinoco.protocol.kafka.Message.SingleMessage
import spinoco.protocol.kafka.Request.{ProduceRequest, RequiredAcks}
import spinoco.protocol.kafka._

import scala.concurrent.duration._

/**
  * Created by pach on 29/05/17.
  */
class KafkaClientLastOffsetSpec extends Fs2KafkaRuntimeSpec {


  def publishOneMessage(address: BrokerAddress, k: ByteVector, v: ByteVector): Task[Unit] = {
   address.toInetSocketAddress flatMap { inetAddress =>
     Stream(
       RequestMessage(
         version = ProtocolVersion.Kafka_0_9
         , correlationId = 1
         , clientId = "test-publisher"
         , request = ProduceRequest(
           requiredAcks = RequiredAcks.LocalOnly
           , timeout = 10.seconds
           , messages = Vector((testTopicA, Vector((part0, Vector(SingleMessage(0l, MessageVersion.V0, None, k, v))))))
         )
       )
     ).through(BrokerConnection(inetAddress))
     .run
   }
  }


  "Last Offset (single broker)" - {

    "queries when topic is empty"  in {
      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
          Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >> {
            KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
              Stream.eval(kc.offsetRangeFor(testTopicA, tag[PartitionId](0)))
            }
          }
        } runLog ) unsafeRun) shouldBe Vector(offset(-1))
    }


    "queries when topic is non-empty" in {
      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
        Stream.eval(publishOneMessage(localBroker1_9092, ByteVector(1, 2, 3), ByteVector(5, 6, 7))) >> {
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
            Stream.eval(kc.offsetRangeFor(testTopicA, tag[PartitionId](0)))
          }
        }
      } runLog ) unsafeRun) shouldBe Vector(offset(-1))
    }


  }


}
