package spinoco.fs2.kafka


import fs2._
import scodec.bits.ByteVector
import shapeless.tag
import spinoco.protocol.kafka._

import scala.concurrent.duration._



class KafkaClientLastOffset0802_P_08_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_8_2_0, ProtocolVersion.Kafka_0_8)

class KafkaClientLastOffset0901_P_08_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_9_0_1, ProtocolVersion.Kafka_0_8)
class KafkaClientLastOffset0901_P_09_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_9_0_1, ProtocolVersion.Kafka_0_9)

class KafkaClientLastOffset1000_P_08_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_8)
class KafkaClientLastOffset1000_P_09_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_9)
class KafkaClientLastOffset1000_P_10_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_10)

class KafkaClientLastOffset1001_P_08_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_8)
class KafkaClientLastOffset1001_P_09_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_9)
class KafkaClientLastOffset1001_P_10_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_10)
class KafkaClientLastOffset1001_P_101_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_10_1)

class KafkaClientLastOffset1002_P_08_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_8)
class KafkaClientLastOffset1002_P_09_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_9)
class KafkaClientLastOffset1002_P_10_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10)
class KafkaClientLastOffset1002_P_101_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10_1)
class KafkaClientLastOffset1002_P_102_Spec extends KafkaClientLastOffset(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10_2)

abstract class KafkaClientLastOffset(val runtime: KafkaRuntimeRelease.Value, val protocol: ProtocolVersion.Value) extends Fs2KafkaRuntimeSpec {

  val version = s"$runtime[$protocol]"


  s"$version: Last Offset (single broker)" - {

    "queries when topic is empty"  in {
      ((withKafkaSingleton(runtime) flatMap { case (zkDockerId, kafkaDockerId) =>
          Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >> {
            KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
              Stream.eval(kc.offsetRangeFor(testTopicA, tag[PartitionId](0)))
            }
          }
        } runLog ) unsafeRun) shouldBe Vector((offset(0), offset(0)))
    }


    "queries when topic is non-empty" in {
      ((withKafkaSingleton(runtime) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
        KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
          Stream.eval(kc.publish1(testTopicA, part0, ByteVector(1, 2, 3), ByteVector(5, 6, 7), false, 10.seconds)) >>
          Stream.eval(kc.offsetRangeFor(testTopicA, tag[PartitionId](0)))
        }

      } runLog ) unsafeRun) shouldBe Vector((offset(0), offset(1)))
    }


  }


}
