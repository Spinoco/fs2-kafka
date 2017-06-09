package spinoco.fs2.kafka

import fs2.{Stream, Task, time}
import scodec.bits.ByteVector
import spinoco.protocol.kafka.ProtocolVersion

import scala.concurrent.duration._


class KafkaClusterPublish_0802_P_08_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_8_2_0, ProtocolVersion.Kafka_0_8)

// 0.9 version failing to start cluster. Need to fix this
//class KafkaClusterPublish_0901_P_08_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_9_0_1, ProtocolVersion.Kafka_0_8)
//class KafkaClusterPublish_0901_P_09_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_9_0_1, ProtocolVersion.Kafka_0_9)

class KafkaClusterPublish_1000_P_08_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_8)
class KafkaClusterPublish_1000_P_09_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_9)
class KafkaClusterPublish_1000_P_10_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_10)

class KafkaClusterPublish_1001_P_08_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_8)
class KafkaClusterPublish_1001_P_09_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_9)
class KafkaClusterPublish_1001_P_10_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_10)
class KafkaClusterPublish_1001_P_101_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_10_1)

class KafkaClusterPublish_1002_P_08_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_8)
class KafkaClusterPublish_1002_P_09_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_9)
class KafkaClusterPublish_1002_P_10_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10)
class KafkaClusterPublish_1002_P_101_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10_1)
class KafkaClusterPublish_1002_P_102_Spec extends KafkaClusterPublish(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10_2)

/**
  * Created by pach on 06/06/17.
  */
class KafkaClusterPublish (val runtime: KafkaRuntimeRelease.Value, val protocol: ProtocolVersion.Value) extends Fs2KafkaRuntimeSpec {

  val version = s"$runtime[$protocol]"
 
  s"$version: cluster" - {


    "publish-response" in {
      def publish(kc: KafkaClient[Task]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publish1(testTopicA, part0, ByteVector(1),  ByteVector(idx), requireQuorum = true, serverAckTimeout = 3.seconds)
        } map (Left(_))
      }

      ((withKafkaCluster(runtime) flatMap { nodes =>
        time.sleep(3.second) >>
        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA)) >> {
          KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
            awaitLeaderAvailable(kc, testTopicA, part0) >>
            publish(kc) ++
              (kc.subscribe(testTopicA, part0, offset(0l)) map (Right(_)))
          } take 20
        }
      } runLog  ) unsafeRun) shouldBe
        (for { idx <- 0 until 10} yield Left(offset(idx))).toVector ++
          (for { idx <- 0 until 10} yield Right(TopicMessage(offset(idx), ByteVector(1), ByteVector(idx), offset(10)))).toVector
    }


  }

}
