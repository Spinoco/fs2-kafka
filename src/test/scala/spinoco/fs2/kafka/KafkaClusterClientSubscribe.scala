package spinoco.fs2.kafka

import fs2._
import spinoco.protocol.kafka.ProtocolVersion

import scala.concurrent.duration._


class KafkaClientClusterSubscribe_0802_P_08_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_8_2_0, ProtocolVersion.Kafka_0_8)

// Kafka 9 has problem to reliably start cluster - need to find a way how to fix this.
//class KafkaClientClusterSubscribe_0901_P_08_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_9_0_1, ProtocolVersion.Kafka_0_8)
//class KafkaClientClusterSubscribe_0901_P_09_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_9_0_1, ProtocolVersion.Kafka_0_9)

class KafkaClientClusterSubscribe_1000_P_08_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_8)
class KafkaClientClusterSubscribe_1000_P_09_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_9)
class KafkaClientClusterSubscribe_1000_P_10_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_10)

class KafkaClientClusterSubscribe_1001_P_08_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_8)
class KafkaClientClusterSubscribe_1001_P_09_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_9)
class KafkaClientClusterSubscribe_1001_P_10_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_10)
class KafkaClientClusterSubscribe_1001_P_101_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_10_1)

class KafkaClientClusterSubscribe_1002_P_08_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_8)
class KafkaClientClusterSubscribe_1002_P_09_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_9)
class KafkaClientClusterSubscribe_1002_P_10_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10)
class KafkaClientClusterSubscribe_1002_P_101_Spec extends KafkaClusterClientSubscribe(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10_1)

/**
  * Created by pach on 06/06/17.
  */
abstract class KafkaClusterClientSubscribe (val runtime: KafkaRuntimeRelease.Value, val protocol: ProtocolVersion.Value) extends Fs2KafkaRuntimeSpec {

  val version = s"$runtime[$protocol]"

  s"$version cluster" - {

    "subscribe-at-zero" in {
      ((withKafkaCluster(runtime) flatMap { nodes =>

        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
          KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
          time.sleep(3.second) >>
            Stream.eval(publishNMessages(kc, 0, 20, quorum = true)) >>
            kc.subscribe(testTopicA, part0, HeadOffset)
        } take 10
      } runLog ) unsafeRun) shouldBe generateTopicMessages(0, 10, 20)

    }


    "subscribe-at-tail" in {

      ((withKafkaCluster(runtime) flatMap { nodes =>

        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
          KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
          time.sleep(3.second) >>
            Stream.eval(publishNMessages(kc, 0, 20, quorum = true)) >>
            concurrent.join(Int.MaxValue)(Stream(
              kc.subscribe(testTopicA, part0, TailOffset)
              , time.sleep_(3.second) ++ Stream.eval_(publishNMessages(kc, 20, 30, quorum = true))
            ))
        } take 10
      } runLog ) unsafeRun).map { _.copy(tail = offset(30)) }  shouldBe generateTopicMessages(20, 30, 30)


    }



    "recovers from leader-failure" in {

      ((withKafkaCluster(runtime) flatMap { nodes =>

        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
        KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
          time.sleep(3.second) >>
          Stream.eval(publishNMessages(kc, 0, 20, quorum = true)) >>
          concurrent.join(Int.MaxValue)(Stream(
            kc.subscribe(testTopicA, part0, HeadOffset)
            , time.sleep_(3.seconds) ++ killLeader(kc, nodes, testTopicA, part0)
            , time.sleep_(10.second) ++ Stream.eval_(publishNMessages(kc, 20, 30, quorum = true))
          ))
        } take 30
      } runLog ) unsafeRun).map { _.copy(tail = offset(30)) }  shouldBe generateTopicMessages(0, 30, 30)


    }


  }

}
