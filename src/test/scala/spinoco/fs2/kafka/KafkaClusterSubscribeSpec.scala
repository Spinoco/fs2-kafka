package spinoco.fs2.kafka

import fs2._
import spinoco.protocol.kafka.ProtocolVersion

import scala.concurrent.duration._



/**
  * Created by pach on 06/06/17.
  */
class KafkaClusterSubscribeSpec extends Fs2KafkaRuntimeSpec {



  s"cluster" - {

    "subscribe-at-zero" in skipFor(
      KafkaRuntimeRelease.V_0_9_0_1 -> ProtocolVersion.Kafka_0_8
      , KafkaRuntimeRelease.V_0_9_0_1 -> ProtocolVersion.Kafka_0_9
    ) {
      ((withKafkaCluster(runtime) flatMap { nodes =>

        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
          KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
          awaitLeaderAvailable(kc, testTopicA, part0) >>
          Stream.eval(publishNMessages(kc, 0, 20, quorum = true)) >>
            kc.subscribe(testTopicA, part0, HeadOffset)
        } take 10
      } runLog ) unsafeRun) shouldBe generateTopicMessages(0, 10, 20)

    }


    "subscribe-at-tail" in skipFor(
      KafkaRuntimeRelease.V_0_9_0_1 -> ProtocolVersion.Kafka_0_8
      , KafkaRuntimeRelease.V_0_9_0_1 -> ProtocolVersion.Kafka_0_9
    ) {

      ((withKafkaCluster(runtime) flatMap { nodes =>

        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
          KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
          awaitLeaderAvailable(kc, testTopicA, part0) >>
          Stream.eval(publishNMessages(kc, 0, 20, quorum = true)) >>
            concurrent.join(Int.MaxValue)(Stream(
              kc.subscribe(testTopicA, part0, TailOffset)
              , time.sleep_(3.second) ++ Stream.eval_(publishNMessages(kc, 20, 30, quorum = true))
            ))
        } take 10
      } runLog ) unsafeRun).map { _.copy(tail = offset(30)) }  shouldBe generateTopicMessages(20, 30, 30)


    }


    "recovers from leader-failure" in skipFor(
      KafkaRuntimeRelease.V_0_9_0_1 -> ProtocolVersion.Kafka_0_8
      , KafkaRuntimeRelease.V_0_9_0_1 -> ProtocolVersion.Kafka_0_9
    ) {

      ((withKafkaCluster(runtime) flatMap { nodes =>

        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
        KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
          awaitLeaderAvailable(kc, testTopicA, part0) flatMap { leader =>
          Stream.eval(publishNMessages(kc, 0, 20, quorum = true)) >>
          concurrent.join(Int.MaxValue)(Stream(
            kc.subscribe(testTopicA, part0, HeadOffset)
            , time.sleep_(3.seconds) ++ killLeader(kc, nodes, testTopicA, part0)
            , time.sleep_(10.seconds) ++ awaitNewLeaderAvailable(kc, testTopicA, part0, leader).drain ++ Stream.eval_(publishNMessages(kc, 20, 30, quorum = true))
          ))
        }} take 30
      } runLog ) unsafeRun).map { _.copy(tail = offset(30)) } shouldBe generateTopicMessages(0, 30, 30)


    }


  }

}
