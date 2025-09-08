package spinoco.fs2.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2._
import org.scalatest.time.Span

import scala.concurrent.duration._



/**
  * Created by pach on 06/06/17.
  */
class KafkaClusterSubscribeSpec extends Fs2KafkaRuntimeSpec {

  override val timeLimit: Span = 200.seconds
  s"cluster" - {

    "subscribe-at-zero" in {
      withKafkaCluster(runtime).flatMap { nodes =>
        Stream.sleep[IO](10.seconds) >> // Wait for cluster to stabilize and topic to propagate
        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
          Stream.resource(KafkaClient.client[IO](localCluster, protocol, "test-client")).flatMap { kc =>
          awaitLeaderAvailable(kc, testTopicA, part0) >>
          Stream.eval(publishNMessages(kc, 0, 20, quorum = true)) >>
            kc.subscribe(testTopicA, part0, HeadOffset)
        }.take(10)
      }.compile.toVector.unsafeRunTimed(180.seconds) shouldBe Some(generateTopicMessages(0, 10, 20))

    }

    "subscribe-at-tail" in {

      withKafkaCluster(runtime).flatMap { nodes =>
        Stream.sleep[IO](10.seconds) >> // Wait for cluster to stabilize and topic to propagate
        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
          Stream.resource(KafkaClient.client[IO](localCluster, protocol, "test-client")).flatMap { kc =>
          awaitLeaderAvailable(kc, testTopicA, part0) >>
          Stream.eval(publishNMessages(kc, 0, 20, quorum = true)) >>
            Stream(
              kc.subscribe(testTopicA, part0, TailOffset)
              , Stream.sleep_[IO](3.second) ++ Stream.exec(publishNMessages(kc, 20, 30, quorum = true))
            ).parJoinUnbounded
        }.take(10)
      }.compile.toVector.unsafeRunTimed(180.seconds).map { _.map { _.copy(tail = offset(30)) } } shouldBe Some(generateTopicMessages(20, 30, 30))
    }

    "recovers from leader-failure" in {

      withKafkaCluster(runtime).flatMap { nodes =>
        Stream.sleep[IO](10.seconds) >> // Wait for cluster to stabilize and topic to propagate
        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
        Stream.resource(KafkaClient.client[IO](localCluster, protocol, "test-client")).flatMap { kc =>
          awaitLeaderAvailable(kc, testTopicA, part0) flatMap { leader =>
          Stream.eval(publishNMessages(kc, 0, 20, quorum = true)) >>
          Stream(
            kc.subscribe(testTopicA, part0, HeadOffset)
            , Stream.sleep[IO](5.seconds) >>
              killLeader(kc, nodes, testTopicA, part0)

            , Stream.sleep[IO](10.seconds) >>
              awaitNewLeaderAvailable(kc, testTopicA, part0, leader) >>
              Stream.sleep[IO](3.seconds) >>
              Stream.exec(publishNMessages(kc, 20, 30, quorum = true))
          ).parJoinUnbounded
        }}.take(30)
      }.compile.toVector.unsafeRunTimed(180.seconds).map { _.map { _.copy(tail = offset(30)) } } shouldBe Some(generateTopicMessages(0, 30, 30))

    }
  }
}
