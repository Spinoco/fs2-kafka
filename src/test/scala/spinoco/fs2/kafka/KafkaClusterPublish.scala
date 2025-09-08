package spinoco.fs2.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2._
import org.scalatest.time.Span
import scodec.bits.ByteVector

import scala.concurrent.duration._


/**
  * Created by pach on 06/06/17.
  */
class KafkaClusterPublish extends Fs2KafkaRuntimeSpec {

  override val timeLimit: Span = 300.seconds
  s"cluster" - {


    "publish-response" in {
      def publish(kc: KafkaClient[IO]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publish1(testTopicA, part0, ByteVector(1),  ByteVector(idx), requireQuorum = true, serverAckTimeout = 3.seconds)
        } map (Left(_))
      }

      withKafkaCluster(runtime).flatMap { nodes =>
        Stream.sleep[IO](10.seconds) >> // Wait for cluster to stabilize and topic to propagate
        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >> {
          Stream.resource(KafkaClient.client[IO](Set(localBroker1_9092), protocol, "test-client")) flatMap { kc =>
            awaitLeaderAvailable(kc, testTopicA, part0) >>
            publish(kc) ++
              (kc.subscribe(testTopicA, part0, offset(0l)) map (Right(_)))
          } take 20
        }
      }.compile.toVector.unsafeRunTimed(290.seconds) shouldBe Some(
        (for { idx <- 0 until 10} yield Left(offset(idx))).toVector ++
          (for { idx <- 0 until 10} yield Right(TopicMessage(offset(idx), ByteVector(1), ByteVector(idx), offset(10)))).toVector
      )
    }


  }

}
