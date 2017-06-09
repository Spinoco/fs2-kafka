package spinoco.fs2.kafka

import fs2.{Stream, Task, time}
import scodec.bits.ByteVector
import spinoco.protocol.kafka.ProtocolVersion

import scala.concurrent.duration._


/**
  * Created by pach on 06/06/17.
  */
class KafkaClusterPublish extends Fs2KafkaRuntimeSpec {

  val version = s"$runtime[$protocol]"
 
  s"$version: cluster" - {


    "publish-response" in skipFor(
      KafkaRuntimeRelease.V_0_9_0_1 -> ProtocolVersion.Kafka_0_8
      , KafkaRuntimeRelease.V_0_9_0_1 -> ProtocolVersion.Kafka_0_9
    ) {
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
