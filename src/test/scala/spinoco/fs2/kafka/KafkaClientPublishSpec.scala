package spinoco.fs2.kafka

import fs2._
import scodec.bits.ByteVector
import spinoco.protocol.kafka.{Compression, ProtocolVersion}

import scala.concurrent.duration._

/**
  * Created by pach on 05/06/17.
  */
class KafkaClientPublishSpec extends Fs2KafkaRuntimeSpec {

  "single-broker" - {

    "publish-unsafe" in {

      def publish(kc: KafkaClient[Task]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishUnsafe1(testTopicA, part0, ByteVector(1),  ByteVector(idx))
        } drain
      }

      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
          time.sleep(1.second) >> {
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
            publish(kc) ++
            kc.subscribe(testTopicA, part0, offset(0l)).take(10)
          }
        }
      } runLog  ) unsafeRun).size shouldBe 10

    }

    "publish-response" in {
      def publish(kc: KafkaClient[Task]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publish1(testTopicA, part0, ByteVector(1),  ByteVector(idx), requireQuorum = false, serverAckTimeout = 3.seconds)
        } map (Left(_))
      }

      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
          time.sleep(1.second) >> {
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
            publish(kc) ++
            (kc.subscribe(testTopicA, part0, offset(0l)) map (Right(_)))
          } take 20
        }
      } runLog  ) unsafeRun) shouldBe
        (for { idx <- 0 until 10} yield Left(offset(idx))).toVector ++
        (for { idx <- 0 until 10} yield Right(TopicMessage(offset(idx), ByteVector(1), ByteVector(idx), offset(10)))).toVector
    }


    "publishN-unsafe" in {
      def publish(kc: KafkaClient[Task]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishUnsafeN(testTopicA, part0, compress = None)(for { i <- 0 until 10} yield (ByteVector(i), ByteVector(i*idx)))
        } drain
      }

      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
          time.sleep(1.second) >> {
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
            publish(kc) ++
            kc.subscribe(testTopicA, part0, offset(0l)).take(100)
          }
        }
      } runLog  ) unsafeRun).size shouldBe 100

    }


    "publishN-response" in {
      def publish(kc: KafkaClient[Task]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishN(testTopicA, part0, requireQuorum = false, serverAckTimeout = 3.seconds, compress = None)(for { i <- 0 until 10} yield (ByteVector(i), ByteVector(idx)))
        } map (Left(_))
      }

      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
          time.sleep(1.second) >> {
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
            publish(kc) ++
              (kc.subscribe(testTopicA, part0, offset(0l)) map (Right(_)))
          } take 110
        }
      } runLog ) unsafeRun) shouldBe
        (for { idx <- 0 until 10} yield Left(offset(idx*10))).toVector ++
        (for { idx <- 0 until 100} yield Right(TopicMessage(offset(idx), ByteVector(idx % 10), ByteVector(idx / 10), offset(100)))).toVector

    }


    "publishN-unsafe-compressed-gzip" in {
      def publish(kc: KafkaClient[Task]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishUnsafeN(testTopicA, part0, compress = Some(Compression.GZIP))(for {i <- 0 until 10} yield (ByteVector(i), ByteVector(i*idx)))
        } drain
      }

      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
          time.sleep(1.second) >> {
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
            publish(kc) ++
              kc.subscribe(testTopicA, part0, offset(0l)).take(100)
          }
        }
      } runLog  ) unsafeRun).size shouldBe 100

    }


    "publishN-response-compressed-gzip" in {
      def publish(kc: KafkaClient[Task]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishN(testTopicA, part0, requireQuorum = false, serverAckTimeout = 3.seconds, compress = Some(Compression.GZIP))(for { i <- 0 until 10} yield (ByteVector(i), ByteVector(idx)))
        } map (Left(_))
      }

      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
          time.sleep(1.second) >> {
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
            publish(kc) ++
              (kc.subscribe(testTopicA, part0, offset(0l)) map (Right(_)))
          } take 110
        }
      } runLog ) unsafeRun) shouldBe
        (for { idx <- 0 until 10} yield Left(offset(idx*10))).toVector ++
          (for { idx <- 0 until 100} yield Right(TopicMessage(offset(idx), ByteVector(idx % 10), ByteVector(idx / 10), offset(100)))).toVector

    }



  }

  "cluster" - {


    "publish-response" in {
      def publish(kc: KafkaClient[Task]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publish1(testTopicA, part0, ByteVector(1),  ByteVector(idx), requireQuorum = true, serverAckTimeout = 3.seconds)
        } map (Left(_))
      }

      ((withKafkaCluster(KafkaRuntimeRelease.V_8_2_0) flatMap { nodes =>
        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA)) >>
          time.sleep(1.second) >> {
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
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
