package spinoco.fs2.kafka

import fs2._
import fs2.util.Async
import scodec.bits.ByteVector
import spinoco.protocol.kafka.{ProtocolVersion}

import scala.concurrent.duration._

/**
  * Created by pach on 31/05/17.
  */
class KafkaClientSubscribeSpec extends Fs2KafkaRuntimeSpec {

  def publishNMessages(client: KafkaClient[Task], version: ProtocolVersion.Value, from: Int, to: Int, quorum: Boolean = false): Task[Unit] = {

    Stream.range(from, to).evalMap { idx =>
      client.publish1(testTopicA, part0, ByteVector(1),  ByteVector(idx), quorum, 3.seconds)
    }
    .run

  }

  def generateTopicMessages(from: Int, to: Int, tail: Long): Vector[TopicMessage] = {
    ((from until to) map { idx =>
      TopicMessage(offset(idx.toLong), ByteVector(1), ByteVector(idx), offset(tail) )
    }) toVector
  }


  "single-broker" - {

    "subscribe-at-zero" in {

      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
        time.sleep(1.second) >> {
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
            Stream.eval(publishNMessages(kc, ProtocolVersion.Kafka_0_8, 0, 20)) >>
            kc.subscribe(testTopicA, part0, offset(0l)).take(10)
          }
        }
      } runLog  ) unsafeRun) shouldBe generateTopicMessages(0, 10, 20)

    }


    "subscribe-at-zero-empty" in {
      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>

        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >> time.sleep(1.second) >>
        Stream.eval(Async.ref[Task, KafkaClient[Task]]) flatMap { kcRef =>
           concurrent.join(Int.MaxValue)(Stream(
            KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
              Stream.eval_(kcRef.setPure(kc)) ++
              kc.subscribe(testTopicA, part0, offset(0l))
            }
            , time.sleep_(1.second) ++ Stream.eval_(kcRef.get flatMap { kc => publishNMessages(kc, ProtocolVersion.Kafka_0_8, 0, 20) })
          )).take(10)
        }
      } runLog ) unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(0, 10, 0)
    }

    "subscriber before head" in {
      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >> time.sleep(1.second) >>
        Stream.eval(Async.ref[Task, KafkaClient[Task]]) flatMap { kcRef =>
          concurrent.join(Int.MaxValue)(Stream(
            KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
              Stream.eval_(kcRef.setPure(kc)) ++
              kc.subscribe(testTopicA, part0, offset(-1l))
            }
            , time.sleep_(1.second) ++ Stream.eval_(kcRef.get flatMap { kc => publishNMessages(kc, ProtocolVersion.Kafka_0_8, 0, 20) })
          )).take(10)
        }
      } runLog ) unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(0, 10, 0)
    }

    "subscriber after head" in {
      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
        time.sleep(1.second) >>
        Stream.eval(Async.ref[Task, KafkaClient[Task]]) flatMap { kcRef =>
          concurrent.join(Int.MaxValue)(Stream(
            KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
              Stream.eval_(publishNMessages(kc, ProtocolVersion.Kafka_0_8, 0, 20)) ++
              Stream.eval_(kcRef.setPure(kc)) ++
              kc.subscribe(testTopicA, part0, TailOffset)
            }
            , time.sleep_(1.second) ++ Stream.eval_(kcRef.get flatMap { kc => publishNMessages(kc, ProtocolVersion.Kafka_0_8, 20, 40) })
          )).take(10)
        }
      } runLog ) unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(20, 30, 0)
    }

  }


  "cluster" - {

    "subscribe-at-zero" in {

      ((withKafkaCluster(KafkaRuntimeRelease.V_8_2_0) flatMap { nodes =>

        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
        Stream.eval(Task.delay { println("XXXR CREATED TOPIC") }) >>
        KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
          time.sleep(1.second) >>
          Stream.eval(publishNMessages(kc, ProtocolVersion.Kafka_0_8, 0, 20, quorum = true)) >>
          kc.subscribe(testTopicA, part0, offset(0l))
        } take(10)
      } runLog ) unsafeRun) shouldBe generateTopicMessages(0, 10, 20)

    }


  }

}
