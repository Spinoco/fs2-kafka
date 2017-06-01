package spinoco.fs2.kafka

import fs2._
import scodec.bits.ByteVector
import spinoco.fs2.kafka.network.BrokerConnection
import spinoco.fs2.kafka.state.BrokerAddress
import spinoco.protocol.kafka.Message.SingleMessage
import spinoco.protocol.kafka.Request.{ProduceRequest, RequiredAcks}
import spinoco.protocol.kafka.{MessageVersion, ProtocolVersion, RequestMessage}

import scala.concurrent.duration._

/**
  * Created by pach on 31/05/17.
  */
class KafkaClientSubscribeSpec extends Fs2KafkaRuntimeSpec {

  def publishNMessages(version: ProtocolVersion.Value, address: BrokerAddress, from: Int, to: Int, acks: RequiredAcks.Value =  RequiredAcks.NoResponse): Task[Unit] = {
    address.toInetSocketAddress flatMap { inetAddress =>
      Stream.range(from, to).map { idx =>
        RequestMessage(
          version = version
          , correlationId = idx
          , clientId = "test-publisher"
          , request = ProduceRequest(
            requiredAcks = acks
            , timeout = 3.seconds
            , messages = Vector((testTopicA, Vector((part0, Vector(SingleMessage(0l, MessageVersion.V0, None, ByteVector(1), ByteVector(idx)))))))
          )
        )
      }
      .through(BrokerConnection(inetAddress))
      .run
    }
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
        time.sleep(1.second) >>
        Stream.eval(publishNMessages(ProtocolVersion.Kafka_0_8, localBroker1_9092, 0, 20)) >> {
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
            kc.subscribe(testTopicA, part0, offset(0l)).take(10)
          }
        }
      } runLog ) unsafeRun) shouldBe generateTopicMessages(0, 10, 20)

    }


    "subscribe-at-zero-empty" in {
      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>

        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >> time.sleep(1.second) >> {
           concurrent.join(Int.MaxValue)(Stream(
            KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
              kc.subscribe(testTopicA, part0, offset(0l)).take(10)
            }
            , time.sleep_(1.second) ++ Stream.eval_(publishNMessages(ProtocolVersion.Kafka_0_8, localBroker1_9092, 0, 20))
          ))
        }
      } runLog ) unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(0, 10, 0)
    }

    "subscriber before head" in {
      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >> time.sleep(1.second) >> {
          concurrent.join(Int.MaxValue)(Stream(
            KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
              kc.subscribe(testTopicA, part0, offset(-1l)).take(10)
            }
            , time.sleep_(1.second) ++ Stream.eval_(publishNMessages(ProtocolVersion.Kafka_0_8, localBroker1_9092, 0, 20))
          ))
        }
      } runLog ) unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(0, 10, 0)
    }

    "subscriber after head" in {
      ((withKafkaSingleton(KafkaRuntimeRelease.V_8_2_0) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
        time.sleep(1.second) >>
        Stream.eval(publishNMessages(ProtocolVersion.Kafka_0_8, localBroker1_9092, 0, 20)) >> {
          concurrent.join(Int.MaxValue)(Stream(
            KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
              kc.subscribe(testTopicA, part0, TailOffset).take(10)
            }
            , time.sleep_(1.second) ++ Stream.eval_(publishNMessages(ProtocolVersion.Kafka_0_8, localBroker1_9092, 20, 40))
          ))
        }
      } runLog ) unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(20, 30, 0)
    }

  }


  "cluster" - {

    "subscribe-at-zero" in {

      ((withKafkaCluster(KafkaRuntimeRelease.V_8_2_0) flatMap { nodes =>

        Stream.eval(createKafkaTopic(nodes.broker1DockerId, testTopicA, replicas = 3)) >>
          Stream.eval(Task.delay { println("XXXR CREATED TOPIC") }) >>
          time.sleep(1.second) >>
          Stream.eval(publishNMessages(ProtocolVersion.Kafka_0_8, localBroker2_9192, 0, 20)) >> Stream.suspend {
          println(s"XXXR PUBLISHED messages")
          KafkaClient(Set(localBroker1_9092), ProtocolVersion.Kafka_0_8, "test-client") flatMap { kc =>
            kc.subscribe(testTopicA, part0, offset(0l)).take(10)
          }
        }
      } runLog ) unsafeRun) shouldBe generateTopicMessages(0, 10, 20)

    }


  }

}
