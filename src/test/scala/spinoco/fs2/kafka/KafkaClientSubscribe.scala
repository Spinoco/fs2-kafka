package spinoco.fs2.kafka

import fs2._
import fs2.util.Async
import fs2.util.Async
import spinoco.protocol.kafka.ProtocolVersion

import scala.concurrent.duration._


class KafkaClientSubscribe_0802_P_08_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_8_2_0, ProtocolVersion.Kafka_0_8)

class KafkaClientSubscribe_0901_P_08_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_9_0_1, ProtocolVersion.Kafka_0_8)
class KafkaClientSubscribe_0901_P_09_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_9_0_1, ProtocolVersion.Kafka_0_9)

class KafkaClientSubscribe_1000_P_08_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_8)
class KafkaClientSubscribe_1000_P_09_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_9)
class KafkaClientSubscribe_1000_P_10_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_0, ProtocolVersion.Kafka_0_10)

class KafkaClientSubscribe_1001_P_08_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_8)
class KafkaClientSubscribe_1001_P_09_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_9)
class KafkaClientSubscribe_1001_P_10_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_10)
class KafkaClientSubscribe_1001_P_101_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_1, ProtocolVersion.Kafka_0_10_1)

class KafkaClientSubscribe_1002_P_08_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_8)
class KafkaClientSubscribe_1002_P_09_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_9)
class KafkaClientSubscribe_1002_P_10_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10)
class KafkaClientSubscribe_1002_P_101_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10_1)
class KafkaClientSubscribe_1002_P_102_Spec extends KafkaClientSubscribe(KafkaRuntimeRelease.V_0_10_2, ProtocolVersion.Kafka_0_10_2)

/**
  * Created by pach on 31/05/17.
  */
abstract class KafkaClientSubscribe(val runtime: KafkaRuntimeRelease.Value, val protocol: ProtocolVersion.Value) extends Fs2KafkaRuntimeSpec {

  val version = s"$runtime[$protocol]"

  s"$version: single-broker" - {

    "subscribe-at-zero" in {

      ((withKafkaSingleton(runtime) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
        time.sleep(1.second) >> {
          KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
            Stream.eval(publishNMessages(kc, 0, 20)) >>
            kc.subscribe(testTopicA, part0, offset(0l)).take(10)
          }
        }
      } runLog  ) unsafeRun) shouldBe generateTopicMessages(0, 10, 20)

    }


    "subscribe-at-zero-empty" in {
      ((withKafkaSingleton(runtime) flatMap { case (zkDockerId, kafkaDockerId) =>

        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >> time.sleep(1.second) >>
        Stream.eval(Async.ref[Task, KafkaClient[Task]]) flatMap { kcRef =>
           concurrent.join(Int.MaxValue)(Stream(
            KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
              Stream.eval_(kcRef.setPure(kc)) ++
              kc.subscribe(testTopicA, part0, offset(0l))
            }
            , time.sleep_(1.second) ++ Stream.eval_(kcRef.get flatMap { kc => publishNMessages(kc, 0, 20) })
          )).take(10)
        }
      } runLog ) unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(0, 10, 0)
    }

    "subscriber before head" in {
      ((withKafkaSingleton(runtime) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >> time.sleep(1.second) >>
        Stream.eval(Async.ref[Task, KafkaClient[Task]]) flatMap { kcRef =>
          concurrent.join(Int.MaxValue)(Stream(
            KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
              Stream.eval_(kcRef.setPure(kc)) ++
              kc.subscribe(testTopicA, part0, offset(-1l))
            }
            , time.sleep_(1.second) ++ Stream.eval_(kcRef.get flatMap { kc => publishNMessages(kc, 0, 20) })
          )).take(10)
        }
      } runLog ) unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(0, 10, 0)
    }

    "subscriber after head" in {
      ((withKafkaSingleton(runtime) flatMap { case (zkDockerId, kafkaDockerId) =>
        Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
        time.sleep(1.second) >>
        Stream.eval(Async.ref[Task, KafkaClient[Task]]) flatMap { kcRef =>
          concurrent.join(Int.MaxValue)(Stream(
            KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
              Stream.eval_(publishNMessages(kc, 0, 20)) ++
              Stream.eval_(kcRef.setPure(kc)) ++
              kc.subscribe(testTopicA, part0, TailOffset)
            }
            , time.sleep_(1.second) ++ Stream.eval_(kcRef.get flatMap { kc => publishNMessages(kc, 20, 40) })
          )).take(10)
        }
      } runLog ) unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(20, 30, 0)
    }

  }




}
