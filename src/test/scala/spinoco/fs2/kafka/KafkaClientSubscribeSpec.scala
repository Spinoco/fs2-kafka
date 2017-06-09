package spinoco.fs2.kafka

import fs2._
import fs2.util.Async

import scala.concurrent.duration._



/**
  * Created by pach on 31/05/17.
  */
class KafkaClientSubscribeSpec extends Fs2KafkaRuntimeSpec {



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
