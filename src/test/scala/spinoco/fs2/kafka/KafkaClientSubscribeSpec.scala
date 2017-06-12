package spinoco.fs2.kafka

import fs2._

import scala.concurrent.duration._



/**
  * Created by pach on 31/05/17.
  */
class KafkaClientSubscribeSpec extends Fs2KafkaRuntimeSpec {


  s"single-broker" - {

    "subscribe-at-zero" in {

      ((withKafkaClient(runtime, protocol) { kc =>
      Stream.eval(publishNMessages(kc, 0, 20)) >>
      kc.subscribe(testTopicA, part0, offset(0l)).take(10)
      } runLog  ) unsafeTimed 30.seconds unsafeRun) shouldBe generateTopicMessages(0, 10, 20)
    }


    "subscribe-at-zero-empty" in {
      ((withKafkaClient(runtime, protocol) { kc =>
        concurrent.join(Int.MaxValue)(Stream(
          kc.subscribe(testTopicA, part0, offset(0l))
          , time.sleep_(1.second) ++ Stream.eval_(publishNMessages(kc, 0, 20))
        )).take(10)
      } runLog) unsafeTimed 30.seconds unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(0, 10, 0)

    }

    "subscriber before head" in {
      ((withKafkaClient(runtime, protocol) { kc =>
        concurrent.join(Int.MaxValue)(Stream(
          kc.subscribe(testTopicA, part0, offset(-1l))
          , time.sleep_(1.second) ++ Stream.eval_(publishNMessages(kc, 0, 20))
        )).take(10)
      } runLog) unsafeTimed 30.seconds unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(0, 10, 0)
    }

    "subscriber after head" in {
      ((withKafkaClient(runtime, protocol) { kc =>
        concurrent.join(Int.MaxValue)(Stream(
          Stream.eval_(publishNMessages(kc, 0, 20)) ++ kc.subscribe(testTopicA, part0, TailOffset)
          , time.sleep_(1.second) ++ Stream.eval_(publishNMessages(kc, 20, 40))
        )).take(10)
      } runLog) unsafeTimed 30.seconds unsafeRun).map { _.copy(tail = offset(0)) } shouldBe generateTopicMessages(20, 30, 0)

    }

  }




}
