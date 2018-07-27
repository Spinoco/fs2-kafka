package spinoco.fs2.kafka

import cats.effect.IO
import fs2._

import scala.concurrent.duration._



/**
  * Created by pach on 31/05/17.
  */
class KafkaClientSubscribeSpec extends Fs2KafkaRuntimeSpec {


  s"single-broker" - {

    "subscribe-at-zero" in {

      withKafkaClient(runtime, protocol) { kc =>
      Stream.eval(publishNMessages(kc, 0, 20)) >>
      kc.subscribe(testTopicA, part0, offset(0l)).take(10)
      }.compile.toVector.unsafeRunTimed(30.seconds) shouldBe Some(generateTopicMessages(0, 10, 20))
    }


    "subscribe-at-zero-empty" in {
      withKafkaClient(runtime, protocol) { kc =>
        Stream[IO, Stream[IO, TopicMessage]](
          kc.subscribe(testTopicA, part0, offset(0l))
          , Stream.sleep_[IO](1.second) ++ Stream.eval_(publishNMessages(kc, 0, 20))
        ).parJoinUnbounded.take(10)
      }.compile.toVector.unsafeRunTimed(30.seconds).map { _.map { _.copy(tail = offset(0)) } } shouldBe Some(generateTopicMessages(0, 10, 0))

    }

    "subscriber before head" in {
      withKafkaClient(runtime, protocol) { kc =>
        Stream[IO, Stream[IO, TopicMessage]](
          kc.subscribe(testTopicA, part0, offset(-1l))
          , Stream.sleep_[IO](1.second) ++ Stream.eval_(publishNMessages(kc, 0, 20))
        ).parJoinUnbounded.take(10)
      }.compile.toVector.unsafeRunTimed(30.seconds).map { _.map { _.copy(tail = offset(0)) } } shouldBe Some(generateTopicMessages(0, 10, 0))
    }

    "subscriber after head" in {
      withKafkaClient(runtime, protocol) { kc =>
        Stream[IO, Stream[IO, TopicMessage]](
          Stream.eval_(publishNMessages(kc, 0, 20)) ++ kc.subscribe(testTopicA, part0, TailOffset)
          , Stream.sleep_[IO](1.second) ++ Stream.eval_(publishNMessages(kc, 20, 40))
        ).parJoinUnbounded.take(10)
      }.compile.toVector.unsafeRunTimed(30.seconds).map { _.map { _.copy(tail = offset(0)) }} shouldBe Some(generateTopicMessages(20, 30, 0))

    }

  }




}
