package spinoco.fs2.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2._
import scodec.bits.ByteVector

import scala.concurrent.duration._



/**
  * Created by pach on 31/05/17.
  */
class KafkaClientSubscribeSpec extends Fs2KafkaSingleBrokerSpec {


  s"single-broker" - {

    "subscribe-at-zero" in {
      withIndexedTopicStream { topic =>
        def publishMessages(from: Int, to: Int) = {
          Stream.range(from, to).evalMap { idx =>
            kafkaClient.publish1(topic, part0, ByteVector(1), ByteVector(idx), false, 10.seconds)
          }.compile.drain
        }
        
        Stream.eval(publishMessages(0, 20)) >>
        kafkaClient.subscribe(topic, part0, offset(0l)).take(10)
      }.compile.toVector.unsafeRunTimed(60.seconds) shouldBe Some(generateTopicMessages(0, 10, 20))
    }


    "subscribe-at-zero-empty" in {
      withIndexedTopicStream { topic =>
        def publishMessages(from: Int, to: Int) = {
          Stream.range(from, to).evalMap { idx =>
            kafkaClient.publish1(topic, part0, ByteVector(1), ByteVector(idx), false, 10.seconds)
          }.compile.drain
        }
        
        Stream[IO, Stream[IO, TopicMessage]](
          kafkaClient.subscribe(topic, part0, offset(0l))
          , Stream.sleep_[IO](1.second) ++ Stream.exec(publishMessages(0, 20))
        ).parJoinUnbounded.take(10)
      }.compile.toVector.unsafeRunTimed(60.seconds).map { _.map { _.copy(tail = offset(0)) } } shouldBe Some(generateTopicMessages(0, 10, 0))
    }

    "subscriber before head" in {
      withIndexedTopicStream { topic =>
        def publishMessages(from: Int, to: Int) = {
          Stream.range(from, to).evalMap { idx =>
            kafkaClient.publish1(topic, part0, ByteVector(1), ByteVector(idx), false, 10.seconds)
          }.compile.drain
        }
        
        Stream[IO, Stream[IO, TopicMessage]](
          kafkaClient.subscribe(topic, part0, offset(-1l))
          , Stream.sleep_[IO](1.second) ++ Stream.exec(publishMessages(0, 20))
        ).parJoinUnbounded.take(10)
      }.compile.toVector.unsafeRunTimed(60.seconds).map { _.map { _.copy(tail = offset(0)) } } shouldBe Some(generateTopicMessages(0, 10, 0))
    }

    "subscriber after head" in {
      withIndexedTopicStream { topic =>
        def publishMessages(from: Int, to: Int) = {
          Stream.range(from, to).evalMap { idx =>
            kafkaClient.publish1(topic, part0, ByteVector(1), ByteVector(idx), false, 10.seconds)
          }.compile.drain
        }
        
        Stream[IO, Stream[IO, TopicMessage]](
          Stream.exec(publishMessages(0, 20)) ++ kafkaClient.subscribe(topic, part0, TailOffset)
          , Stream.sleep_[IO](1.second) ++ Stream.exec(publishMessages(20, 40))
        ).parJoinUnbounded.take(10)
      }.compile.toVector.unsafeRunTimed(60.seconds).map { _.map { _.copy(tail = offset(0)) }} shouldBe Some(generateTopicMessages(20, 30, 0))
    }

  }




}
