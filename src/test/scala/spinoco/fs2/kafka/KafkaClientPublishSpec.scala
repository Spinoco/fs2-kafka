package spinoco.fs2.kafka

import cats.effect.IO

import fs2._
import scodec.bits.ByteVector
import spinoco.protocol.kafka.Compression

import scala.concurrent.duration._




class KafkaClientPublishSpec extends Fs2KafkaRuntimeSpec {


  s"single-broker" - {

    "publish-unsafe" in {

      def publish(kc: KafkaClient[IO]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishUnsafe1(testTopicA, part0, ByteVector(1),  ByteVector(idx))
        } drain
      }

      withKafkaClient(runtime, protocol) { kc =>
        publish(kc) ++
        Stream.sleep[IO](2.second) >> // wait for message to be accepted
        kc.subscribe(testTopicA, part0, offset(0l)).take(10)
      }.compile.toVector.unsafeRunTimed(30.seconds).map(_.size) shouldBe Some(10)

    }

    "publish-response" in {
      def publish(kc: KafkaClient[IO]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publish1(testTopicA, part0, ByteVector(1),  ByteVector(idx), requireQuorum = false, serverAckTimeout = 3.seconds)
        } map (Left(_))
      }

      withKafkaClient(runtime, protocol) { kc =>
          publish(kc) ++
          (kc.subscribe(testTopicA, part0, offset(0l)) map (Right(_)))
      }.take(20).compile.toVector.unsafeRunTimed(30.seconds) shouldBe Some {
        (for { idx <- 0 until 10} yield Left(offset(idx))).toVector ++
        (for { idx <- 0 until 10} yield Right(TopicMessage(offset(idx), ByteVector(1), ByteVector(idx), offset(10)))).toVector
      }
    }


    "publishN-unsafe" in {
      def publish(kc: KafkaClient[IO]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishUnsafeN(testTopicA, part0, compress = None)(Chunk.seq(for { i <- 0 until 10} yield (ByteVector(i), ByteVector(i*idx))))
        } drain
      }

      withKafkaClient(runtime, protocol) { kc =>
        publish(kc) ++
        Stream.sleep[IO](3.second) >> // wait for message to be accepted
        kc.subscribe(testTopicA, part0, offset(0l)).take(100)
      }.compile.toVector.unsafeRunTimed(30.seconds).map(_.size) shouldBe Some(100)

    }


    "publishN-response" in {
      def publish(kc: KafkaClient[IO]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishN(testTopicA, part0, requireQuorum = false, serverAckTimeout = 3.seconds, compress = None)(Chunk.seq(for { i <- 0 until 10} yield (ByteVector(i), ByteVector(idx))))
        } map (Left(_))
      }

      withKafkaClient(runtime, protocol) { kc =>
        publish(kc) ++
        (kc.subscribe(testTopicA, part0, offset(0l)) map (Right(_)))
      }.take(110).compile.toVector.unsafeRunTimed(30.seconds) shouldBe Some {
        (for { idx <- 0 until 10} yield Left(offset(idx*10))).toVector ++
        (for { idx <- 0 until 100} yield Right(TopicMessage(offset(idx), ByteVector(idx % 10), ByteVector(idx / 10), offset(100)))).toVector
      }
    }


    "publishN-unsafe-compressed-gzip" in {
      def publish(kc: KafkaClient[IO]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishUnsafeN(testTopicA, part0, compress = Some(Compression.GZIP))(Chunk.seq(for {i <- 0 until 10} yield (ByteVector(i), ByteVector(i*idx))))
        } drain
      }

      withKafkaClient(runtime, protocol) { kc =>
        publish(kc) ++
        Stream.sleep[IO](3.second) >> // wait for message to be accepted
        kc.subscribe(testTopicA, part0, offset(0l)).take(100)
      }.compile.toVector.unsafeRunTimed(30.seconds).map(_.size) shouldBe Some(100)

    }


    "publishN-response-compressed-gzip" in {
      def publish(kc: KafkaClient[IO]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishN(testTopicA, part0, requireQuorum = false, serverAckTimeout = 3.seconds, compress = Some(Compression.GZIP))(Chunk.seq(for { i <- 0 until 10 } yield (ByteVector(i), ByteVector(idx))))
        } map (Left(_))
      }

      withKafkaClient(runtime, protocol) { kc =>
        publish(kc) ++
        ((kc.subscribe(testTopicA, part0, offset(0l)) map (Right(_))) take 100)
      }.compile.toVector.unsafeRunTimed(30.seconds) shouldBe Some {
        (for { idx <- 0 until 10} yield Left(offset(idx*10))).toVector ++
        (for { idx <- 0 until 100} yield Right(TopicMessage(offset(idx), ByteVector(idx % 10), ByteVector(idx / 10), offset(100)))).toVector
      }
    }


    "publishN-response-compressed-gzip-not-aligned" in {
      def publish(kc: KafkaClient[IO]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishN(testTopicA, part0, requireQuorum = false, serverAckTimeout = 3.seconds, compress = Some(Compression.GZIP))(Chunk.seq(for { i <- 0 until 10 } yield (ByteVector(i), ByteVector(idx))))
        } map (Left(_))
      }

      withKafkaClient(runtime, protocol) { kc =>
        publish(kc) ++
        ((kc.subscribe(testTopicA, part0, offset(5l)) map (Right(_)))  take 95)
      }.compile.toVector.unsafeRunTimed(30.seconds) shouldBe Some {
        (for { idx <- 0 until 10} yield Left(offset(idx*10))).toVector ++
        (for { idx <- 0 until 100} yield Right(TopicMessage(offset(idx), ByteVector(idx % 10), ByteVector(idx / 10), offset(100)))).drop(5).toVector
      }
    }


    "publishN-unsafe-compressed-snappy" in {
      def publish(kc: KafkaClient[IO]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishUnsafeN(testTopicA, part0, compress = Some(Compression.Snappy))(Chunk.seq(for {i <- 0 until 10} yield (ByteVector(i), ByteVector(i*idx))))
        } drain
      }

      withKafkaClient(runtime, protocol) { kc =>
        publish(kc) ++
        Stream.sleep[IO](3.second) >> // wait for message to be accepted
        kc.subscribe(testTopicA, part0, offset(0l)).take(100)
      }.compile.toVector.unsafeRunTimed(30.seconds).map(_.size) shouldBe Some(100)

    }


    "publishN-response-compressed-snappy" in {
      def publish(kc: KafkaClient[IO]) = {
        Stream.range(0, 10) evalMap { idx =>
          kc.publishN(testTopicA, part0, requireQuorum = false, serverAckTimeout = 3.seconds, compress = Some(Compression.Snappy))(Chunk.seq(for { i <- 0 until 10 } yield (ByteVector(i), ByteVector(idx))))
        } map (Left(_))
      }

      withKafkaClient(runtime, protocol) { kc =>
        publish(kc) ++
        ((kc.subscribe(testTopicA, part0, offset(0l)) map (Right(_))) take 100)
      }.compile.toVector.unsafeRunTimed(30.seconds) shouldBe Some {
        (for { idx <- 0 until 10} yield Left(offset(idx*10))).toVector ++
        (for { idx <- 0 until 100} yield Right(TopicMessage(offset(idx), ByteVector(idx % 10), ByteVector(idx / 10), offset(100)))).toVector
      }
    }

  }



}
