package spinoco.fs2.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2._
import org.scalatest.time.Span
import scodec.bits.ByteVector

import scala.concurrent.duration._


/**
  * Created by pach on 06/06/17.
  */
class KafkaClusterPublish extends Fs2KafkaClusterSpec {

  override val timeLimit: Span = 300.seconds
  s"cluster" - {


    "publish-response" in {
      withIndexedTopicStream(replicationFactor = 3) { topic =>
        def publish() = {
          Stream.range(0, 10) evalMap { idx =>
            kafkaClient.publish1(topic, part0, ByteVector(1),  ByteVector(idx), requireQuorum = true, serverAckTimeout = 3.seconds)
          } map (Left(_))
        }

        Stream.sleep[IO](5.seconds) >> // Wait for cluster to stabilize and topic to propagate
        awaitLeaderAvailable(kafkaClient, topic, part0) >>
        publish() ++
          (kafkaClient.subscribe(topic, part0, offset(0L)) map (Right(_)))
      }.take(20).compile.toVector.unsafeRunTimed(290.seconds) shouldBe Some(
        (for { idx <- 0 until 10} yield Left(offset(idx))).toVector ++
          (for { idx <- 0 until 10} yield Right(TopicMessage(offset(idx), ByteVector(1), ByteVector(idx), offset(10)))).toVector
      )
    }


  }

}
