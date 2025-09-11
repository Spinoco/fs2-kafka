package spinoco.fs2.kafka


import cats.effect.unsafe.implicits.global
import scodec.bits.ByteVector
import shapeless.tag
import spinoco.protocol.kafka._

import scala.concurrent.duration._



class KafkaClientLastOffsetSpec extends Fs2KafkaSingleBrokerSpec {

  s"Last Offset (single broker)" - {

    "queries when topic is empty"  in {
      withIndexedTopic { topic =>
        kafkaClient.offsetRangeFor(topic, tag[PartitionId](0))
      }.unsafeRunTimed(60.seconds) shouldBe Some((offset(0), offset(0)))
    }


    "queries when topic is non-empty" in {
      withIndexedTopic { topic =>
        kafkaClient.publish1(topic, part0, ByteVector(1, 2, 3), ByteVector(5, 6, 7), false, 10.seconds) >>
        kafkaClient.offsetRangeFor(topic, tag[PartitionId](0))
      }.unsafeRunTimed(60.seconds) shouldBe Some((offset(0), offset(1)))
    }


  }


}
