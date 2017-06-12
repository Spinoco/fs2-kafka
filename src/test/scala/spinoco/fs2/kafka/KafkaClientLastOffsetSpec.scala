package spinoco.fs2.kafka


import fs2._
import scodec.bits.ByteVector
import shapeless.tag
import spinoco.protocol.kafka._

import scala.concurrent.duration._



class KafkaClientLastOffsetSpec extends Fs2KafkaRuntimeSpec {

  s"Last Offset (single broker)" - {

    "queries when topic is empty"  in {
      ((withKafkaClient(runtime, protocol) { kc =>
          Stream.eval(kc.offsetRangeFor(testTopicA, tag[PartitionId](0)))
      }  runLog ) unsafeTimed 30.seconds unsafeRun) shouldBe Vector((offset(0), offset(0)))
    }


    "queries when topic is non-empty" in {
      ((withKafkaClient(runtime, protocol)  { kc =>
          Stream.eval(kc.publish1(testTopicA, part0, ByteVector(1, 2, 3), ByteVector(5, 6, 7), false, 10.seconds)) >>
          Stream.eval(kc.offsetRangeFor(testTopicA, tag[PartitionId](0)))
      } runLog ) unsafeTimed 30.seconds unsafeRun) shouldBe Vector((offset(0), offset(1)))
    }


  }


}
