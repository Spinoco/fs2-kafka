package spinoco.fs2.kafka.network

import cats.effect.IO
import cats.effect.concurrent.Ref

import spinoco.fs2.kafka.Fs2KafkaClientSpec
import fs2._
import scodec.bits.ByteVector
import shapeless.tag

import spinoco.protocol.kafka.Request.RequiredAcks
import spinoco.protocol.kafka.Response.MetadataResponse
import spinoco.protocol.kafka._
import spinoco.protocol.kafka.codec.MessageCodec
import scala.concurrent.duration._

class BrokerConnectionSpec extends Fs2KafkaClientSpec {
 import BrokerConnection._



  val metaRequestMessage = RequestMessage(
    version = ProtocolVersion.Kafka_0_8
    , correlationId = 1
    , clientId = "client-1"
    , request = Request.MetadataRequest(Vector.empty)
  )

  val metaResponse = ResponseMessage(
    correlationId = 1
    , response = MetadataResponse(Vector.empty,Vector.empty)
  )

  val produceRequest = Request.ProduceRequest(
    requiredAcks =  RequiredAcks.LocalOnly
    , timeout = 10.seconds
    , messages = Vector(
      (tag[TopicName]("test"), Vector(
        (tag[PartitionId](0), Vector(
          Message.SingleMessage(0l,MessageVersion.V0,None,ByteVector(1,2,3), ByteVector(5,6,7,8))
        ))
      ))
    )
  )

  val produceRequestMessage = RequestMessage(
    version = ProtocolVersion.Kafka_0_8
    , correlationId = 1
    , clientId = "client-1"
    , request = produceRequest
  )



  "Sending of messages" - {

    "will send and register MetadataRequest" in {
      var send:Vector[ByteVector] = Vector.empty
      val ref = Ref.of[IO, Map[Int, RequestMessage]](Map.empty).unsafeRunSync()
      Stream(
        metaRequestMessage
      ).covary[IO].through(impl.sendMessages[IO](
        openRequests = ref
        , sendOne = { (chunk:Chunk[Byte]) => IO{ val bs = chunk.toBytes; send = send :+ ByteVector.view(bs.values).drop(bs.offset).take(bs.size) }}
      )).compile.drain.unsafeRunSync()

      send.size shouldBe 1
      ref.get.unsafeRunSync().get(metaRequestMessage.correlationId) shouldBe Some(metaRequestMessage)
    }

    "will send and register Produce Request " in {
      var send:Vector[ByteVector] = Vector.empty
      val ref = Ref.of[IO, Map[Int,RequestMessage]](Map.empty).unsafeRunSync()
      Stream(
        produceRequestMessage
      ).covary[IO].through(impl.sendMessages[IO](
        openRequests = ref
        , sendOne = { (chunk:Chunk[Byte]) => IO{ val bs = chunk.toBytes; send = send :+ ByteVector.view(bs.values).drop(bs.offset).take(bs.size) }}
      )).compile.drain.unsafeRunSync()

      send.size shouldBe 1
      ref.get.unsafeRunSync().get(produceRequestMessage.correlationId) shouldBe Some(produceRequestMessage)
    }

    "will send Produce Request bot won't register if reply is not expected" in {
      var send:Vector[ByteVector] = Vector.empty
      val ref = Ref.of[IO, Map[Int,RequestMessage]](Map.empty).unsafeRunSync()
      Stream(
        produceRequestMessage.copy(request = produceRequest.copy(requiredAcks = RequiredAcks.NoResponse))
      ).covary[IO].through(impl.sendMessages[IO](
        openRequests = ref
        , sendOne = { (chunk:Chunk[Byte]) => IO{ val bs = chunk.toBytes; send = send :+ ByteVector.view(bs.values).drop(bs.offset).take(bs.size) }}
      )).compile.drain.unsafeRunSync()

      send.size shouldBe 1
      ref.get.unsafeRunSync().get(produceRequestMessage.correlationId) shouldBe None
    }

  }


  "Receiving of messages" - {


    "correctly chunks based on size of message" in  forAll { (messages:Seq[Seq[Seq[Byte]]]) =>
      val source = messages.map { oneMsg =>
        val sizeOfMsg = oneMsg.map(_.size).sum
        val chunks = oneMsg.map(sb => Chunk.bytes(sb.toArray))
        chunks.foldLeft(Stream.chunk[IO, Byte](Chunk.bytes(ByteVector.fromInt(sizeOfMsg).toArray))) { case (s,next) => s ++ Stream.chunk(next) }
      }.foldLeft(Stream.empty:Stream[IO, Byte])(_ ++ _)

      val resultMsg = source.through(impl.receiveChunks).compile.toVector.unsafeRunSync()
      val expectedMsg = messages.map { oneMsg =>
        oneMsg.foldLeft(ByteVector.empty){ case (bv, n) => bv ++ ByteVector.view(n.toArray) }
      }

      resultMsg shouldBe expectedMsg.toVector

    }

    "correctly decodes received message (MetadataResponse)" in {

      val ref = Ref.of[IO, Map[Int,RequestMessage]](Map.empty).unsafeRunSync()
      ref.set(Map(1 -> metaRequestMessage)).unsafeRunSync()

      val bytes =
      MessageCodec.responseCodecFor(ProtocolVersion.Kafka_0_10,ApiKey.MetadataRequest).encode(metaResponse.response)
      .flatMap(rbv => MessageCodec.responseCorrelationCodec.encode(metaResponse.correlationId -> rbv))
      .getOrElse(fail("Encoding of response failed"))

      val result = Stream(bytes.bytes.drop(4)).covary[IO].through(impl.decodeReceived[IO](ref)).compile.toVector.unsafeRunSync()

      result shouldBe Vector(metaResponse)
      ref.get.unsafeRunSync() shouldBe Map()
    }


  }

}
