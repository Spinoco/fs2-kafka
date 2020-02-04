package spinoco.fs2.kafka.network

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup

import cats.syntax.all._
import cats.{Applicative, Monad}
import cats.effect.{ConcurrentEffect, Timer}
import cats.effect.concurrent.Ref
import fs2._
import fs2.Stream._
import scodec.bits.ByteVector

import spinoco.protocol.kafka.Request.{ProduceRequest, RequiredAcks}
import spinoco.protocol.kafka.codec.MessageCodec
import spinoco.protocol.kafka.{ApiKey, RequestMessage, ResponseMessage}
import scala.annotation.tailrec
import scala.concurrent.duration._



object BrokerConnection {

  /**
    *
    * Pipe to send/receive messages to/from Broker.
    *
    * When this resulting Stream is run, it will connect to destination broker,
    * then it will start to consume messages from `source` and send them
    * to given broker with kafka protocol.
    *
    * In parallel this receives any messages from broker, de-serializes them
    * and emits them.
    *
    * At any time, when the connection fails, or messages cannot be encoded/decoded
    * this process will fail resulting in termination of the connection with Broker.
    *
    * @param address          Address of the kafka Broker
    * @param writeTimeout     Timeout for performing the write operations
    * @param AG
    * @tparam F
    * @return
    */
  def apply[F[_] : ConcurrentEffect : Timer](
    address: InetSocketAddress
    , writeTimeout: Option[FiniteDuration] = None
    , readTimeout: Option[FiniteDuration] = None
    , readMaxChunkSize: Int = 256 * 1024      // 256 Kilobytes
  )(implicit AG:AsynchronousChannelGroup): Pipe[F, RequestMessage, ResponseMessage] = {
    (source: Stream[F,RequestMessage]) =>
      Stream.resource(fs2.io.tcp.client[F](address)).flatMap { socket =>
        eval(Ref.of(Map.empty[Int,RequestMessage])).flatMap { openRequests =>
          val send = source.through(impl.sendMessages(
            openRequests = openRequests
            , sendOne = (x) => socket.write(x, writeTimeout)
          ))

          val receive =
            socket.reads(readMaxChunkSize, timeout = readTimeout)
            .through(impl.receiveMessages(
              openRequests = openRequests
            ))

          (send.drain.onFinalize(socket.endOfInput) mergeHaltBoth receive)
        }
      }
  }


  object impl {

    /**
      * Pipe that will send one message with `sendOne` shile updating the `openRequests`.
      * The `openRequests` or not updated for ProduceRequests that do not expect confirmation from kafka
      * @param openRequests
      * @param sendOne
      * @param F
      * @tparam F
      * @return
      */
    def sendMessages[F[_] : Applicative](
     openRequests: Ref[F,Map[Int,RequestMessage]]
     , sendOne: Chunk[Byte] => F[Unit]
    )(implicit F: Monad[F]):Sink[F,RequestMessage] = {
      _.evalMap { rm =>
        rm.request match {
          case produce: ProduceRequest if produce.requiredAcks == RequiredAcks.NoResponse =>
            Applicative[F].pure(rm)
          case _ =>
            openRequests.update(_ + (rm.correlationId -> rm)) as rm
        }
      }
       .flatMap { rm =>
         MessageCodec.requestCodec.encode(rm).fold(
           err => raiseError(new Throwable(s"Failed to serialize message: $err : $rm"))
           , data => eval(sendOne(Chunk.bytes(data.toByteArray)))
         )
       }
    }


    def receiveMessages[F[_]](
       openRequests: Ref[F,Map[Int,RequestMessage]]
    ):Pipe[F,Byte,ResponseMessage] = {
      _.through(receiveChunks)
      .through(decodeReceived(openRequests))
    }

    /**
      * Collects bytes as they arrive producing chunks of ByteVector
      * This reads 32 bits size first, then it reads up to that size of message data
      * emitting single ByteVector.
      *
      * This combinator respects chunks. So if there was more chunks collected in single
      * go, they all are emitted in chunk.
      *
      *
      * @return
      */
    def receiveChunks[F[_]]: Pipe[F,Byte,ByteVector] = {

      def go(acc: ByteVector, msgSz: Option[Int], s: Stream[F, Byte]): Pull[F, ByteVector, Unit] = {
        s.pull.unconsChunk flatMap {
          case Some((ch, tail)) =>
            val bs = ch.toBytes
            val buff = acc ++ ByteVector.view(bs.values, bs.offset, bs.size)
            val (rem, sz, out) = collectChunks(buff, msgSz)

            Pull.segment(out) >> go(rem, sz, tail)

          case None =>
            if (acc.nonEmpty) Pull.raiseError(new Throwable(s"Input terminated before all data were consumed. Buff: $acc"))
            else Pull.done
        }
      }

      s => go(ByteVector.empty, None, s).stream
    }


    /**
      * Collects chunks of messages received.
      * Each chunk is forming whole message, that means this looks for the first 4 bytes, that indicates message size,
      * then this take up to that size to produce single ByteVector of message content, and emits that
      * content it term of Segment. Note that Segment may be empty or may contain multiple characters
      */

    def collectChunks(
      in: ByteVector
      , msgSz:Option[Int]
    ):(ByteVector, Option[Int], Segment[ByteVector, Unit]) = {
      @tailrec
      def go(buff: ByteVector, currSz: Option[Int], acc: Vector[ByteVector]): (ByteVector, Option[Int], Segment[ByteVector, Unit]) = {
        currSz match {
          case None =>
            if (buff.size < 4) (buff, None, Segment.indexedSeq(acc))
            else {
              val (sz, rem) = buff.splitAt(4)
              go(rem, Some(sz.toInt()), acc)
            }

          case Some(sz) =>
            if (buff.size < sz) (buff, Some(sz), Segment.indexedSeq(acc))
            else {
              val (h,t) = buff.splitAt(sz)
              go(t, None, acc :+ h)
            }
        }
      }
      go(in, msgSz, Vector.empty)
    }


    /**
      * Decodes message received. Due to kafka protocol not having response type encoded
      * in protocol itself, we need to consult correlation id, that is read first from the
      * message to identify response type.
      *
      * If request is found for given message, this will remove that request from supplied
      * map and will deserialize message according the request type.
      *
      * If request cannot be found, this fails, as well as when message cannot be decoded.
      *
      * @param openRequests   Ref of current open requests.
      * @tparam F
      * @return
      */
    def decodeReceived[F[_]](
      openRequests: Ref[F,Map[Int,RequestMessage]]
    ):Pipe[F,ByteVector,ResponseMessage] = {
      _.flatMap { bs =>
        if (bs.size < 4) Stream.raiseError(new Throwable(s"Message chunk does not have correlation id included: $bs"))
        else {
          val correlationId = bs.take(4).toInt()
          eval(openRequests.modify { m => (m - correlationId, m) }).flatMap { m =>
            m.get(correlationId) match {
              case None => Stream.raiseError(new Throwable(s"Received message correlationId for message that does not exists: $correlationId : $bs : $m"))
              case Some(req) =>
                MessageCodec.responseCodecFor(req.version, ApiKey.forRequest(req.request)).decode(bs.drop(4).bits)
                .fold(
                  err => Stream.raiseError(new Throwable(s"Failed to decode response to request: $err : $req : $bs"))
                  , result => Stream.emit(ResponseMessage(correlationId,result.value))
                )
            }
          }
        }
      }
    }

  }

}
