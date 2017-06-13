package spinoco.fs2.kafka.network

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup

import fs2._
import Stream._
import fs2.util.Async.Change
import fs2.util._
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
  def apply[F[_]](
    address: InetSocketAddress
    , writeTimeout: Option[FiniteDuration] = None
    , readMaxChunkSize: Int = 256 * 1024      // 256 Kilobytes
  )(implicit AG:AsynchronousChannelGroup, F: Async[F]): Pipe[F, RequestMessage, ResponseMessage] = {
    (source: Stream[F,RequestMessage]) =>
      fs2.io.tcp.client(address).flatMap { socket =>
        eval(F.refOf(Map.empty[Int,RequestMessage])).flatMap { openRequests =>
          val send = source.through(impl.sendMessages(
            openRequests = openRequests
            , sendOne = (x) => socket.write(x, writeTimeout)
          ))

          val receive =
            socket.reads(readMaxChunkSize, timeout = None)
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
    def sendMessages[F[_]](
     openRequests: Async.Ref[F,Map[Int,RequestMessage]]
     , sendOne: Chunk[Byte] => F[Unit]
    )(implicit F: Monad[F]):Sink[F,RequestMessage] = {
      _.evalMap { rm =>
        rm.request match {
          case produce: ProduceRequest if produce.requiredAcks == RequiredAcks.NoResponse =>
            F.pure(rm)
          case _ =>
            F.map(openRequests.modify(_ + (rm.correlationId -> rm))){ _ => rm }
        }
      }
       .flatMap { rm =>
         println(s"XXXY >>> : $rm")
         MessageCodec.requestCodec.encode(rm).fold(
           err => fail(new Throwable(s"Failed to serialize message: $err : $rm"))
           , data => eval(sendOne(Chunk.bytes(data.toByteArray)))
         )
       }
    }


    def receiveMessages[F[_]](
       openRequests: Async.Ref[F,Map[Int,RequestMessage]]
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
    def receiveChunks[F[_]]:Pipe[F,Byte,ByteVector] = {
      def go(acc:ByteVector,msgSz:Option[Int]): Handle[F,Byte] => Pull[F,ByteVector,Unit] = {
        _.receive { case (ch,h) =>
          val bytes = ch.toBytes
          val bv = ByteVector.view(bytes.values).drop(bytes.offset).take(bytes.size)

          val (acc0, rem0, out) = collectChunks( Vector.empty, msgSz, acc ++ bv)
          Pull.outputs(out) >> go(acc0,rem0)(h)
        }
      }
      _.pull(go(ByteVector.empty,None))
    }

    /**
      * Collects chunk of messages received from the broker.
      * If any message size is larger then `maxMessageSize` this will fail w/o emitting single value
      * returns remainder to be processed when more data are available, Size of required chunk, and resulting stream.
      */
    @tailrec
    def collectChunks[F[_]](
      acc:Vector[ByteVector]
      , msgSz:Option[Int]
      , received: ByteVector
    ):(ByteVector,Option[Int],Stream[F,ByteVector]) = {
      msgSz match {
        case None =>
          if (received.size < 4) (received, None, Stream.emits(acc))
          else {
            val sz = received.take(4).toInt()
            collectChunks[F](acc,Some(sz),received.drop(4))
          }

        case Some(sz) =>
          val (chunk, rest)  = received.splitAt(sz)
          if (rest.nonEmpty) collectChunks[F](acc :+ chunk, None, rest)
          else {
            if (chunk.size.toInt == sz) (rest,None,Stream.emits(acc :+ chunk))
            else (chunk, Some(sz), Stream.emits(acc))
          }
      }
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
      openRequests: Async.Ref[F,Map[Int,RequestMessage]]
    ):Pipe[F,ByteVector,ResponseMessage] = {
      _.flatMap { bs =>
        if (bs.size < 4) Stream.fail(new Throwable(s"Message chunk does not have correlation id included: $bs"))
        else {
          val correlationId = bs.take(4).toInt()
          eval(openRequests.modify{ _ - correlationId}).flatMap { case Change(m,_) =>
            m.get(correlationId) match {
              case None => Stream.fail(new Throwable(s"Received message correlationId for message that does not exists: $correlationId : $bs : $m"))
              case Some(req) =>
                MessageCodec.responseCodecFor(req.version, ApiKey.forRequest(req.request)).decode(bs.drop(4).bits)
                .fold(
                  err => Stream.fail(new Throwable(s"Failed to decode repsonse to request: $err : $req : $bs"))
                  , result => Stream.emit(ResponseMessage(correlationId,result.value))
                )
            }
          }
        }
      } map { x => println(s"XXXY <<<< $x"); x}
    }

  }

}
