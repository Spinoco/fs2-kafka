package spinoco.fs2.kafka.network

import java.net.{InetSocketAddress, SocketAddress, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels._
import java.nio.channels.spi.AsynchronousChannelProvider
import java.util.concurrent.TimeUnit


import fs2.Stream._
import fs2._
import fs2.io.tcp.Socket
import fs2.util.Async
import fs2.util.syntax._

import scala.concurrent.duration.FiniteDuration

/**
  * Created by pach on 06/06/17.
  */
object tcp2 {

  /** see [[fs2.io.tcp.client]] **/
  def client[F[_]](
    to: InetSocketAddress
    , reuseAddress: Boolean = true
    , sendBufferSize: Int = 1024*256
    , receiveBufferSize: Int = 1024*256
    , keepAlive: Boolean = false
    , noDelay: Boolean = false
  )(
    implicit
    AG: AsynchronousChannelGroup
    , F:Async[F]
  ): Stream[F,Socket[F]] = Stream.suspend {

    def setup: Stream[F,AsynchronousSocketChannel] = Stream.suspend {
      val ch = AsynchronousChannelProvider.provider().openAsynchronousSocketChannel(AG)
      ch.setOption[java.lang.Boolean](StandardSocketOptions.SO_REUSEADDR, reuseAddress)
      ch.setOption[Integer](StandardSocketOptions.SO_SNDBUF, sendBufferSize)
      ch.setOption[Integer](StandardSocketOptions.SO_RCVBUF, receiveBufferSize)
      ch.setOption[java.lang.Boolean](StandardSocketOptions.SO_KEEPALIVE, keepAlive)
      ch.setOption[java.lang.Boolean](StandardSocketOptions.TCP_NODELAY, noDelay)
      Stream.emit(ch)
    }

    def connect(ch: AsynchronousSocketChannel): F[AsynchronousSocketChannel] = F.async { cb =>
      F.delay {
        ch.connect(to, null, new CompletionHandler[Void, Void] {
          def completed(result: Void, attachment: Void): Unit = F.unsafeRunAsync(F.start(F.delay(cb(Right(ch)))))(_ => ())
          def failed(rsn: Throwable, attachment: Void): Unit = F.unsafeRunAsync(F.start(F.delay(cb(Left(rsn)))))(_ => ())
        })
      }
    }

    def cleanup(ch: AsynchronousSocketChannel): F[Unit] =
      F.delay  { ch.close() }

    setup flatMap { ch =>
      Stream.bracket(connect(ch))( {_ => eval(mkSocket(ch)) }, cleanup)
    }
  }


  def server[F[_]](
                    address: InetSocketAddress
                    , maxQueued: Int
                    , reuseAddress: Boolean
                    , receiveBufferSize: Int )(
                    implicit AG: AsynchronousChannelGroup
                    , F:Async[F]
                  ): Stream[F, Either[InetSocketAddress, Stream[F, Socket[F]]]] = Stream.suspend {

    def setup: F[AsynchronousServerSocketChannel] = F.delay {
      val ch = AsynchronousChannelProvider.provider().openAsynchronousServerSocketChannel(AG)
      ch.setOption[java.lang.Boolean](StandardSocketOptions.SO_REUSEADDR, reuseAddress)
      ch.setOption[Integer](StandardSocketOptions.SO_RCVBUF, receiveBufferSize)
      ch.bind(address)
      ch
    }

    def cleanup(sch: AsynchronousServerSocketChannel): F[Unit] = F.delay{
      if (sch.isOpen) sch.close()
    }

    def acceptIncoming(sch: AsynchronousServerSocketChannel): Stream[F,Stream[F, Socket[F]]] = {
      def go: Stream[F,Stream[F, Socket[F]]] = {
        def acceptChannel: F[AsynchronousSocketChannel] =
          F.async[AsynchronousSocketChannel] { cb => F.pure {
            sch.accept(null, new CompletionHandler[AsynchronousSocketChannel, Void] {
              def completed(ch: AsynchronousSocketChannel, attachment: Void): Unit = F.unsafeRunAsync(F.start(F.delay(cb(Right(ch)))))(_ => ())
              def failed(rsn: Throwable, attachment: Void): Unit = F.unsafeRunAsync(F.start(F.delay(cb(Left(rsn)))))(_ => ())
            })
          }}

        def close(ch: AsynchronousSocketChannel): F[Unit] =
          F.delay { if (ch.isOpen) ch.close() }.attempt.as(())

        eval(acceptChannel.attempt).map {
          case Left(err) => Stream.empty
          case Right(accepted) => eval(mkSocket(accepted)).onFinalize(close(accepted))
        } ++ go
      }

      go.onError {
        case err: AsynchronousCloseException =>
          if (sch.isOpen) Stream.fail(err)
          else Stream.empty
        case err => Stream.fail(err)
      }
    }

    Stream.bracket(setup)(sch => Stream.emit(Left(sch.getLocalAddress.asInstanceOf[InetSocketAddress])) ++ acceptIncoming(sch).map(Right(_)), cleanup)
  }


  def mkSocket[F[_]](ch:AsynchronousSocketChannel)(implicit F:Async[F]):F[Socket[F]] = {
    async.semaphore(1) flatMap { readSemaphore =>
      F.refOf(ByteBuffer.allocate(0)) map { bufferRef =>

        // Reads data to remaining capacity of supplied ByteBuffer
        // Also measures time the read took returning this as tuple
        // of (bytes_read, read_duration)
        def readChunk(buff:ByteBuffer, timeoutMs:Long):F[(Int,Long)] = F.async { cb => F.pure {
          val started = System.currentTimeMillis()
          println(s"STARTING TO READ: ${ch.getLocalAddress}")
          ch.read(buff, timeoutMs, TimeUnit.MILLISECONDS, (), new CompletionHandler[Integer, Unit] {
            def completed(result: Integer, attachment: Unit): Unit =  {
              println(s"READ COMPLETED: ${ch.getLocalAddress} : $result")
              val took = System.currentTimeMillis() - started
              F.unsafeRunAsync(F.start(F.delay(cb(Right((result, took))))))(_ => ())
            }
            def failed(err: Throwable, attachment: Unit): Unit = {
              println(s"FAILED TO READ: ${ch.getLocalAddress} : $err")
              F.unsafeRunAsync(F.start(F.delay(cb(Left(err)))))(_ => ())
            }
          })
        }}

        // gets buffer of desired capacity, ready for the first read operation
        // If the buffer does not have desired capacity it is resized (recreated)
        // buffer is also reset to be ready to be written into.
        def getBufferOf(sz: Int): F[ByteBuffer] = {
          bufferRef.get.flatMap { buff =>
            if (buff.capacity() < sz) bufferRef.modify { _ => ByteBuffer.allocate(sz) } map { _.now }
            else {
              buff.clear()
              F.pure(buff)
            }
          }
        }

        // When the read operation is done, this will read up to buffer's position bytes from the buffer
        // this expects the buffer's position to be at bytes read + 1
        def releaseBuffer(buff: ByteBuffer): F[Chunk[Byte]] = {
          val read = buff.position()
          if (read == 0) F.pure(Chunk.bytes(Array.empty))
          else {
            val dest = Array.ofDim[Byte](read)
            buff.flip()
            buff.get(dest)
            F.pure(Chunk.bytes(dest))
          }
        }

        def read0(max:Int, timeout:Option[FiniteDuration]):F[Option[Chunk[Byte]]] = {
          readSemaphore.decrement >>
            F.attempt[Option[Chunk[Byte]]](getBufferOf(max) flatMap { buff =>
              readChunk(buff, timeout.map(_.toMillis).getOrElse(0l)) flatMap {
                case (read, _) =>
                  if (read < 0) { println("READ DONE "); F.pure(None) }
                  else releaseBuffer(buff) map (Some(_))
              }
            }).flatMap { r => readSemaphore.increment >> (r match {
              case Left(err) => F.fail(err)
              case Right(maybeChunk) => F.pure(maybeChunk)
            })}
        }

        def readN0(max:Int, timeout:Option[FiniteDuration]):F[Option[Chunk[Byte]]] = {
          readSemaphore.decrement >>
            F.attempt(getBufferOf(max) flatMap { buff =>
              def go(timeoutMs: Long): F[Option[Chunk[Byte]]] = {
                readChunk(buff, timeoutMs) flatMap { case (readBytes, took) =>
                  if (readBytes < 0 || buff.position() >= max) {
                    // read is done
                    releaseBuffer(buff) map (Some(_))
                  } else go((timeoutMs - took) max 0)
                }
              }

              go(timeout.map(_.toMillis).getOrElse(0l))
            }) flatMap { r => readSemaphore.increment >> (r match {
            case Left(err) => F.fail(err)
            case Right(maybeChunk) => F.pure(maybeChunk)
          })}
        }

        def write0(bytes:Chunk[Byte],timeout: Option[FiniteDuration]): F[Unit] = {
          def go(buff:ByteBuffer,remains:Long):F[Unit] = {
            F.async[Option[Long]] { cb => F.pure {
              val start = System.currentTimeMillis()
              ch.write(buff, remains, TimeUnit.MILLISECONDS, (), new CompletionHandler[Integer, Unit] {
                def completed(result: Integer, attachment: Unit): Unit = {
                  println(s"WRITE COMPLETED: $result")
                  F.unsafeRunAsync(F.start(F.delay(cb(Right(
                    if (buff.remaining() <= 0) None
                    else Some(System.currentTimeMillis() - start)
                  )))))(_ => ())
                }
                def failed(err: Throwable, attachment: Unit): Unit = {
                  println(s"WRITE FAILED: ERR: $err")
                  F.unsafeRunAsync(F.start(F.delay(cb(Left(err)))))(_ => ())
                }
              })
            }}.flatMap {
              case None => F.pure(())
              case Some(took) => go(buff,(remains - took) max 0)
            }
          }

          val bytes0 = bytes.toBytes
          go(
            ByteBuffer.wrap(bytes0.values, bytes0.offset, bytes0.size)
            , timeout.map(_.toMillis).getOrElse(0l)
          )
        }

        ///////////////////////////////////
        ///////////////////////////////////


        new Socket[F] {
          def readN(numBytes: Int, timeout: Option[FiniteDuration]): F[Option[Chunk[Byte]]] = readN0(numBytes,timeout)
          def read(maxBytes: Int, timeout: Option[FiniteDuration]): F[Option[Chunk[Byte]]] = read0(maxBytes,timeout)
          def reads(maxBytes: Int, timeout: Option[FiniteDuration]): Stream[F, Byte] = {
            Stream.eval(read(maxBytes,timeout)) flatMap {
              case Some(bytes) => Stream.chunk(bytes) ++ reads(maxBytes, timeout)
              case None => Stream.empty
            }
          }

          def write(bytes: Chunk[Byte], timeout: Option[FiniteDuration]): F[Unit] = write0(bytes,timeout)
          def writes(timeout: Option[FiniteDuration]): Sink[F, Byte] =
            _.chunks.flatMap { bs => Stream.eval(write(bs, timeout)) }

          def localAddress: F[SocketAddress] = F.delay(ch.getLocalAddress)
          def remoteAddress: F[SocketAddress] = F.delay(ch.getRemoteAddress)
          def close: F[Unit] = F.delay(ch.close())
          def endOfOutput: F[Unit] = F.delay{ ch.shutdownOutput(); () }
          def endOfInput: F[Unit] = F.delay{ ch.shutdownInput(); () }
        }
      }}
  }


}
