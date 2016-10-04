package spinoco.fs2.kafka


import fs2._
import fs2.Stream._

import scala.concurrent.duration._


/**
  * Created by pach on 30/05/16.
  */
class PlayGroundSpec extends Fs2KafkaClientSpec {

  " Playground" - {

    " fails the stream" in {

      println("STARTING")

      eval(async.unboundedQueue[Task,Int]).flatMap { q =>
        println("XXXX GOT Q")
        val source =
          q.dequeue.map { i =>
            emit(i) ++ time.sleep[Task](i*5.seconds) ++ Stream.fail(new Throwable(s"Kaboom $i"))
          }
        concurrent.join(Int.MaxValue)(
          emit(emit(0)) ++
          Stream(1,2,3).evalMap(q.enqueue1).drain ++
          source.map{x => println(s"XXX $x"); x.drain}
        )
      }
      .map{ i => println(s">>> $i"); time.sleep[Task](1.minute)}
      .run.unsafeRun


    }

//    "concurrent failure" in {
//
//      def perfrom[F[_]](msg:Int*)(implicit F:Async[F]):Stream[F,Unit] = {
//        eval(async.signalOf[F,Boolean](false)).flatMap {  sig =>
//          eval(async.unboundedQueue[F,Int]).flatMap { q =>
//            val deq =
//              (emit(-1) ++ q.dequeue). flatMap { i =>
//                eval(F.bind(F.suspend(F.pure( if (i < 0) i else  throw new Throwable(s"Kabooom $i"))))(_ => F.pure(())))
//              }
//            val enq = emits(msg).flatMap(i => eval(q.enqueue1(i)))
//
//
//            (deq merge enq) interruptWhen(sig.discrete)
//          }
//
//        }}
//
//
//      val p =
//      concurrent.join[Task,Int](3)(Stream(
//        emit(1) ++ time.sleep[Task](1.minute)
//        , emit(2) ++ time.sleep[Task](1.minute)
//        , emit(3) ++ time.sleep[Task](1.minute)
//      ))
//      .chunkN(3,allowFewer = false)
//      .take(1)
//      .flatMap { chunks =>
//
//        perfrom[Task](1)
//      }
//
//      p.run.unsafeRun
//
//    }

  }

}
