package spinoco.fs2.kafka


import fs2._

import scala.concurrent.duration._

/**
  * Created by pach on 12/06/17.
  */
object TestApp extends App{

  implicit val S: Scheduler = Scheduler.fromFixedDaemonPool(4)
  implicit val ST: Strategy = Strategy.fromFixedDaemonPool(4)


  def withResources: Stream[Task, String] = {
    Stream.eval(Task.delay(println("RESOURCE ABOUT TO BE ALLOCATED"))) >>
    Stream.eval(Task.delay {println("ALLOCATED") }) flatMap { _ =>
      (time.sleep_[Task](1.seconds) ++ Stream.emit("RESOURCE"))
      .onFinalize {
        Task.delay { println("RESOURCE RELEASED") }
      }
    }

  }


  ((withResources flatMap { r =>
    Stream.eval_(Task.delay(println(s"USING $r")))
  } runLog) unsafeTimed(1 minute)) unsafeRun()


}
