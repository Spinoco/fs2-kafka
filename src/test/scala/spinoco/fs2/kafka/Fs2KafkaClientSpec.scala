package spinoco.fs2.kafka

import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors

import fs2._
import fs2.util.Async
import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.{FreeSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.time.SpanSugar._



class Fs2KafkaClientSpec extends FreeSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with TimeLimitedTests
  with Eventually {





  val timeLimit = 90.seconds

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = timeLimit)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 25, workers = 1)


  implicit val S: Strategy = Strategy.fromFixedDaemonPool(8,"fs2-kafka-spec")
  implicit val Sch: Scheduler =  Scheduler.fromFixedDaemonPool(4, "fs2-kafka-spec-scheduler")
  implicit val AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(8, Strategy.daemonThreadFactory("fs2-kafka-spec-acg")))
  implicit val F: Async[fs2.Task] = fs2.Task.asyncInstance

  val TestTopic:String = "test-topic"




}