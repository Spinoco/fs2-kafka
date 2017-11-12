package spinoco.fs2.kafka

import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors

import fs2._
import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.{FreeSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.time.SpanSugar._

import scala.concurrent.ExecutionContext



class Fs2KafkaClientSpec extends FreeSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with TimeLimitedTests
  with Eventually {

  val timeLimit = 90.seconds

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = timeLimit)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 25, workers = 1)


  implicit val EC: ExecutionContext = ExecutionContext.global
  implicit val S: Scheduler =  fs2.Scheduler.fromScheduledExecutorService(Executors.newScheduledThreadPool(4))
  implicit val AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(8))

  val TestTopic:String = "test-topic"




}