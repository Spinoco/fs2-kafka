package spinoco.fs2.kafka

import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors

import cats.effect.{Concurrent, IO, Timer}
import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.{FreeSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.time.SpanSugar._

import scala.concurrent.ExecutionContext

object Fs2KafkaClientResources {
  implicit val _timer: Timer[IO] = IO.timer(ExecutionContext.global)
  implicit val _concurrent: Concurrent[IO] = IO.ioConcurrentEffect(_timer)
  implicit val AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(8))


}

class Fs2KafkaClientSpec extends FreeSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with TimeLimitedTests
  with Eventually {

  val timeLimit = 90.seconds

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = timeLimit)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 25, workers = 1)


  implicit val _timer: Timer[IO] = Fs2KafkaClientResources._timer
  implicit val _concurrent: Concurrent[IO] = Fs2KafkaClientResources._concurrent
  implicit val AG: AsynchronousChannelGroup = Fs2KafkaClientResources.AG

  val TestTopic:String = "test-topic"




}