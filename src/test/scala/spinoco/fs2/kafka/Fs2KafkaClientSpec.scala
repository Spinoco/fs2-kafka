package spinoco.fs2.kafka

import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors

import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.time.SpanSugar._


object Fs2KafkaClientResources {
  implicit val AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(8))


}

class Fs2KafkaClientSpec extends AnyFreeSpec
  with ScalaCheckDrivenPropertyChecks
  with Matchers
  with TimeLimitedTests
  with Eventually {

  val timeLimit = 90.seconds

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = timeLimit)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 25, workers = 1)


  implicit val AG: AsynchronousChannelGroup = Fs2KafkaClientResources.AG

  val TestTopic:String = "test-topic"




}