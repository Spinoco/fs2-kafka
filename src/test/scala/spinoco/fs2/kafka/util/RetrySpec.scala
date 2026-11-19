package spinoco.fs2.kafka.util

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import spinoco.fs2.kafka.Fs2KafkaClientSpec

import scala.concurrent.duration._

class RetrySpec extends Fs2KafkaClientSpec {

  case object Transient extends Throwable("transient")
  case object Permanent extends Throwable("permanent")

  def failFirst(attempts: Ref[IO, Int], failures: Int, failure: Throwable): IO[Int] =
    attempts.updateAndGet(_ + 1) flatMap { n => if (n <= failures) IO.raiseError(failure) else IO.pure(n) }

  def run[A](io: IO[A]): A = io.unsafeRunTimed(30.seconds).getOrElse(fail("Timed out"))

  "withConstantDelay" - {

    def attemptRetry(failures: Int, failure: Throwable, maxAttempts: Int): IO[(Either[Throwable, Int], Int)] =
      Ref.of[IO, Int](0) flatMap { attempts =>
        Retry.withConstantDelay(failFirst(attempts, failures, failure), 1.millis, maxAttempts)(_ == Transient).attempt flatMap { r => attempts.get.map(r -> _) }
      }

    "retries retriable failure until success" in {
      run(attemptRetry(failures = 3, Transient, maxAttempts = 5)) shouldBe ((Right(4), 4))
    }

    "fails with last failure after max attempts" in {
      run(attemptRetry(failures = 10, Transient, maxAttempts = 5)) shouldBe ((Left(Transient), 5))
    }

    "returns non retriable failure immediately" in {
      run(attemptRetry(failures = 3, Permanent, maxAttempts = 5)) shouldBe ((Left(Permanent), 1))
    }

    "is available as syntax" in {
      import Retry.syntax._
      run(Ref.of[IO, Int](0) flatMap { attempts => failFirst(attempts, 3, Transient).retry(1.millis, 5)(_ == Transient) }) shouldBe 4
    }

  }

}
