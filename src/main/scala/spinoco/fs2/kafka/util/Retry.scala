package spinoco.fs2.kafka.util

import cats.effect.Temporal
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

object Retry {

  /**
    * Evaluates `fa`. When it fails with an error for which `retriable` holds, waits `delay` and evaluates `fa` again.
    * After `maxAttempts` attempts (must be > 0) this fails with the last failure. Failure that is not retriable is returned immediately.
    */
  def withConstantDelay[F[_] : Temporal, A](fa: F[A], delay: FiniteDuration, maxAttempts: Int)(retriable: Throwable => Boolean): F[A] =
    Stream.retry(fa, delay, identity[FiniteDuration], maxAttempts, retriable).compile.lastOrError

  object syntax {

    implicit class RetrySyntax[F[_], A](val self: F[A]) extends AnyVal {

      /** See [[Retry.withConstantDelay]] **/
      def retry(delay: FiniteDuration, maxAttempts: Int)(retriable: Throwable => Boolean)(implicit F: Temporal[F]): F[A] =
        withConstantDelay(self, delay, maxAttempts)(retriable)

    }

  }

}
