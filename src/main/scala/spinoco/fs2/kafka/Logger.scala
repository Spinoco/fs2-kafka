package spinoco.fs2.kafka

import java.util.logging.LogRecord

import fs2._
import fs2.util.Async
import spinoco.fs2.kafka.Logger.Level

/**
  * Logger trait allowing to attach any Logging framework required.
  * Jdk Instance is available
  */
trait Logger[F[_]] {

  /** performs log on given level. Note that throwable may be null. **/
  def log(level: Level.Value, msg: => String, throwable: Throwable): F[Unit]


}


object Logger {

  object Level extends Enumeration {
    val Trace, Debug, Info, Warn, Error = Value
  }


  implicit class LoggerSyntax[F[_]](val self:Logger[F]) extends AnyVal{
    @inline def trace(msg: => String, thrown:Throwable = null): F[Unit] = self.log(Level.Trace, msg, thrown)
    @inline def debug(msg: => String, thrown:Throwable = null): F[Unit] = self.log(Level.Debug, msg, thrown)
    @inline def info(msg: => String, thrown:Throwable = null): F[Unit] = self.log(Level.Info, msg, thrown)
    @inline def warn(msg: => String, thrown:Throwable = null): F[Unit] = self.log(Level.Warn, msg, thrown)
    @inline def error(msg: => String, thrown:Throwable = null): F[Unit] = self.log(Level.Error, msg, thrown)
    @inline def trace2(msg: => String, thrown:Throwable = null): Stream[F, Unit] = Stream.eval(self.log(Level.Trace, msg, thrown))
    @inline def debug2(msg: => String, thrown:Throwable = null): Stream[F, Unit] = Stream.eval(self.log(Level.Debug, msg, thrown))
    @inline def info2(msg: => String, thrown:Throwable = null): Stream[F, Unit] = Stream.eval(self.log(Level.Info, msg, thrown))
    @inline def warn2(msg: => String, thrown:Throwable = null): Stream[F, Unit] = Stream.eval(self.log(Level.Warn, msg, thrown))
    @inline def error2(msg: => String, thrown:Throwable = null): Stream[F, Unit] = Stream.eval(self.log(Level.Error, msg, thrown))
  }


  def JDKLogger[F[_]](jdkLogger:java.util.logging.Logger)(implicit F: Async[F]):F[Logger[F]] = F.delay {
    new Logger[F] {
      def log(level: Logger.Level.Value, msg: => String, throwable: Throwable): F[Unit] = F.delay {
        val jdkLevel =
          level match {
            case Level.Trace => java.util.logging.Level.FINEST
            case Level.Debug => java.util.logging.Level.FINE
            case Level.Info => java.util.logging.Level.INFO
            case Level.Warn => java.util.logging.Level.WARNING
            case Level.Error => java.util.logging.Level.SEVERE
          }
        if (jdkLogger.isLoggable(jdkLevel)) {
          val record = new LogRecord(jdkLevel,msg)
          if (record != null) record.setThrown(throwable)
          jdkLogger.log(record)
        }
      }
    }

  }

}
