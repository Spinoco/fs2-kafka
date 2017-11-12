package spinoco.fs2.kafka

import cats.effect.{Effect, IO}
import cats.syntax.all._

import fs2._
import shapeless.tag
import shapeless.tag.@@

import scala.concurrent.ExecutionContext
import scala.sys.process.{Process, ProcessLogger}

/**
  *Various helpers for interacting with docker instance
  */
object DockerSupport {
  val ExtractVersion = """Docker version ([0-9\.\-a-z]+), build ([0-9a-fA-F]+).*""".r("version", "build")
  sealed trait DockerId

  /** Returns version of docker, if that docker is available. **/
  def dockerVersion:IO[Option[String]] = IO {
    val output = Process("docker -v").!!
    for {
      m <- ExtractVersion.findAllMatchIn(output).toList.headOption
      version <- Option(m.group("version"))
      build <- Option(m.group("build"))
    } yield version + " " + build
  }

  /**
    *  installs given image, if needed, that means when it is not yet installed locally.
    *  Returns true if image was pulled, false if it was already present
    * @param imageName  name of the image inclusive the tag
    * @return
    */
  def installImageWhenNeeded(imageName:String):IO[Boolean] = IO {
    val current:String= Process(s"docker images $imageName -q").!!
    if (current.lines.isEmpty) {
      Process(s"docker pull $imageName").!!
      true
    } else false
  }

  /**
    * Runs the given image (in background), returning the `id` of the image as an result.
    * @param imageName      Name (uri) of the image
    * @param name           Name of the image run (must be unique or not supplied)
    * @param props          Parameters, props to docker `run` command
    * @return
    */
  def runImage(imageName: String, name: Option[String])(props: String*):IO[String @@ DockerId] = IO {
    val cmd = s"docker run -d ${ name.map(n => s"--name=$n").mkString } ${props.mkString(" ")} $imageName"
    tag[DockerId](Process(cmd).!!.trim)
  }

  /**
    * Follows log of executed image. Adequate to docker logs -f <imageId>
    * @param imageId  Id of image to follow
    * @return
    */
  def followImageLog(imageId:String @@ DockerId)(implicit F: Effect[IO], ec: ExecutionContext): Stream[IO,String] = {
    Stream.eval(async.semaphore(1)) flatMap { semaphore =>
    Stream.eval(async.refOf(false)) flatMap { isDone =>
    Stream.eval(async.unboundedQueue[IO,String]).flatMap { q =>

      def enqueue(s: String): Unit = {
        semaphore.increment *>
        isDone.get.flatMap { done => if (!done) q.enqueue1(s) else IO.unit } *>
        semaphore.decrement
      } unsafeRunSync

      val logger = new ProcessLogger {
        def buffer[T](f: => T): T = f
        def out(s: => String): Unit = enqueue(s)

        def err(s: => String): Unit = enqueue(s)
      }

      Stream.bracket(IO(Process(s"docker logs -f $imageId").run(logger)))(
        _ => q.dequeue
        , p => semaphore.increment *> isDone.modify(_ => true) *> IO(p.destroy()) *> semaphore.decrement
      )
    }}}
  }

  def runningImages: IO[Set[String @@ DockerId]] = IO {
    Process(s"docker ps -q").!!.lines.filter(_.trim.nonEmpty).map(tag[DockerId](_)).toSet
  }

  def availableImages: IO[Set[String @@ DockerId]] = IO {
    Process(s"docker ps -aq").!!.lines.filter(_.trim.nonEmpty).map(tag[DockerId](_)).toSet
  }


  /**
    * Issues a kill to image with given id
    */
  def killImage(imageId: String @@ DockerId):IO[Unit] = {
    IO { Process(s"docker kill $imageId").!! } *>
    runningImages.flatMap { allRun =>
      if (allRun.exists(imageId.startsWith)) killImage(imageId)
      else IO.pure(())
    }
  }

  /**
    * Cleans supplied image from the docker
    */
  def cleanImage(imageId: String @@ DockerId):IO[Unit] = {
    IO { Process(s"docker rm $imageId").!! } *>
    availableImages.flatMap { allAvail =>
      if (allAvail.exists(imageId.startsWith)) cleanImage(imageId)
      else IO.pure(())
    }
  }


  def createNetwork(name: String, ipSubnet:String = "172.30.0.0/16 "): IO[Unit] = IO {
    Process(s"""docker network create --subnet $ipSubnet $name""").!!
    ()
  }


  def removeNetwork(name: String): IO[Unit] = IO {
    Process(s"""docker network rm $name""").!!
    ()
  }



}
