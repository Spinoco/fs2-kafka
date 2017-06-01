package spinoco.fs2.kafka

import fs2._
import fs2.Task
import fs2.util.Async
import shapeless.tag
import shapeless.tag.@@

import scala.sys.process.{Process, ProcessLogger}

/**
  *Various helpers for interacting with docker instance
  */
object DockerSupport {
  val ExtractVersion = """Docker version ([0-9\.\-a-z]+), build ([0-9a-fA-F]+).*""".r("version", "build")
  sealed trait DockerId

  /** Returns version of docker, if that docker is available. **/
  def dockerVersion:Task[Option[String]] = Task.delay {
    val output = Process("docker -v").!!
    println(s"XXXXY ${output} = ${ExtractVersion.findAllMatchIn(output).toList}")
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
  def installImageWhenNeeded(imageName:String):Task[Boolean] = Task.delay {
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
  def runImage(imageName: String, name: Option[String])(props: String*):Task[String @@ DockerId] = Task.delay {
    val cmd = s"docker run -d ${ name.map(n => s"--name=$n").mkString } ${props.mkString(" ")} $imageName"
    println(s"RUNNING: $cmd")
    tag[DockerId](Process(cmd).!!.trim)
  }

  /**
    * Follows log of executed image. Adequate to docker logs -f <imageId>
    * @param imageId  Id of image to follow
    * @return
    */
  def followImageLog(imageId:String @@ DockerId)(implicit F:Async[Task]):Stream[Task,String] = {
    Stream.eval(async.unboundedQueue[Task,String]).flatMap { q =>
      val logger = new ProcessLogger {
        def buffer[T](f: => T): T = f
        def out(s: => String): Unit = q.enqueue1(s).unsafeRun()
        def err(s: => String): Unit = q.enqueue1(s).unsafeRun()
      }

      Stream.bracket(Task.delay(Process(s"docker logs -f $imageId").run(logger)))(
        _ => q.dequeue
        , p => Task.delay(p.destroy())
      )
    }
  }

  /**
    * Issues a kill to image with given id
    */
  def killImage(imageId:String @@ DockerId):Task[Unit] = Task.delay {
    Process(s"docker kill $imageId").!!
    ()
  }

  /**
    * Cleans supplied image from the docker
    */
  def cleanImage(imageId:String @@ DockerId):Task[Unit] = Task.delay {
    Process(s"docker rm $imageId").!!
    ()
  }


  def createNetwork(name: String, ipSubnet:String = "172.25.0.0/16 "): Task[Unit] = Task.delay {
    Process(s"""docker network create --subnet $ipSubnet $name""").!!
    ()
  }


  def removeNetwork(name: String): Task[Unit] = Task.delay {
    Process(s"""docker network rm $name""").!!
    ()
  }



}
