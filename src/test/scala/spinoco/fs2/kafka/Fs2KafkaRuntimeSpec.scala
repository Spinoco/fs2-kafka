package spinoco.fs2.kafka

import java.net.InetAddress

import fs2.Task
import shapeless.tag.@@
import spinoco.protocol.kafka.TopicName

import scala.sys.process.Process


object Fs2KafkaRuntimeSpec {
  val ZookeeperImage = "jplock/zookeeper:3.4.8"
  val DefaultZkPort:Int = 2181

  val Kafka8Image = "wurstmeister/kafka:0.8.2.0"
  val Kafka9Image = "wurstmeister/kafka:0.9.0.1"
  val Kafka10Image = "wurstmeister/kafka:0.10.0.0"
}

/**
  * Specification that will start kafka runtime before tests are performed.
  * Note that data are contained withing docker images, so once the image stops, the data needs to be recreated.
  */
class Fs2KafkaRuntimeSpec extends Fs2KafkaClientSpec {
  import DockerSupport._
  import Fs2KafkaRuntimeSpec._

  lazy val thisLocalHost:InetAddress = {
    val addr = InetAddress.getLocalHost
    if (addr == null) throw new Exception("Localhost cannot be identified")
    addr
  }

  /**
    * Starts zookeeper listening on given port. ZK runs on host network.
    * @return
    */
  def startZk(port:Int = DefaultZkPort):Task[String @@ DockerId] = {
    for {
      _ <- dockerVersion.flatMap(_.fold[Task[String]](Task.fail(new Throwable("Docker is not available")))(Task.now))
      _ <- installImageWhenNeeded(ZookeeperImage)
      runId <- runImage(ZookeeperImage,None)(
        "--restart=no"
        , s"-p $port:$port/tcp"
      )
    } yield runId
  }


  /** stops and cleans the given image **/
  def stopImage(zkImageId: String @@ DockerId):Task[Unit] = {
    killImage(zkImageId).flatMap(_ => cleanImage(zkImageId))
  }

  /** starts kafka. Kafka runs in host network **/
  def startKafka(image:String, port:Int, zkPort:Int = DefaultZkPort):Task[String @@ DockerId] = {
    for {
      _ <- dockerVersion.flatMap(_.fold[Task[String]](Task.fail(new Throwable("Docker is not available")))(Task.now))
      _ <- installImageWhenNeeded(image)
      runId <- runImage(image,None)(
        "--restart=no"
        , s"""--env KAFKA_PORT=$port """
        , s"""--env KAFKA_ADVERTISED_HOST_NAME=${thisLocalHost.getHostAddress} """
        , s"""--env KAFKA_ADVERTISED_PORT=$port """
        , s"""--env KAFKA_ZOOKEEPER_CONNECT=${thisLocalHost.getHostAddress}:$zkPort """
        , s"-p $port:$port/tcp"
      )
    } yield runId
  }


  /** creates supplied kafka topic with number of partitions, starting at index 0 **/
  def createKafkaTopic (
   kafkaDockerId: String @@ DockerId
   , name: String @@ TopicName
   , partitionCount: Int = 1
   , replicas: Int = 1
 ):Task[Unit] = Task.delay {
    Process("docker", Seq(
      "exec", "-i"
      , kafkaDockerId
      , "bash", "-c", s"$$KAFKA_HOME/bin/kafka-topics.sh --zookeeper ${thisLocalHost.getHostAddress} --create --topic $name --partitions $partitionCount --replication-factor $replicas"
    )).!!
    ()
  }



}
