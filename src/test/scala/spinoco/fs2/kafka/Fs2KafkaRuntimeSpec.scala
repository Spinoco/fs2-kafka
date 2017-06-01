package spinoco.fs2.kafka

import java.net.InetAddress

import fs2.{Stream, Task}
import shapeless.tag
import shapeless.tag.@@
import spinoco.fs2.kafka.state.BrokerAddress
import spinoco.protocol.kafka.{Broker, TopicName}

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

  val testTopicA = topic("test-topic-A")
  val part0 = partition(0)

  val localBroker1_9092 = BrokerAddress("127.0.0.1", 9092)
  val localBroker2_9192 = BrokerAddress("127.0.0.1", 9192)
  val localBroker3_9292 = BrokerAddress("127.0.0.1", 9292)

  implicit lazy val logger: Logger[Task] = new Logger[Task] {
    def log(level: Logger.Level.Value, msg: => String, throwable: Throwable): Task[Unit] =
      Task.delay { println(s"$level: $msg"); if (throwable != null) throwable.printStackTrace() }
  }


  object KafkaRuntimeRelease extends Enumeration {
    val V_8_2_0 = Value
    val V_0_9_0_1 = Value
    val V_0_10_0 = Value
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
        , "--network=test-kafka"
        , "--name=zookeeper"
        , s"-p $port:$port/tcp"
      )
    } yield runId
  }


  /** stops and cleans the given image **/
  def stopImage(zkImageId: String @@ DockerId):Task[Unit] = {
    killImage(zkImageId).flatMap(_ => cleanImage(zkImageId))
  }

  /** starts kafka. Kafka runs in host network **/
  def startKafka(image: String, port: Int, zkPort: Int = DefaultZkPort, brokerId: Int = 1, links: Map[String, String @@ DockerId] = Map.empty): Task[String @@ DockerId] = {
    for {
      _ <- dockerVersion.flatMap(_.fold[Task[String]](Task.fail(new Throwable("Docker is not available")))(Task.now))
      _ <- installImageWhenNeeded(image)
      params = Seq(
        "--restart=no"
        , "--network=test-kafka"
        , s"--name=broker$brokerId"
        , s"""--env KAFKA_PORT=$port"""
        , s"""--env KAFKA_BROKER_ID=$brokerId"""
        , s"""--env KAFKA_ADVERTISED_HOST_NAME=broker$brokerId"""
        , s"""--env KAFKA_ADVERTISED_PORT=$port"""
        , s"""--env KAFKA_ZOOKEEPER_CONNECT=zookeeper:$zkPort"""
        , s"-p $port:$port/tcp"
      )
      runId <- runImage(image,None)(params :_*)
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


  /** process emitting once docker id of zk and kafka in singleton (one node) **/
  def withKafkaSingleton(version: KafkaRuntimeRelease.Value):Stream[Task,(String @@ DockerId, String @@ DockerId)] = {

    Stream.bracket(startZk())(
      zkId => awaitZKStarted(zkId) ++ Stream.bracket(startK(version, 1))(
        kafkaId => awaitKStarted(version, kafkaId) ++ Stream.emit(zkId -> kafkaId)
        , stopImage
      )
      , stopImage
    )

  }

  def startK(version: KafkaRuntimeRelease.Value, brokerId: Int, links: Map[String, String @@ DockerId] = Map.empty):Task[String @@ DockerId] = {
    val port = 9092+ 100*(brokerId -1)
    version match {
      case KafkaRuntimeRelease.V_8_2_0 => startKafka(Kafka8Image, port = port, brokerId = brokerId, links = links)
      case KafkaRuntimeRelease.V_0_9_0_1 => startKafka(Kafka9Image, port = port, brokerId = brokerId, links = links)
      case KafkaRuntimeRelease.V_0_10_0 => startKafka(Kafka10Image, port = port, brokerId = brokerId, links = links)
    }
  }

  def awaitZKStarted(zkId: String @@ DockerId):Stream[Task,Nothing] = {
    followImageLog(zkId).takeWhile(! _.contains("binding to port")).map(println).drain
  }

  def awaitKStarted(version: KafkaRuntimeRelease.Value, kafkaId: String @@ DockerId): Stream[Task, Nothing] = {
    version match {
      case KafkaRuntimeRelease.V_8_2_0 =>
        followImageLog(kafkaId).takeWhile(! _.contains("New leader is "))
          .map(println).drain

      case KafkaRuntimeRelease.V_0_9_0_1 =>
        followImageLog(kafkaId).takeWhile(! _.contains("New leader is "))
          .map(println).drain

      case KafkaRuntimeRelease.V_0_10_0 =>
        followImageLog(kafkaId).takeWhile(! _.contains("New leader is "))
          .map(println).drain
    }
  }

  def awaitKFollowerReady(version: KafkaRuntimeRelease.Value, kafkaId: String @@ DockerId, brokerId: Int): Stream[Task, Nothing] = {
    version match {
      case KafkaRuntimeRelease.V_8_2_0 =>
        followImageLog(kafkaId).takeWhile(! _.contains(s"[Kafka Server $brokerId], started"))
          .map(println).drain

      case KafkaRuntimeRelease.V_0_9_0_1 =>
        followImageLog(kafkaId).takeWhile(! _.contains(s"[Kafka Server $brokerId], started"))
          .map(println).drain

      case KafkaRuntimeRelease.V_0_10_0 =>
        followImageLog(kafkaId).takeWhile(! _.contains(s"[Kafka Server $brokerId], started"))
          .map(println).drain
    }
  }



  case class KafkaNodes(
    zk: String @@ DockerId
    , nodes: Map[Int @@ Broker, String @@ DockerId]
  ) { self =>

    def broker1DockerId : String @@ DockerId = nodes(tag[Broker](1))
    def broker2DockerId : String @@ DockerId = nodes(tag[Broker](2))
    def broker3DockerId : String @@ DockerId = nodes(tag[Broker](3))


  }

  /** start 3 node kafka cluster with zookeeper **/
  def withKafkaCluster(version: KafkaRuntimeRelease.Value): Stream[Task, KafkaNodes] = {

    Stream.eval_(createNetwork("test-kafka")) ++
    Stream.bracket(startZk())(
      zkId => {
        awaitZKStarted(zkId) ++ Stream.bracket(startK(version, 1))(
          broker1 => awaitKStarted(version, broker1) ++ Stream.bracket(startK(version, 2, Map("broker_1" -> broker1)))(
            broker2 => awaitKFollowerReady(version, broker2, 2) ++ Stream.bracket(startK(version, 3, Map("broker_2" -> broker2)))(
              broker3 => awaitKFollowerReady(version, broker3, 3) ++ Stream.emit(KafkaNodes(zkId, Map(tag[Broker](1) -> broker1, tag[Broker](2) -> broker2, tag[Broker](3) -> broker3)))
              , stopImage
            )
            , stopImage
          )
          , stopImage
        )
      }
      , stopImage
    ).onFinalize(removeNetwork("test-kafka"))

  }


}
