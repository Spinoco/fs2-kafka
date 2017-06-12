package spinoco.fs2.kafka

import java.net.InetAddress

import fs2._
import fs2.util.syntax._
import org.scalatest.{Args, Status}
import scodec.bits.ByteVector
import shapeless.tag
import shapeless.tag.@@
import spinoco.fs2.kafka.state.BrokerAddress
import spinoco.protocol.kafka.{Broker, PartitionId, ProtocolVersion, TopicName}

import scala.sys.process.Process
import scala.concurrent.duration._


object Fs2KafkaRuntimeSpec {
  val ZookeeperImage = "jplock/zookeeper:3.4.8"
  val DefaultZkPort:Int = 2181

  val Kafka8Image =  "wurstmeister/kafka:0.8.2.0"
  val Kafka9Image =  "wurstmeister/kafka:0.9.0.1"
  val Kafka10Image = "wurstmeister/kafka:0.10.0.0"
  val Kafka101Image = "wurstmeister/kafka:0.10.1.0"
  val Kafka102Image = "wurstmeister/kafka:0.10.2.0"
}

object KafkaRuntimeRelease extends Enumeration {
  val V_8_2_0 = Value
  val V_0_9_0_1 = Value
  val V_0_10_0 = Value
  val V_0_10_1 = Value
  val V_0_10_2 = Value
}


/**
  * Specification that will start kafka runtime before tests are performed.
  * Note that data are contained withing docker images, so once the image stops, the data needs to be recreated.
  */
class Fs2KafkaRuntimeSpec extends Fs2KafkaClientSpec {
  import DockerSupport._
  import Fs2KafkaRuntimeSpec._

  val runtime: KafkaRuntimeRelease.Value = Option(System.getenv().get("KAFKA_TEST_RUNTIME")).map(KafkaRuntimeRelease.withName).getOrElse(KafkaRuntimeRelease.V_0_10_2)
  val protocol: ProtocolVersion.Value = Option(System.getenv().get("KAFKA_TEST_PROTOCOL")).map(ProtocolVersion.withName).getOrElse(ProtocolVersion.Kafka_0_10_2)

  def skipFor(versions: (KafkaRuntimeRelease.Value, ProtocolVersion.Value)*)(test: => Any): Any = {
    if (! versions.contains((runtime, protocol))) test
  }

  lazy val thisLocalHost: InetAddress = {
    val addr = InetAddress.getLocalHost
    if (addr == null) throw new Exception("Localhost cannot be identified")
    addr
  }

  val testTopicA = topic("test-topic-A")
  val part0 = partition(0)

  val localBroker1_9092 = BrokerAddress(thisLocalHost.getHostAddress, 9092)
  val localBroker2_9192 = BrokerAddress(thisLocalHost.getHostAddress, 9192)
  val localBroker3_9292 = BrokerAddress(thisLocalHost.getHostAddress, 9292)

  implicit lazy val logger: Logger[Task] = new Logger[Task] {
    def log(level: Logger.Level.Value, msg: => String, throwable: Throwable): Task[Unit] =
      Task.delay { println(s"$level: $msg"); if (throwable != null) throwable.printStackTrace() }
  }



  /**
    * Starts zookeeper listening on given port. ZK runs on host network.
    * @return
    */
  def startZk(port:Int = DefaultZkPort):Task[String @@ DockerId] = {
    for {
      _ <- Task.delay { println(s"DOCKER INIT ZK @$port") }
      _ <- dockerVersion.flatMap(_.fold[Task[String]](Task.fail(new Throwable("Docker is not available")))(Task.now))
      _ <- availableImages map { avail => println(s"DOCKER HAS CURRENT $avail IMAGES RUNNING") }
      _ <- installImageWhenNeeded(ZookeeperImage)
      _ <- Task.delay { println(s"STARTING ZK @$port") }
      runId <- runImage(ZookeeperImage,None)(
        "--restart=no"
        , "--net=fs2-kafka-network"
        , "--name=zookeeper"
        , s"-p $port:$port/tcp"
      )
    } yield runId
  }


  /** stops and cleans the given image **/
  def stopImage(zkImageId: String @@ DockerId):Task[Unit] = {
    runningImages flatMap { allRunning =>
      if (allRunning.exists(zkImageId.startsWith)) killImage(zkImageId) >> cleanImage(zkImageId)
      else availableImages flatMap { allAvailable =>
        if (allAvailable.exists(zkImageId.startsWith)) cleanImage(zkImageId)
        else Task.now(())
      }
    }

  }

  /** starts kafka. Kafka runs in host network **/
  def startKafka(image: String, port: Int, zkPort: Int = DefaultZkPort, brokerId: Int = 1): Task[String @@ DockerId] = {
    for {
      _ <- dockerVersion.flatMap(_.fold[Task[String]](Task.fail(new Throwable("Docker is not available")))(Task.now))
      _ <- installImageWhenNeeded(image)
      params = Seq(
        "--restart=no"
        , "--net=fs2-kafka-network"
        , s"--name=broker$brokerId"
        , s"""--env KAFKA_PORT=$port"""
        , s"""--env KAFKA_BROKER_ID=$brokerId"""
        , s"""--env KAFKA_ADVERTISED_HOST_NAME=broker$brokerId"""
        , s"""--env KAFKA_ADVERTISED_PORT=$port"""
        , s"""--env KAFKA_ZOOKEEPER_CONNECT=zookeeper:$zkPort"""
        , s"-p $port:$port/tcp"

      )
      - <- Task.delay { println(s"STARTING BROKER[$brokerId] @$port") }
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
      , "bash", "-c", s"$$KAFKA_HOME/bin/kafka-topics.sh --zookeeper zookeeper --create --topic $name --partitions $partitionCount --replication-factor $replicas"
    )).!!
    ()
  }


  /** process emitting once docker id of zk and kafka in singleton (one node) **/
  def withKafkaSingleton[A](version: KafkaRuntimeRelease.Value)(f: (String @@ DockerId, String @@ DockerId) => Stream[Task, A]):Stream[Task,A] = {
    Stream.eval(createNetwork("fs2-kafka-network")) >>
    Stream.eval(startZk()).flatMap { zkId =>
    awaitZKStarted(zkId) ++ time.sleep_(2.seconds) ++
    Stream.eval(startK(version, 1)).flatMap { kafkaId =>
      (awaitKStarted(version, kafkaId) ++ f(zkId, kafkaId))
      .onFinalize {
        stopImage(kafkaId) >>
        stopImage(zkId) >>
        removeNetwork("fs2-kafka-network")
      }
    }}

  }

  def withKafkaClient[A](version: KafkaRuntimeRelease.Value, protocol: ProtocolVersion.Value)(f: KafkaClient[Task] => Stream[Task, A]): Stream[Task, A] = {
    withKafkaSingleton(version) { (_, kafkaDockerId) =>
      time.sleep(1.second) >>
      Stream.eval(createKafkaTopic(kafkaDockerId, testTopicA)) >>
      KafkaClient(Set(localBroker1_9092), protocol, "test-client") flatMap { kc =>
        awaitLeaderAvailable(kc, testTopicA, part0).drain ++ f(kc)
      }
    }
  }

  def startK(version: KafkaRuntimeRelease.Value, brokerId: Int):Task[String @@ DockerId] = {
    val port = 9092+ 100*(brokerId -1)
    version match {
      case KafkaRuntimeRelease.V_8_2_0 => startKafka(Kafka8Image, port = port, brokerId = brokerId)
      case KafkaRuntimeRelease.V_0_9_0_1 => startKafka(Kafka9Image, port = port, brokerId = brokerId)
      case KafkaRuntimeRelease.V_0_10_0 => startKafka(Kafka10Image, port = port, brokerId = brokerId)
      case KafkaRuntimeRelease.V_0_10_1 => startKafka(Kafka101Image, port = port, brokerId = brokerId)
      case KafkaRuntimeRelease.V_0_10_2 => startKafka(Kafka102Image, port = port, brokerId = brokerId)
    }
  }

  def awaitZKStarted(zkId: String @@ DockerId):Stream[Task,Nothing] = {
    followImageLog(zkId).takeWhile(! _.contains("binding to port")).drain ++
    Stream.eval_(Task.delay(println(s"Zookeeper started at $zkId")))
  }

  def awaitKStarted(version: KafkaRuntimeRelease.Value, kafkaId: String @@ DockerId): Stream[Task, Nothing] = {
    val output = Stream.eval_(Task.delay(println(s"Broker $version started at $kafkaId")))
    version match {
      case KafkaRuntimeRelease.V_8_2_0 =>
        followImageLog(kafkaId).takeWhile(! _.contains("New leader is ")).drain ++ output

      case KafkaRuntimeRelease.V_0_9_0_1 =>
        followImageLog(kafkaId).takeWhile(! _.contains("New leader is ")).drain ++ output

      case KafkaRuntimeRelease.V_0_10_0 =>
        followImageLog(kafkaId).takeWhile(! _.contains("New leader is ")).drain ++ output

      case KafkaRuntimeRelease.V_0_10_1 =>
        followImageLog(kafkaId).takeWhile(! _.contains("New leader is ")).drain ++ output

      case KafkaRuntimeRelease.V_0_10_2 =>
        followImageLog(kafkaId).takeWhile(! _.contains("New leader is ")).drain ++ output
    }
  }

  def awaitKFollowerReady(version: KafkaRuntimeRelease.Value, kafkaId: String @@ DockerId, brokerId: Int): Stream[Task, Nothing] = {
    val output = Stream.eval_(Task.delay(println(s"Broker $brokerId (follower) $version started at $kafkaId")))
    version match {
      case KafkaRuntimeRelease.V_8_2_0 =>
        followImageLog(kafkaId).takeWhile(! _.contains(s"[Kafka Server $brokerId], started")).drain ++ output

      case KafkaRuntimeRelease.V_0_9_0_1 =>
        followImageLog(kafkaId).takeWhile(! _.contains(s"[Kafka Server $brokerId], started")).drain ++ output

      case KafkaRuntimeRelease.V_0_10_0 =>
        followImageLog(kafkaId).takeWhile(! _.contains(s"[Kafka Server $brokerId], started")).drain ++ output

      case KafkaRuntimeRelease.V_0_10_1 =>
        followImageLog(kafkaId).takeWhile(! _.contains(s"[Kafka Server $brokerId], started")).drain ++ output

      case KafkaRuntimeRelease.V_0_10_2 =>
        followImageLog(kafkaId).takeWhile(! _.contains(s"[Kafka Server $brokerId], started")).drain ++ output
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
    Stream.eval_(createNetwork("fs2-kafka-network")) ++
    Stream.bracket(startZk())(
      zkId => {
        awaitZKStarted(zkId) ++ time.sleep_(2.seconds) ++ Stream.bracket(startK(version, 1))(
          broker1 => awaitKStarted(version, broker1) ++ time.sleep_(2.seconds) ++ Stream.bracket(startK(version, 2))(
            broker2 => awaitKFollowerReady(version, broker2, 2) ++ time.sleep_(2.seconds) ++ Stream.bracket(startK(version, 3))(
              broker3 => awaitKFollowerReady(version, broker3, 3) ++ time.sleep_(2.seconds) ++ Stream.emit(KafkaNodes(zkId, Map(tag[Broker](1) -> broker1, tag[Broker](2) -> broker2, tag[Broker](3) -> broker3)))
              , stopImage
            )
            , stopImage
          )
          , stopImage
        )
      }
      , stopImage
    )
    .onFinalize(removeNetwork("fs2-kafka-network"))
  }


  def publishNMessages(client: KafkaClient[Task],from: Int, to: Int, quorum: Boolean = false): Task[Unit] = {

    Stream.range(from, to).evalMap { idx =>
      client.publish1(testTopicA, part0, ByteVector(1),  ByteVector(idx), quorum, 10.seconds)
    }
    .run

  }

  def generateTopicMessages(from: Int, to: Int, tail: Long): Vector[TopicMessage] = {
    ((from until to) map { idx =>
      TopicMessage(offset(idx.toLong), ByteVector(1), ByteVector(idx), offset(tail) )
    }) toVector
  }


  def killLeader(client: KafkaClient[Task], nodes: KafkaNodes, topic: String @@ TopicName, partition: Int @@ PartitionId): Stream[Task, Nothing] = {
    client.leaders.discrete.take(1) map { _((topic, partition)) } flatMap { leader =>
      println(s"KILLING LEADER: $leader")
      leader match {
        case BrokerAddress(_, 9092) => Stream.eval_(killImage(nodes.nodes(tag[Broker](1))))
        case BrokerAddress(_, 9192) => Stream.eval_(killImage(nodes.nodes(tag[Broker](2))))
        case BrokerAddress(_, 9292) => Stream.eval_(killImage(nodes.nodes(tag[Broker](3))))
        case other => Stream.fail(new Throwable(s"Unexpected broker: $other"))
      }
    }
  }



  def awaitLeaderAvailable(client: KafkaClient[Task], topic: String @@ TopicName, partition: Int @@ PartitionId): Stream[Task, BrokerAddress] = {
    val leaderReady =  client.leaders.map { _.get((topic, partition)) }
    leaderReady.discrete .map { x => println(s"LEADER for test: $x") ; x }.unNone.map { x => println(s"TOPOLOGY: $x"); x}.take(1) mergeDrainR
    ((time.awakeEvery(1.second) evalMap { _ => println(s"Refreshing topology (SPEC): $topic $partition"); client.refreshTopology }) interruptWhen leaderReady.map { _.nonEmpty })
  }

  def awaitNewLeaderAvailable(client: KafkaClient[Task], topic: String @@ TopicName, partition: Int @@ PartitionId, previous: BrokerAddress): Stream[Task, BrokerAddress] = {
    val leaderReady =  client.leaders.map { _.get((topic, partition)) }
    leaderReady.discrete.map { x => println(s"CLUSTER LEADER for test: $x") ; x }. filter(! _.contains(previous)).unNone.take(1) map { x => println(s"NEW LEADER AVAILABLE: $x"); x} mergeDrainR
    ((time.awakeEvery(1.second) evalMap { _ => println(s"Refreshing topology (SPEC): $topic $partition"); client.refreshTopology }) interruptWhen leaderReady.map { _.filterNot(_ == previous).nonEmpty })
  }

   override def runTest(testName: String, args: Args): Status = {
     println(s"Starting: $testName")
     try super.runTest(testName, args)
     finally println(s"Stopping: $testName")
   }
}
