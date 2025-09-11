package spinoco.fs2.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2._
import org.scalatest.{Args, Status}
import scodec.bits.ByteVector
import shapeless.tag
import shapeless.tag.@@
import spinoco.fs2.kafka.network.BrokerAddress
import spinoco.protocol.kafka.{Broker, PartitionId, ProtocolVersion, TopicName}

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try


object Fs2KafkaRuntimeSpec {
  val ZookeeperImage = "zookeeper:3.8.4"
  val DefaultZkPort:Int = 2181

  val Kafka10Image = "wurstmeister/kafka:0.10.0.0"
  val Kafka101Image = "wurstmeister/kafka:0.10.1.0"
  val Kafka102Image = "wurstmeister/kafka:0.10.2.0"
  val Kafka11Image = "wurstmeister/kafka:0.11.0.0"
  val Kafka1101Image = "wurstmeister/kafka:0.11.0.1"
  val Kafka1Image = "wurstmeister/kafka:1.0.0"
}

object KafkaRuntimeRelease extends Enumeration {
  val V_0_10_0 = Value
  val V_0_10_1 = Value
  val V_0_10_2 = Value
  val V_0_11_0 = Value
  val V_0_11_0_1 = Value
  val V_1_0_0 = Value

  def toKafkaVersion(runtime: Value): String = runtime match {
    case V_0_10_0 => "0.10.0.0"
    case V_0_10_1 => "0.10.1.0"
    case V_0_10_2 => "0.10.2.0"
    case V_0_11_0 => "0.11.0.0"
    case V_0_11_0_1 => "0.11.0.1"
    case V_1_0_0 => "1.0.0"
  }
}

/**
 * Trait that provides indexed topic naming for test isolation and inspection.
 * Each test gets a unique topic with format: test-topic-{ClassName}-{index}
 */
trait IndexedTopicSupport {
  private val testIndex = new AtomicInteger(0)
  
  def nextTopicIndex(): String = f"${testIndex.incrementAndGet()}%03d"
  
  def indexedTopic(): String @@ TopicName = {
    val className = this.getClass.getSimpleName
    val index = nextTopicIndex()
    tag[TopicName](s"test-topic-$className-$index")
  }
  
  // Helper method to create topic on running Kafka instance
  def createIndexedTopic(topicName: String @@ TopicName, partitions: Int = 1, replicationFactor: Int = 1): IO[Unit] = {
    import scala.sys.process._
    IO {
      val command = s"docker exec broker1 kafka-topics.sh --create --topic $topicName --partitions $partitions --replication-factor $replicationFactor --zookeeper zookeeper:2181"
      val result = command.!
      if (result != 0) {
        // Topic might already exist, which is fine for our use case
        println(s"Note: Topic creation for $topicName returned exit code: $result (topic may already exist)")
      }
    }
  }
}

/**
  * Specification that will start kafka runtime before tests are performed.
  * Note that data are contained withing docker images, so once the image stops, the data needs to be recreated.
  */
class Fs2KafkaRuntimeSpec extends Fs2KafkaClientSpec with IndexedTopicSupport {
  import DockerSupport._

  val runtime: KafkaRuntimeRelease.Value = Option(System.getenv().get("KAFKA_TEST_RUNTIME")).map(KafkaRuntimeRelease.withName).getOrElse(KafkaRuntimeRelease.V_1_0_0)
  val protocol: ProtocolVersion.Value = Option(System.getenv().get("KAFKA_TEST_PROTOCOL")).map(ProtocolVersion.withName).getOrElse(ProtocolVersion.Kafka_0_10_2)

  
  // Scripts paths
  val scriptDir = System.getProperty("user.dir") + "/scripts"
  val startScript = s"$scriptDir/start-kafka.sh"
  val stopScript = s"$scriptDir/stop-kafka.sh"
  val testScript = s"$scriptDir/test-kafka.sh"

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

  // Static IP addresses matching the Docker network configuration
  val localBroker1_9092 = BrokerAddress("172.30.0.11", 9092)
  val localBroker2_9192 = BrokerAddress("172.30.0.12", 9192) 
  val localBroker3_9292 = BrokerAddress("172.30.0.13", 9292)

  val localCluster = Set(localBroker1_9092, localBroker2_9192, localBroker3_9292)

  implicit lazy val logger: Logger[IO] = new Logger[IO] {
    def log(level: Logger.Level.Value, msg: => String, throwable: Throwable): IO[Unit] =
      IO { println(s"LOGGER: $level: $msg"); if (throwable != null) throwable.printStackTrace() }
  }

  // Helper method to create topic for single broker tests
  def createTopicForSingleBroker(): IO[Unit] = {
    IO {
      println("Creating test topic for single broker...")
      val result = scala.sys.process.Process(Seq(
        "docker", "exec", "broker1", "kafka-topics.sh",
        "--create", "--topic", "test-topic-A", 
        "--partitions", "1", "--replication-factor", "1",
        "--zookeeper", "zookeeper:2181"
      )).!
      if (result != 0) {
        throw new RuntimeException(s"Failed to create test topic (exit code: $result)")
      }
      println("Test topic created successfully")
    }
  }

  // New script-based Kafka management
  def startKafkaUsingScript(mode: String = "single"): IO[Unit] = {
    val kafkaVersion = KafkaRuntimeRelease.toKafkaVersion(runtime)
    val command = s"$startScript $mode $kafkaVersion"
    IO {
      println(s"Starting Kafka: $command")
      val result = scala.sys.process.Process(command).!
      if (result != 0) {
        throw new RuntimeException(s"Failed to start Kafka with command: $command (exit code: $result)")
      }
      println(s"Kafka started successfully (version: $kafkaVersion, mode: $mode)")
    }
  }

  def stopKafkaUsingScript(): IO[Unit] = {
    IO {
      println("Stopping Kafka...")
      val result = scala.sys.process.Process(stopScript).!
      if (result != 0) {
        println(s"Warning: Stop script returned exit code: $result")
      }
      println("Kafka stop script completed")
    }
  }

  def testKafkaConnectivity(): IO[Unit] = {
    IO {
      println("Testing Kafka connectivity...")
      val result = scala.sys.process.Process(testScript).!
      if (result != 0) {
        throw new RuntimeException(s"Kafka connectivity test failed (exit code: $result)")
      }
      println("Kafka connectivity test passed")
    }
  }


  // Helper method for single broker tests
  def withKafkaSingle[A](test: KafkaClient[IO] => Stream[IO, A]): Stream[IO, A] = {
    Stream.eval(startKafkaUsingScript("single") >> testKafkaConnectivity()) >>
    Stream.eval(createTopicForSingleBroker()) >>
    Stream.resource(KafkaClient.client[IO](Set(localBroker1_9092), protocol, "test-client")).flatMap { kc =>
      test(kc)
    }.onFinalize(stopKafkaUsingScript())
  }

  // Helper method for cluster tests  
  def withKafkaCluster[A](test: KafkaClient[IO] => Stream[IO, A]): Stream[IO, A] = {
    Stream.eval(startKafkaUsingScript("cluster") >> testKafkaConnectivity()) >>
    Stream.resource(KafkaClient.client[IO](localCluster, protocol, "test-client")).flatMap { kc =>
      test(kc)
    }.onFinalize(stopKafkaUsingScript())
  }



  /** creates supplied kafka topic with number of partitions, starting at index 0 **/
  def createKafkaTopic (
   kafkaDockerId: String @@ DockerId
   , name: String @@ TopicName
   , partitionCount: Int = 1
   , replicas: Int = 1
 ):IO[Unit] = IO {
    Process("docker", Seq(
      "exec", "-i"
      , kafkaDockerId
      , "bash", "-c", s"$$KAFKA_HOME/bin/kafka-topics.sh --zookeeper zookeeper --create --topic $name --partitions $partitionCount --replication-factor $replicas"
    )).!!
    ()
  }

  def cleanAll: IO[Unit] = IO {
    val images = Process("docker", Seq("ps", "-qa")).lineStream
    Try(Process("docker", Seq("kill") ++ images).!!(ProcessLogger(_ => ())))
    Try(Process("docker", Seq("rm") ++ images).!!(ProcessLogger(_ => ())))
    Try(Process("docker", Seq("network", "rm", "fs2-kafka-network") ++ images).!!(ProcessLogger(_ => ())))
    ()
  }


  def createKafkaTopicScript(topicName: String @@ TopicName): IO[Unit] = {
    import scala.sys.process._
    IO {
      val command = s"docker exec broker1 kafka-topics.sh --create --topic $topicName --partitions 1 --replication-factor 1 --zookeeper zookeeper:2181"
      val result = command.!
      if (result != 0) {
        println(s"Warning: Topic creation returned exit code: $result (topic may already exist)")
      }
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

  // Simplified KafkaNodes for script-based tests
  case class SimpleKafkaNodes() {
    // These methods provide compatibility but use fixed container names from scripts
    def broker1DockerId: String @@ DockerId = tag[DockerId]("broker1")
    def broker2DockerId: String @@ DockerId = tag[DockerId]("broker2") 
    def broker3DockerId: String @@ DockerId = tag[DockerId]("broker3")
  }

  /** start 3 node kafka cluster with zookeeper - now uses scripts **/
  def withKafkaCluster(version: KafkaRuntimeRelease.Value): Stream[IO, KafkaNodes] = {
    // Note: version parameter is now controlled by environment variables, but we keep method signature for compatibility
    Stream.eval(startKafkaUsingScript("cluster") >> testKafkaConnectivity()).as(
      // Create a simple KafkaNodes compatible structure for script-based tests
      KafkaNodes(
        tag[DockerId]("script-zk"), 
        Map(
          tag[Broker](1) -> tag[DockerId]("broker1"),
          tag[Broker](2) -> tag[DockerId]("broker2"), 
          tag[Broker](3) -> tag[DockerId]("broker3")
        )
      )
    ).onFinalize(stopKafkaUsingScript())
  }



  def publishNMessages(client: KafkaClient[IO],from: Int, to: Int, quorum: Boolean = false): IO[Unit] = {

    Stream.range(from, to).evalMap { idx =>
      client.publish1(testTopicA, part0, ByteVector(1),  ByteVector(idx), quorum, 10.seconds)
    }
    .compile.drain

  }

  def generateTopicMessages(from: Int, to: Int, tail: Long): Vector[TopicMessage] = {
    ((from until to) map { idx =>
      TopicMessage(offset(idx.toLong), ByteVector(1), ByteVector(idx), offset(tail) )
    }) toVector
  }


  def killLeader(client: KafkaClient[IO], topic: String @@ TopicName, partition: Int @@ PartitionId): Stream[IO, BrokerAddress] = {
    client.leaderFor(500.millis)(topic).take(1).map(_((topic, partition))).flatMap { leader =>
      val containerName = leader match {
        case BrokerAddress(_, 9092) => "broker1"
        case BrokerAddress(_, 9192) => "broker2" 
        case BrokerAddress(_, 9292) => "broker3"
        case other => throw new RuntimeException(s"Unexpected broker address: $other")
      }
      
      Stream.exec(IO {
        import scala.sys.process._
        println(s"Killing leader broker: $containerName (${leader})")
        val result = s"docker kill $containerName".!
        if (result != 0) {
          throw new RuntimeException(s"Failed to kill broker $containerName (exit code: $result)")
        }
        println(s"Successfully killed leader broker: $containerName")
      }).as(leader)
    }
  }



  def awaitLeaderAvailable(client: KafkaClient[IO], topic: String @@ TopicName, partition: Int @@ PartitionId): Stream[IO, BrokerAddress] = {
    client.leaderFor(500.millis)(topic).map(_.get((topic, partition))).unNone.take(1)
  }

  def awaitNewLeaderAvailable(client: KafkaClient[IO], topic: String @@ TopicName, partition: Int @@ PartitionId, previous: BrokerAddress): Stream[IO, BrokerAddress] = {
    client.leaderFor(500.millis)(topic).map(_.get((topic, partition)).filterNot(_ == previous)).unNone.take(1)
  }

   override def runTest(testName: String, args: Args): Status = {
     println(s"Starting: $testName")
     try super.runTest(testName, args)
     finally println(s"Stopping: $testName")
   }
}

/**
 * Base class for tests that use a single Kafka broker.
 * Assumes Kafka is already running at 172.30.0.11:9092.
 * For local development: start with ./scripts/start-kafka.sh single
 * For CI: Kafka management is handled by CI steps
 */
abstract class Fs2KafkaSingleBrokerSpec extends Fs2KafkaRuntimeSpec {
  
  // Pre-configured client for single broker setup
  lazy val kafkaClient: KafkaClient[IO] = {
    KafkaClient.client[IO](Set(localBroker1_9092), protocol, "test-client")
      .allocated.unsafeRunSync()._1
  }
  
  // Helper to create topic and ensure it exists
  def withIndexedTopic[A](f: (String @@ TopicName) => IO[A]): IO[A] = {
    val topic = indexedTopic()
    createIndexedTopic(topic) >> f(topic)
  }
  
  // Helper for tests that return Stream
  def withIndexedTopicStream[A](f: (String @@ TopicName) => Stream[IO, A]): Stream[IO, A] = {
    val topic = indexedTopic()
    Stream.eval(createIndexedTopic(topic)) >> f(topic)
  }
}

/**
 * Base class for tests that use a Kafka cluster.
 * Assumes Kafka cluster is already running at 172.30.0.11-13.
 * For local development: start with ./scripts/start-kafka.sh cluster
 * For CI: Kafka management is handled by CI steps
 */
abstract class Fs2KafkaClusterSpec extends Fs2KafkaRuntimeSpec {
  
  // Pre-configured client for cluster setup
  lazy val kafkaClient: KafkaClient[IO] = {
    KafkaClient.client[IO](localCluster, protocol, "test-client")
      .allocated.unsafeRunSync()._1
  }
  
  // Helper to create replicated topic and ensure it exists
  def withIndexedTopic[A](replicationFactor: Int = 3)(f: (String @@ TopicName) => IO[A]): IO[A] = {
    val topic = indexedTopic()
    createIndexedTopic(topic, partitions = 1, replicationFactor) >> f(topic)
  }
  
  // Helper for tests that return Stream
  def withIndexedTopicStream[A](replicationFactor: Int = 3)(f: (String @@ TopicName) => Stream[IO, A]): Stream[IO, A] = {
    val topic = indexedTopic()
    Stream.eval(createIndexedTopic(topic, partitions = 1, replicationFactor)) >> f(topic)
  }
}
