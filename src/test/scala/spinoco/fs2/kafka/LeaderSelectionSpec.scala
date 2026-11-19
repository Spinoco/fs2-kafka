package spinoco.fs2.kafka

import java.net.UnknownHostException

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import fs2._
import scodec.bits.ByteVector
import shapeless.tag
import shapeless.tag.@@
import spinoco.fs2.kafka.failure.NoBrokerAvailable
import spinoco.fs2.kafka.network.BrokerAddress
import spinoco.protocol.kafka.Request.{FetchRequest, MetadataRequest}
import spinoco.protocol.kafka.Response.{FetchResponse, MetadataResponse, PartitionFetchResult}
import spinoco.protocol.kafka._

import scala.concurrent.duration._

/**
  * Leader selection against a fake DNS. Host missing in DNS simulates pod that is restarting.
  */
class LeaderSelectionSpec extends Fs2KafkaClientSpec {

  implicit val logger: Logger[IO] = new Logger[IO] {
    def log(level: Logger.Level.Value, msg: => String, throwable: Throwable): IO[Unit] = IO.unit
  }

  val testTopic = topic("test-topic")
  val part0 = partition(0)

  val seedA = BrokerAddress("broker-a", 9092)
  val seedB = BrokerAddress("broker-b", 9092)

  def metaFrom(leader: BrokerAddress, leaderId: Int): MetadataResponse =
    MetadataResponse(
      brokers = Vector(Broker(tag[Broker](leaderId), leader.host, leader.port))
      , topics = Vector(TopicMetadata(None, testTopic, Vector(
        PartitionMetadata(None, part0, Some(tag[Broker](leaderId)), Vector.empty, Vector.empty)
      )))
    )

  // each seed reports itself as the leader so we can see which seed served the request
  def metaFrom(seed: BrokerAddress): MetadataResponse =
    metaFrom(seed, if (seed == seedA) 1 else 2)

  def leaderMap(leader: BrokerAddress): Map[(String @@ TopicName, Int @@ PartitionId), BrokerAddress] =
    Map((testTopic, part0) -> leader)

  def resolve(dns: Ref[IO, Set[String]], address: BrokerAddress): IO[Unit] =
    dns.get flatMap { hosts =>
      if (hosts.contains(address.host)) IO.unit
      else IO.raiseError(new UnknownHostException(address.host))
    }

  def requestMeta(dns: Ref[IO, Set[String]])(address: BrokerAddress, rq: MetadataRequest): IO[MetadataResponse] =
    resolve(dns, address) as metaFrom(address)

  def metaConnection(dns: Ref[IO, Set[String]])(address: BrokerAddress): Pipe[IO, MetadataRequest, MetadataResponse] =
    _.evalMap { _ => resolve(dns, address) as metaFrom(address) }

  def mkDns(hosts: String*): IO[Ref[IO, Set[String]]] = Ref.of[IO, Set[String]](hosts.toSet)

  def run[A](io: IO[A]): A = io.unsafeRunTimed(30.seconds).getOrElse(fail("Timed out"))


  "leaderFor" - {

    "returns leader from first seed" in {
      run(mkDns("broker-a", "broker-b") flatMap { dns =>
        KafkaClient.impl.leaderFor[IO](requestMeta(dns), Seq(seedA, seedB))(testTopic, part0)
      }) shouldBe Some(seedA)
    }

    "skips seed with missing DNS record" in {
      run(mkDns("broker-b") flatMap { dns =>
        KafkaClient.impl.leaderFor[IO](requestMeta(dns), Seq(seedA, seedB))(testTopic, part0)
      }) shouldBe Some(seedB)
    }

    "returns None when no seed resolves" in {
      run(mkDns() flatMap { dns =>
        KafkaClient.impl.leaderFor[IO](requestMeta(dns), Seq(seedA, seedB))(testTopic, part0)
      }) shouldBe None
    }

  }


  "leadersDiscrete" - {

    def leaders(dns: Ref[IO, Set[String]], seed: Seq[BrokerAddress]): Stream[IO, Map[(String @@ TopicName, Int @@ PartitionId), BrokerAddress]] =
      KafkaClient.impl.leadersDiscrete[IO](metaConnection(dns), seed, delay = 50.millis, topics = Vector(testTopic))

    "skips seed with missing DNS record" in {
      run(mkDns("broker-b") flatMap { dns =>
        leaders(dns, Seq(seedA, seedB)).take(1).compile.toVector
      }) shouldBe Vector(leaderMap(seedB))
    }

    "fails over to next seed when serving seed disappears" in {
      run(mkDns("broker-a", "broker-b") flatMap { dns =>
        leaders(dns, Seq(seedA, seedB))
        .evalTap { _ => dns.update(_ - "broker-a") }
        .take(2).compile.toVector
      }) shouldBe Vector(leaderMap(seedA), leaderMap(seedB))
    }

    "fails with NoBrokerAvailable when no seed resolves" in {
      run(mkDns() flatMap { dns =>
        leaders(dns, Seq(seedA, seedB)).compile.drain.attempt
      }) shouldBe Left(NoBrokerAvailable)
    }

    "starts new pass from first seed when the last seed disappears" in {
      run(mkDns("broker-b") flatMap { dns =>
        leaders(dns, Seq(seedA, seedB))
        .evalTap { _ => dns.set(Set("broker-a")) }
        .take(2).compile.toVector
      }) shouldBe Vector(leaderMap(seedB), leaderMap(seedA))
    }

    "fails when the only seed disappears" in {
      run(mkDns("broker-a") flatMap { dns =>
        leaders(dns, Seq(seedA))
        .evalTap { _ => dns.update(_ - "broker-a") }
        .compile.drain.attempt
      }) shouldBe Left(NoBrokerAvailable)
    }

  }


  "subscribePartition" - {

    def fetchResponse(request: FetchRequest, withMessage: Boolean): FetchResponse = {
      val startFrom = request.topics.head._2.head._2
      val messages = {
        if (withMessage) Vector(Message.SingleMessage(startFrom, MessageVersion.V0, None, ByteVector.empty, ByteVector(startFrom.toByte)))
        else Vector.empty
      }
      FetchResponse(Vector((testTopic, Vector(PartitionFetchResult(part0, None, offset(100), messages)))), None)
    }

    def subscribe(
      getLeader: (String @@ TopicName, Int @@ PartitionId) => IO[Option[BrokerAddress]]
      , fetchConnection: (BrokerAddress, FiniteDuration) => Pipe[IO, FetchRequest, (FetchRequest, FetchResponse)]
    ): Stream[IO, TopicMessage] =
      KafkaClient.impl.subscribePartition[IO](
        topicId = testTopic
        , partition = part0
        , firstOffset = offset(0)
        , prefetch = false
        , minChunkByteSize = 1
        , maxChunkByteSize = 1024
        , maxWaitTime = 100.millis
        , protocol = ProtocolVersion.Kafka_0_10
        , fetchConnection = fetchConnection
        , getLeader = getLeader
        , queryOffsetRange = (_, _) => IO.raiseError(new Throwable("Not expected"))
        , leaderFailureTimeout = 10.millis
        , leaderFailureMaxAttempts = 3
      )

    def leaderA(topicId: String @@ TopicName, partition: Int @@ PartitionId): IO[Option[BrokerAddress]] = IO.pure(Some(seedA))

    def fetchWithMessages(address: BrokerAddress, readTimeout: FiniteDuration): Pipe[IO, FetchRequest, (FetchRequest, FetchResponse)] =
      _.map { rq => (rq, fetchResponse(rq, withMessage = true)) }

    "recovers when leader is not known at start" in {
      val result = {
        run(Ref.of[IO, Int](0) flatMap { lookups =>
          def getLeader(topicId: String @@ TopicName, partition: Int @@ PartitionId): IO[Option[BrokerAddress]] = {
            lookups.getAndUpdate(_ + 1) map { n => if (n < 2) None else Some(seedA) }
          }
          subscribe(getLeader, fetchWithMessages).take(3).compile.toVector
        })
      }
      result.map(m => m.offset: Long) shouldBe Vector(0L, 1L, 2L)
    }

    "recovers when leader DNS record is missing temporarily" in {
      val result = {
        run(Ref.of[IO, Int](0) flatMap { connections =>
          def fetch(address: BrokerAddress, readTimeout: FiniteDuration): Pipe[IO, FetchRequest, (FetchRequest, FetchResponse)] = { s =>
            Stream.eval(connections.getAndUpdate(_ + 1)) flatMap { n =>
              if (n < 2) Stream.raiseError[IO](new UnknownHostException(address.host))
              else s through fetchWithMessages(address, readTimeout)
            }
          }
          subscribe(leaderA, fetch).take(3).compile.toVector
        })
      }
      result.map(m => m.offset: Long) shouldBe Vector(0L, 1L, 2L)
    }

    "keeps running on quiet topic with repeated, separated leader restarts" in {
      // every connection serves a few empty fetches and then fails, as if the leader pod restarted
      val (result, connectionCount) = {
        run(Ref.of[IO, Int](0) flatMap { connections =>
          def fetch(address: BrokerAddress, readTimeout: FiniteDuration): Pipe[IO, FetchRequest, (FetchRequest, FetchResponse)] = { s =>
            Stream.exec(connections.update(_ + 1)) ++
            s.take(3).map { rq => (rq, fetchResponse(rq, withMessage = false)) } ++
            Stream.raiseError[IO](new UnknownHostException(address.host))
          }
          subscribe(leaderA, fetch).interruptAfter(1.second).compile.drain.attempt flatMap { r => connections.get.map(r -> _) }
        })
      }
      result shouldBe Right(())
      connectionCount should be > 5
    }

  }

}
