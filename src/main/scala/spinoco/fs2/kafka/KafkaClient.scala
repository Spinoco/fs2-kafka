package spinoco.fs2.kafka

import java.nio.channels.AsynchronousChannelGroup
import java.time.LocalDateTime
import java.util.Date

import fs2._
import fs2.util.{Async, Attempt, Effect}
import fs2.util.syntax._
import fs2.async.immutable.Signal
import fs2.async.mutable
import scodec.bits.ByteVector
import shapeless.{Typeable, tag}
import shapeless.tag._
import spinoco.fs2.kafka.KafkaClient.impl.PartitionPublishConnection
import spinoco.fs2.kafka.failure._
import spinoco.fs2.kafka.network.BrokerConnection
import spinoco.fs2.kafka.state._
import spinoco.protocol.kafka.Request._
import spinoco.protocol.kafka.{ProtocolVersion, Request, _}
import spinoco.protocol.kafka.Response._

import scala.annotation.tailrec
import scala.concurrent.duration._
/**
  * Client that binds to kafka broker. Usually application need only one client.
  *
  * Client lives until the emitted process is interrupted, or fails.
  *
  */
sealed trait KafkaClient[F[_]] {


  /**
    * Subscribes to specified topic to receive messages published to that topic.
    *
    * Essentially this acts sort of unix `tail` command.
    *
    *
    * Note that user can fine-tune reads from topic by specifying `minChunkByteSize`, `maxChunkByteSize` and `maxWaitTime` parameters
    * to optimize chunking and flow control of reads from Kafka. Default values provide polling each 1 minute whenever at least one message is available.
    *
    * User can by fine-tuning the maxWaitTime and `leaderFailureMaxAttempts` recovery in case of leadership changes in kafka cluster.
    *
    * For example, when leader fails, the stream will stop for about `leaderFailureTimeout` and then tries to continue where the last fetch ended.
    * However wehn there are leaderFailureMaxAttempts successive failures, then the stream will fail.
    *
    * Setting `leaderFailureTimeout` to 0 and `leaderFailureMaxAttempts` to 0 will cause resulting stream to fail immediatelly when any failure occurs.
    *
    *
    * @param topicId          Name of the topic to subscribe to
    * @param partition        Partition to subscribe to
    * @param offset           Offset of the topic to start to read from. First received message may have offset larger
    *                         than supplied offset only if the oldest message has offset higher than supplied offset.
    *                         Otherwise this will always return first message with this offset. -1 specified start from tail (new message arriving to topic)
    * @param prefetch         When true, the implementation will prefetch next chunk of messages from kafka while processing last chunk of messages.
    * @param minChunkByteSize Min size of bytes to read from kafka in single attempt. That number of bytes must be available, in order for read to succeed.
    * @param maxChunkByteSize Max number of bytes to include in reply. Should be always > than max siz of single message including key.
    * @param maxWaitTime      Maximum time to wait before reply, even when `minChunkByteSize` is not satisfied.
    * @param leaderFailureTimeout When fetch from Kafka leader fails, this will try to recover connection every this period up to `leaderFailureMaxAttempts` attempt count is exhausted
    * @param leaderFailureMaxAttempts  Maximum attempts to recover from leader failure, then this will fail.
    * @return
    */
  def subscribe(
    topicId: String @@ TopicName
    , partition: Int @@ PartitionId
    , offset: Long @@ Offset
    , prefetch: Boolean = true
    , minChunkByteSize: Int = 1
    , maxChunkByteSize: Int = 1024 * 1024
    , maxWaitTime: FiniteDuration = 1.minute
    , leaderFailureTimeout: FiniteDuration = 5.seconds
    , leaderFailureMaxAttempts: Int = 3
  ): Stream[F, TopicMessage]


  /**
    * Queries offsets for given topic and partition.
    * Returns offset of first message kept (head) and offset of next message that will arrive to topic.
    * When numbers are equal, then the topic does not include any messages at all.
    *
    * @param topicId      Id of the topic
    * @param partition    Id of the partition
    * @return
    */
  def offsetRangeFor(
     topicId: String @@ TopicName
     , partition: Int @@ PartitionId
   ): F[(Long @@ Offset, Long @@ Offset)]

  /**
    * Publishes single message to the supplied topic.
    * Returns None, if the message was not published due topic/partition not existent or
    * Some(offset) of published message.
    *
    * When `F` finishes its evaluation, message is guaranteed to be seen by the ensemble.
    *
    * @param topicId            Topic to publish to
    * @param partition          Partition to publish to
    * @param key                Key of the message
    * @param message            Message itself
    * @param requireQuorum      If true, this requires quorum of ISR to commit message before leader will reply.
    *                           If false, only leader is required to confirm this publish request.
    * @param serverAckTimeout   Timeout server waits for replicas to ack the request. If the publish request won't be acked by
    *                           server in this time, then the request fails to be published.
    * @return
    */
  def publish1(
  topicId           : String @@ TopicName
  , partition       : Int @@ PartitionId
  , key             : ByteVector
  , message         : ByteVector
  , requireQuorum   : Boolean
  , serverAckTimeout: FiniteDuration
  ): F[Long] = publishN(topicId, partition, requireQuorum, serverAckTimeout, None)(Chunk.singleton((key, message)))

  /**
    * Like `publish` except this won't wait for the confirmation that message was published (fire'n forget).
    *
    * Note that this does not guarantee that message was even sent to server. It will get queued and will
    * be delivered to server within earliest opportunity (once server will be ready to accept it).
    *
    */
  def publishUnsafe1(
    topicId: String @@ TopicName
    , partition: Int @@ PartitionId
    , key: ByteVector
    , message: ByteVector
  ): F[Unit] = publishUnsafeN(topicId, partition, None)(Chunk.singleton((key, message)))

  /**
    * Publishes Chunk of messages to the ensemble. The messages are published as a whole batch, so when this
    * terminates, all messages are guaranteed to be processed by kafka server.
    *
    * Returns offset of very first message published.
    *
    * @param topicId            Id of the topic to publish to.
    * @param partition          Partition to publish to.
    * @param compress           When defined, messages will be compressed by supplied algorithm.
    * @param serverAckTimeout   Defines how long to wait for server to confirm that messages were published.
    *                           Note that this will fail the resulting task if timeout is exceeded, but that won't guarantee that
    *                           messages were not published.
    * @param messages           Chunk of messages to publish. First is id of the topic, second is partition, then key and message itself.
    *                           Additionally `A` may be passed to pair the offset of the message in resulting chunk.
    * @param requireQuorum      If true, this requires quorum of ISR to commit message before leader will reply.
    *                           If false, only leader is required to confirm this publish request.
    *
    * @return
    */
  def publishN(
    topicId: String @@ TopicName
    , partition: Int @@ PartitionId
    , requireQuorum: Boolean
    , serverAckTimeout: FiniteDuration
    , compress: Option[Compression.Value]
  )(messages: Chunk[(ByteVector, ByteVector)]): F[Long]

  /**
    * Like `publishN` except this won't await for messages to be confirmed to be published successfully.
    *
    * Note that this does not guarantee that message was even sent to server. It will get queued and will
    * be delivered to server within earliest opportunity (once server will be ready to accept it).
    *
    */
  def publishUnsafeN(
    topic: String @@ TopicName
    , partition: Int @@ PartitionId
    , compress: Option[Compression.Value]
  )(messages: Chunk[(ByteVector, ByteVector)]): F[Unit]

  /**
    * Provides signal of topics and partitions available to this client.
    * Normally, this signals updates at beginning when client starts, and then there is change in topology detected
    * (i.e. broker becoming unavailable, leader changes).
    *
    * Additionally whenever you call `refreshTopology` then the client refreshes the topology which in result will turn
    * in this signal being updated.
    *
    * @return
    */
  def topics: Signal[F, Map[String @@ TopicName, Set[Int @@ PartitionId]]]

  /**
    * Provides signal of all leaders for all topics and partitions.
    * If the topic and partition is not in this map, it indicates either that topic and partition does not exists
    * or that leader is not known.
    * @return
    */
  def leaders: Signal[F, Map[(String @@ TopicName, Int @@ PartitionId), BrokerAddress]]

  /**
    * When this is evaluated, then brokers are queried for their topology view and topology is updated.
    *
    * @return
    */
  def refreshTopology: F[Unit]
}


object KafkaClient {

  /**
    *
    * @param ensemble                 Ensemble to connect to.  Must not be empty.
    * @param protocol                 Protocol that will be used for requests. This shall be lowest common protocol supported by all brokers.
    * @param clientName               Name of the client. Name is suffixed for different type of connections to broker:
    *                                   - initial-meta-rq : Initial connection to query all available brokers
    *                                   - control : Control connection where publish requests and metadata requests are sent to
    *                                   - fetch: Connection where fetch requests are sent to.
    * @param brokerWriteTimeout       Timeout to complete single write (tcp) operation to broker before failing it.
    * @param queryOffsetTimeout       Timeout to query any partition offset.
    * @param brokerReadMaxChunkSize   Max size of chunk that is read in single tcp operation from broker
    * @param brokerRetryDelay         Delay between broker reconnects if the connection to the broker failed.
    * @param metadataRefreshDelay     Delay of refreshing the metadata.
    * @param brokerControlQueueBound  Max number of unprocessed messages to keep for broker, before stopping accepting new messages for broker.
    *
    * @see [[spinoco.fs2.kafka.client]]
    */
  def apply[F[_]](
    ensemble: Set[BrokerAddress]
    , protocol: ProtocolVersion.Value
    , clientName: String
    , getNow: => LocalDateTime = LocalDateTime.now()
    , brokerWriteTimeout: Option[FiniteDuration] = Some(10.seconds)
    , queryOffsetTimeout: FiniteDuration = 10.seconds
    , brokerReadMaxChunkSize: Int = 256 * 1024
    , brokerRetryDelay: FiniteDuration = 3.seconds
    , metadataRefreshDelay: FiniteDuration = 5.seconds
    , brokerControlQueueBound: Int = 10 * 1000
  )(implicit AG: AsynchronousChannelGroup, F: Async[F], S: Scheduler, Logger: Logger[F]): Stream[F,KafkaClient[F]] = {


    def brokerConnection(addr: BrokerAddress):Pipe[F,RequestMessage,ResponseMessage] = s =>
      Stream.eval(addr.toInetSocketAddress).flatMap { inetSocketAddress =>
        s through BrokerConnection(inetSocketAddress, brokerWriteTimeout, brokerReadMaxChunkSize)
      }

    val fetchMeta = impl.requestReplyBroker[F, Request.MetadataRequest, Response.MetadataResponse](brokerConnection, protocol, s"$clientName-meta-rq") _

    def publishConnection(signal: mutable.Signal[F, ClientState])(topicId: String @@ TopicName, partitionId: Int @@ PartitionId): F[PartitionPublishConnection[F]] = {
      impl.publishLeaderConnection(
        connection = brokerConnection
        , stateSignal = signal
        , protocol = protocol
        , clientId = s"$clientName-produce"
        , failureDelay = brokerRetryDelay
        , refreshMeta = impl.refreshMetadata(fetchMeta, signal)(_) as (())
        , topicId = topicId
        , partition = partitionId
        , refreshMetaDelay = metadataRefreshDelay
      )
    }


    Stream.bracket(impl.mkClient(
      ensemble = ensemble
      , publishConnection = publishConnection
      , fetchMetadata = fetchMeta
      , fetchConnection = impl.fetchBrokerConnection(brokerConnection, protocol, s"$clientName-fetch")
      , offsetConnection =  impl.offsetConnection(brokerConnection, protocol, s"$clientName-offset")
      , queryOffsetTimeout = queryOffsetTimeout
    ))(
      use = { case (client, _) => Stream.emit(client) }
      , release = { case (_, shutdoen) => shutdoen }
    )
  }



  protected[kafka] object impl {

    sealed trait PartitionPublishConnection[F[_]] {
      def shutdown: F[Unit]
      def publish(data: Vector[Message], timeout: FiniteDuration, acks: RequiredAcks.Value): F[Option[(Long @@ Offset, Option[Date])]]
    }

    sealed trait Publisher[F[_]] {
      def shutdown: F[Unit]
      def publish(topic: String @@ TopicName, partition: Int @@ PartitionId, data: Vector[Message], timeout: FiniteDuration, acks: RequiredAcks.Value): F[Option[(Long @@ Offset, Option[Date])]]
    }


    /**
      * Creates a client and F that cleans up lients resources.
      * @param ensemble       Initial kafka clients to connect to
      * @param fetchMetadata  A function fo fetch metadata from client specified provided address and signal of state.
      * @return
      */
    def mkClient[F[_]](
      ensemble: Set[BrokerAddress]
      , publishConnection: mutable.Signal[F, ClientState] => (String @@ TopicName, Int @@ PartitionId) => F[PartitionPublishConnection[F]]
      , fetchMetadata: (BrokerAddress, MetadataRequest) => F[MetadataResponse]
      , fetchConnection : BrokerAddress => Pipe[F, FetchRequest, (FetchRequest, FetchResponse)]
      , offsetConnection : BrokerAddress => Pipe[F, OffsetsRequest, OffsetResponse]
      , queryOffsetTimeout: FiniteDuration
    )(implicit F: Async[F], L: Logger[F], S: Scheduler): F[(KafkaClient[F], F[Unit])] =  {
      async.signalOf(ClientState.initial) flatMap { stateSignal =>
      refreshFullMetadataFromBrokers(ensemble, fetchMetadata, stateSignal) >>
      mkPublishers(publishConnection(stateSignal)) map { publisher =>

        val refreshMeta = refreshMetadata(fetchMetadata, stateSignal) _
        val queryOffsetRange = impl.queryOffsetRange(stateSignal, offsetConnection, queryOffsetTimeout) _

        def preparePublishMessages(messages: Chunk[(ByteVector, ByteVector)], compress: Option[Compression.Value]) = {
          val singleMessages =  messages.map { case (k, v) => Message.SingleMessage(0, MessageVersion.V0, None, k , v) }
          compress match {
            case None => singleMessages.toVector
            case Some(compression) => Vector(Message.CompressedMessages(0, MessageVersion.V0, compression, None, singleMessages.toVector))
          }
        }

        val NoResponseTimeout = 10.seconds

        val client = new KafkaClient[F] {

          def subscribe(
             topicId: @@[String, TopicName]
             , partition: @@[Int, PartitionId]
             , offset: @@[Long, Offset]
             , prefetch: Boolean
             , minChunkByteSize: Int
             , maxChunkByteSize: Int
             , maxWaitTime: FiniteDuration
             , leaderFailureTimeout: FiniteDuration
             , leaderFailureMaxAttempts: Int
           ): Stream[F, TopicMessage] =
            subscribePartition(topicId, partition, offset, prefetch, minChunkByteSize, maxChunkByteSize, maxWaitTime, stateSignal, fetchConnection, refreshMeta, queryOffsetRange, leaderFailureTimeout, leaderFailureMaxAttempts)

          def offsetRangeFor(
            topicId: @@[String, TopicName]
            , partition: @@[Int, PartitionId]
          ): F[(Long @@ Offset, Long @@ Offset)] =
            queryOffsetRange(topicId, partition)

          def publishN(
            topicId: String @@ TopicName
            , partition: Int @@ PartitionId
            , requireQuorum: Boolean
            , serverAckTimeout: FiniteDuration
            , compress: Option[Compression.Value]
          )(messages: Chunk[(ByteVector, ByteVector)]): F[Long] = {

            val toPublish = preparePublishMessages(messages, compress)
            val requiredAcks = if (requireQuorum) RequiredAcks.Quorum else RequiredAcks.LocalOnly
            publisher.publish(topicId, partition, toPublish, serverAckTimeout, requiredAcks) flatMap {
              case None => F.fail(new Throwable(s"Successfully published to $topicId, $partition, but no result available?"))
              case Some((o, _)) => F.pure(o)
            }
          }

          def publishUnsafeN(
            topicId: @@[String, TopicName]
            , partition: @@[Int, PartitionId]
            , compress: Option[Compression.Value]
          )(messages: Chunk[(ByteVector, ByteVector)]): F[Unit] = {
            val toPublish = preparePublishMessages(messages, compress)
            publisher.publish(topicId, partition, toPublish, NoResponseTimeout, RequiredAcks.NoResponse) as (())
          }

          def topics: Signal[F, Map[String @@ TopicName, Set[Int @@ PartitionId]]] =
            stateSignal.map { _.topics.keys.groupBy(_._1).mapValues(_.map(_._2).toSet) }

          def leaders: Signal[F, Map[(@@[String, TopicName], @@[Int, PartitionId]), BrokerAddress]] =
            stateSignal.map { s => s.topics.flatMap { case (tap, taps) => taps.leader flatMap s.brokers.get map { leader => (tap, leader.address)  } } }

          def refreshTopology: F[Unit] =  refreshFullMetadataFromBrokers(ensemble, fetchMetadata, stateSignal)
        }

        client -> publisher.shutdown
      }}
    }



    /**
      * Requests refresh of metadata from all seed brokers. All seeds are queried initially, and then first response is returned.
      * If the query to one broker fails, that is silently ignored (logged). If all queries fail, this fails with failure of NoBrokerAvailable.
      *
      * Returns updated state signal from received metadata
      *
      * @param seeds      Seed nodes. Must not be empty.
      */
    def refreshFullMetadataFromBrokers[F[_]](
      seeds: Set[BrokerAddress]
      , fetchMetadata: (BrokerAddress, MetadataRequest) => F[MetadataResponse]
      , signal: mutable.Signal[F, ClientState]
    )(implicit F: Async[F], Logger: Logger[F]): F[Unit] = {
        signal.get.map(_.brokers.values.map(_.address).toSet ++ seeds) flatMap { brokers =>
          (concurrent.join(Int.MaxValue)(
            Stream.emits(brokers.toSeq) map { broker =>
              Stream.eval(fetchMetadata(broker, MetadataRequest(Vector.empty))).attempt.flatMap { r => Stream.emits(r.right.toOption.toSeq) }
            }
          ) take 1 runLast) flatMap {
            case Some(meta) => signal.modify(updateClientState(meta)) as (())
            case None => F.fail(NoBrokerAvailable)
          }
        }
    }

    /**
      * Update client state with metadata from the response. Only updates metadata in response,
      * other values and state of brokers that already exists are preserved.
      */
    def updateClientState[F[_]](mr: MetadataResponse)(s: ClientState): ClientState = {
      val newBrokers =
        mr.brokers.map { broker =>
          broker.nodeId ->
            BrokerData(
              brokerId = broker.nodeId
              , address = BrokerAddress(broker.host, broker.port)
            )
        } toMap

      val newTopics = mr.topics.flatMap { tm =>
        tm.partitions.map { pm =>
          (tm.name, pm.id) -> TopicAndPartitionState(tm.name, pm.id, pm.leader, pm.isr)
        }
      } toMap

      ClientState(
        brokers = s.brokers ++ newBrokers.map { case (brokerId, state) =>
          brokerId -> s.brokers.getOrElse(brokerId, state)
        }
        , topics = s.topics ++ newTopics
      )
    }



    /**
      * Performs refresh of the metadata for supplied topic and updates state.
      * Used when metadata for given topic // partition seems to be not up-to date (i.e. leader changed)
      * @param brokerConnection  A function to individual broker for metadata
      * @param stateSignal       State signal to get initial broker list from and then update with state of that broker
      * @param topicId           Id of the topic
      * @return updated client state
      */
    def refreshMetadata[F[_]](
     brokerConnection: (BrokerAddress, MetadataRequest) => F[MetadataResponse]
     , stateSignal: mutable.Signal[F, ClientState]
    )(
      topicId: String @@ TopicName
    )(implicit F: Effect[F]): F[ClientState] = {
      def go(remains: Seq[BrokerData]): F[ClientState] = {
        remains.headOption match {
          case None => F.fail(NoBrokerAvailable)
          case Some(broker) =>
            brokerConnection(broker.address, MetadataRequest(Vector(topicId))).attempt flatMap {
              case Right(resp) => stateSignal.modify(updateClientState(resp)).map(_.now)
              case Left(err) => go(remains.tail)

            }
        }
      }
      stateSignal.get.map { _.brokers.values.toSeq } flatMap go
    }

    /** like refreshMetadata, but continuous with interval. This will also never fail, even when brokers are not available for whatever reason.  **/
    def refreshMetronome[F[_]](
      brokerConnection: (BrokerAddress, MetadataRequest) => F[MetadataResponse]
      , stateSignal: mutable.Signal[F, ClientState]
      , interval: FiniteDuration
    )(
      topicId: String @@ TopicName
    )(implicit F: Async[F], S: Scheduler): Stream[F, Unit] = {
      time.awakeEvery(interval) evalMap { _ =>
        refreshMetadata(brokerConnection, stateSignal)(topicId).attempt as (())
      }
    }


    val consumerBrokerId = tag[Broker](-1)


    /**
      * Augments connection to broker to FetchRequest/FetchResponse pattern.
      *
      * Apart of supplying fetch fith proper details, this echoes original request with every fetch
      *
      * @param brokerConnection  Connection to broker
      * @param version           protocol version
      * @param clientId          Id of client
      * @param address           Address of broker.
      */
    def fetchBrokerConnection[F[_]](
     brokerConnection : BrokerAddress => Pipe[F, RequestMessage, ResponseMessage]
     , version: ProtocolVersion.Value
     , clientId: String
    )(address: BrokerAddress)(implicit F: Async[F]): Pipe[F, FetchRequest, (FetchRequest, FetchResponse)] = { s =>

      Stream.eval(async.signalOf(Map[Int, FetchRequest]())) flatMap { openRequestSignal =>
        (s.zipWithIndex evalMap { case (request, idx) =>
          openRequestSignal.modify(_ + (idx -> request)) as RequestMessage(version, idx, clientId, request)
        } through brokerConnection(address)) evalMap[F, F, (FetchRequest, FetchResponse)] { resp => resp.response match {
          case fetch: FetchResponse =>
            openRequestSignal.get map { _.get(resp.correlationId) } flatMap {
              case Some(req) => openRequestSignal.modify(_ - resp.correlationId) as ((req, fetch))
              case None => F.fail(new Throwable(s"Invalid response to fetch request, request not available: $resp"))
            }
          case _ =>
            F.fail(new Throwable(s"Invalid response to fetch request: $resp"))
        }}
      }
    }


    /**
      * Creates connection that allows to submit offset Requests.
      */
    def offsetConnection[F[_]](
      brokerConnection : BrokerAddress => Pipe[F, RequestMessage, ResponseMessage]
      , version: ProtocolVersion.Value
      , clientId: String
    )(address: BrokerAddress)(implicit F: Async[F]): Pipe[F, OffsetsRequest, OffsetResponse] = { s =>
      (s.zipWithIndex map { case (request, idx) =>
        RequestMessage(version, idx, clientId, request)
      } through brokerConnection(address)) flatMap { resp => resp.response match {
        case offset: OffsetResponse => Stream.emit(offset)
        case _ => Stream.fail(new Throwable(s"Invalid response to offset request: $resp"))
      }}
    }


    /**
      * Subscribes to given partition and topic starting offet supplied.
      * Each subscription creates single connection to isr.
      *
      *
      * @param topicId        Id of the topic
      * @param partition      Partition id
      * @param firstOffset    Offset from where to start (including this one). -1 designated start with very first message published (tail)
      * @param stateSignal    Signal to use to select and update broker.
      * @param refreshMeta    A `F` that refreshes metadata when evaluated. It also updates the state of clients while returning the one updated
      * @param queryOffsetRange Queries range of offset kept for given topic. First is head (oldest message offset) second is tail (offset of the message not yet in topic)
      * @return
      */
    def subscribePartition[F[_]](
      topicId           : String @@ TopicName
      , partition       : Int @@ PartitionId
      , firstOffset     : Long @@ Offset
      , prefetch        : Boolean
      , minChunkByteSize: Int
      , maxChunkByteSize: Int
      , maxWaitTime     : FiniteDuration
      , stateSignal     : mutable.Signal[F, ClientState]
      , brokerConnection: BrokerAddress => Pipe[F, FetchRequest, (FetchRequest, FetchResponse)]
      , refreshMeta     : (String @@ TopicName) => F[ClientState]
      , queryOffsetRange : (String @@ TopicName, Int @@ PartitionId) => F[(Long @@ Offset, Long @@ Offset)]
      , leaderFailureTimeout: FiniteDuration
      , leaderFailureMaxAttempts: Int
    )(implicit F: Async[F], S: Scheduler, Logger: Logger[F]): Stream[F, TopicMessage] = {

      def updateLeader: Stream[F, Option[BrokerData]] =
        Stream.eval(refreshMeta(topicId) map { _.leaderFor(topicId, partition) })

      Stream.eval(F.refOf((firstOffset, 0))) flatMap { startFromRef =>
        def fetchFromBroker(broker: BrokerData): Stream[F, TopicMessage] = {
          def tryRecover(rsn: Throwable): Stream[F, TopicMessage] = {
            Stream.eval(startFromRef.get map { _._2 }) flatMap { failures =>
              if (failures >= leaderFailureMaxAttempts) Stream.fail(rsn)
              else {
                Stream.eval(startFromRef.modify { case (start, failures) => (start, failures + 1) }) >>
                time.sleep(leaderFailureTimeout) >>
                Stream.eval(refreshMeta(topicId) map { _.leaderFor(topicId, partition) }) flatMap {
                  case None => tryRecover(LeaderNotAvailable(topicId, partition))
                  case Some(leader) => fetchFromBroker(leader)
                }

              }
            }
          }

          Stream.eval(async.unboundedQueue[F, FetchRequest]) flatMap { requestQueue =>
            def requestNextChunk: F[Unit] = {
              startFromRef.get map { _._1 } flatMap { startFrom =>
                requestQueue.enqueue1(
                  FetchRequest(consumerBrokerId, maxWaitTime, minChunkByteSize, None, Vector((topicId, Vector((partition, startFrom, maxChunkByteSize)))))
                )
              }
            }

            Stream.eval(requestNextChunk) >>
            ((requestQueue.dequeue through brokerConnection(broker.address)) flatMap { case (request, fetch) =>
              fetch.data.find(_._1 == topicId).flatMap(_._2.find(_.partitionId == partition)) match {
                case None =>
                  Stream.fail(InvalidBrokerResponse(broker.address, "FetchResponse", request, Some(fetch)))

                case Some(result) =>
                  result.error match {
                    case Some(error) =>
                      Stream.fail(BrokerReportedFailure(broker.address, request, error))

                    case None =>
                      val messages = messagesFromResult(result)

                      val updateLastKnown = messages.lastOption.map(m => m.offset) match {
                        case None => Stream.empty // No messages emitted, just go on
                        case Some(lastOffset) => Stream.eval_(startFromRef.modify { _ => (offset(lastOffset + 1), 0) })
                      }
                      updateLastKnown ++ {
                        if (prefetch) Stream.eval_(requestNextChunk) ++ Stream.emits(messages)
                        else Stream.emits(messages) ++ Stream.eval_(requestNextChunk)
                      }
                  }
              }
            }) ++ {
              // in normal situations this append shall never be consulted. But the broker may close connection from its side
              // and in that case we need to start querying from the last unfinished request or eventually continue from the
              // as such we fail there and OnError shall handle failure of early termination from broker
              Stream.fail(new Throwable(s"Leader closed connection early: $broker ($topicId, $partition)"))
            }

          } onError {
            case err: LeaderNotAvailable => tryRecover(err)

            case err: BrokerReportedFailure => err.failure match {
              case ErrorType.OFFSET_OUT_OF_RANGE =>
                Stream.eval(queryOffsetRange(topicId, partition)) flatMap { case (min, max) =>
                Stream.eval(startFromRef.get) flatMap { case (startFrom, _) =>
                  if (startFrom < min) Stream.eval(startFromRef.modify(_ => (min, 0))) >> fetchFromBroker(broker)
                  else if (startFrom > max) Stream.eval(startFromRef.modify(_ => (max, 0))) >> fetchFromBroker(broker)
                  else Stream.fail(new Throwable(s"Offset supplied is in acceptable range, but still not valid: $startFrom ($min, $max)", err))
                }}

              case other => tryRecover(err)
            }

            case other => tryRecover(other)
          }
        }

        Stream.eval(stateSignal.get map { _.leaderFor(topicId, partition) }) flatMap {
          case None =>
            // metadata may be stale, so we need to re-query them here and re-read if the leader appeared
            Stream.eval(refreshMeta(topicId) map { _.leaderFor(topicId, partition) }) flatMap {
              case None => Stream.fail(LeaderNotAvailable(topicId, partition))
              case Some(broker) => fetchFromBroker(broker)
            }

          case Some(broker) => fetchFromBroker(broker)
        }
      }

    }


    /**
      * Because result of fetch can retrieve messages in compressed and nested forms,
      * This decomposes result to simple vector by traversing through the nested message results.
      *
      * @param result  Result from teh fetch
      * @return
      */
    def messagesFromResult(result: Response.PartitionFetchResult): Vector[TopicMessage] = {
      @tailrec
      def go(remains: Vector[Message], acc: Vector[TopicMessage]): Vector[TopicMessage] = {
        remains.headOption match {
          case None => acc
          case Some(message: Message.SingleMessage) =>
            go(remains.tail, acc :+ TopicMessage(offset(message.offset), message.key, message.value, result.highWMOffset))

          case Some(messages: Message.CompressedMessages) =>
            go(messages.messages ++ remains.tail, acc)
        }
      }

      go(result.messages, Vector.empty)
    }




    /**
      * Queries offsets for given topic and partition.
      * Returns offset of first message kept (head) and offset of next message that will arrive to topic.
      * When numbers are equal, then the topic does not include any messages at all.
      * @param topicId              Id of the topic
      * @param partition            Id of the partition
      * @param stateSignal          Signal of the state
      * @param brokerOffsetConnection     A function to create connection to broker to send // receive OffsetRequests
      * @tparam F
      */
    def queryOffsetRange[F[_]](
      stateSignal: mutable.Signal[F, ClientState]
      , brokerOffsetConnection : BrokerAddress => Pipe[F, OffsetsRequest, OffsetResponse]
      , maxTimeForQuery: FiniteDuration
    )(
      topicId: String @@ TopicName
      , partition: Int @@ PartitionId
    )(implicit F: Async[F], S: Scheduler): F[(Long @@ Offset, Long @@ Offset)] = {
      stateSignal.get map { _.leaderFor(topicId, partition) } flatMap {
        case None => F.fail(LeaderNotAvailable(topicId, partition))
        case Some(broker) =>
          val requestOffsetDataMin = OffsetsRequest(consumerBrokerId, Vector((topicId, Vector((partition, new Date(-1), Some(Int.MaxValue))))))
          val requestOffsetDataMax = OffsetsRequest(consumerBrokerId, Vector((topicId, Vector((partition, new Date(-2), Some(Int.MaxValue))))))
          (((Stream(requestOffsetDataMin, requestOffsetDataMax) ++ time.sleep_(maxTimeForQuery)) through brokerOffsetConnection(broker.address)) take(2) runLog) flatMap { responses =>
            val results = responses.flatMap(_.data.filter(_._1 == topicId).flatMap(_._2.find(_.partitionId == partition)))
            results.collectFirst(Function.unlift(_.error)) match {
              case Some(err) => F.fail(BrokerReportedFailure(broker.address, requestOffsetDataMin, err))
              case None =>
                val offsets = results.flatMap { _.offsets } map { o => (o: Long) }
                if (offsets.isEmpty) F.fail(new Throwable(s"Invalid response. No offsets available: $responses, min: $requestOffsetDataMin, max: $requestOffsetDataMax"))
                else F.pure ((offset(offsets.min), offset(offsets.max)))
            }
          }
      }
    }


    /**
      * Request // reply communication to broker. This sends one message `I` and expect one result `O`
      */
    def requestReplyBroker[F[_], I <: Request, O <: Response](
      f: BrokerAddress => Pipe[F, RequestMessage, ResponseMessage]
      , protocol:  ProtocolVersion.Value
      , clientId: String
    )(address: BrokerAddress, input: I)(implicit F: Async[F], T: Typeable[O]): F[O] = {
      F.ref[Attempt[Option[ResponseMessage]]] flatMap { ref =>
       F.start(((Stream.emit(RequestMessage(protocol, 1, clientId, input)) ++ Stream.eval(ref.get).drain) through f(address) take 1).runLast.attempt.flatMap { r => ref.setPure(r) }) >>
        ref.get flatMap {
          case Right(Some(response)) => T.cast(response.response) match {
            case Some(o) => F.pure(o)
            case None => F.fail(InvalidBrokerResponse(address, T.describe, input, Some(response.response)))
          }
          case Right(None) =>  F.fail(InvalidBrokerResponse(address, T.describe, input, None))
          case Left(err) => F.fail(BrokerRequestFailure(address, input, err))
        }
      }
    }


    /**
      * With every leader for each topic and partition active this keeps connection open.
      * Connection is open once the topic and partition will get first produce request to serve.
      * @param connection     Function handling connection to Kafka Broker
      * @param topicId        Id of the topic
      * @param partition      Id of the partition
      * @param stateSignal    Signal of the state
      * @param protocol       Protocol
      * @param clientId       Id of the client
      * @param failureDelay   When leader fails, that much time will be delayed before new broker will be checked and processing of messages will be restarted.
      *                       This prevents from situations, where leader failed and new leader was not yet elected.
      * @param refreshMeta    A F, that causes when run to refresh metadata for supplied topic
      * @param refreshMetaDelay A delay that in case there is no leader refreshes metadata
      */
    def publishLeaderConnection[F[_]](
      connection: BrokerAddress => Pipe[F, RequestMessage, ResponseMessage]
      , stateSignal: Signal[F, ClientState]
      , protocol:  ProtocolVersion.Value
      , clientId: String
      , failureDelay: FiniteDuration
      , refreshMeta: (String @@ TopicName) => F[Unit]
      , topicId: String @@ TopicName
      , partition: Int @@ PartitionId
      , refreshMetaDelay: FiniteDuration
    )(implicit F: Async[F], S: Scheduler, L: Logger[F]) : F[PartitionPublishConnection[F]] = {
      type Response = Option[(Long @@ Offset, Option[Date])]
      async.signalOf[F, Boolean](false) flatMap { termSignal =>
      async.synchronousQueue[F, (ProduceRequest, Attempt[Response] => F[Unit])] flatMap { queue =>
      F.refOf[Map[Int, (ProduceRequest, Attempt[Response] => F[Unit])]](Map.empty) flatMap { ref =>
        def registerMessage(in: (ProduceRequest, Attempt[Response] => F[Unit]), idx: Int): F[RequestMessage] = {
          val (produce, cb) = in

          val msg = RequestMessage(
            version = protocol
            , correlationId = idx
            , clientId = clientId
            , request = produce
          )

          produce.requiredAcks match {
            case RequiredAcks.NoResponse => cb(Right(None)) as msg
            case _ => ref.modify { _ + (idx -> ((produce, cb))) } as msg
          }


        }

        def completeNotProcessed: F[Unit] = {
          ref.modify(_ => Map.empty) map { _.previous.values } flatMap { toCancel =>
            val fail = Left(ClientTerminated)
            toCancel.toSeq.traverse(_._2 (fail)) as (())
          }
        }


        // This runs the connection with partition leader.
        // this await leader for given topic/partition, then establishes connection to that partition // leader
        // and sends publish requests to broker. Each request is then completed once response is received.
        // if response is invalid or contains error the response is completed with failure, including the cases where
        // leader changed.
        //
        // if leader is not available, this will fail all incoming requests with `LeaderNotAvailable` failure.
        // Also in this state we fill periodically try to fetch new partition metadata to eventually see if the leader is available again
        //
        //
        val runner =
          (stateSignal.continuous.map( _.leaderFor(topicId, partition) ) flatMap {

            case None =>
              // leader is not available just quickly terminate the requests, so they may be eventually rescheduled
              // once leader will be available this will get interrupted and we start again
              (queue.dequeue.evalMap { case (_, cb) =>
                cb(Left(LeaderNotAvailable(topicId, partition)))
              } mergeDrainR (time.awakeEvery(refreshMetaDelay) evalMap { _ => refreshMeta(topicId) })  ) interruptWhen stateSignal.map(_.leaderFor(topicId, partition).nonEmpty)


            case Some(leader) =>
              ((queue.dequeue.zipWithIndex evalMap (registerMessage _ tupled)) through connection(leader.address)) flatMap { response =>

                Stream.eval(ref.modify2 { m => (m - response.correlationId, m.get(response.correlationId)) }) flatMap { case (c, found) => found match {
                  case None => Stream.fail(UnexpectedResponse(leader.address, response))
                  case Some((req, cb)) =>
                    response match {
                      case ResponseMessage(_, produceResp: ProduceResponse) =>
                        produceResp.data.find(_._1 == topicId).flatMap(_._2.find(_._1 == partition)) match {
                          case None => Stream.fail(UnexpectedResponse(leader.address, response))
                          case Some((_, result)) => result.error match {
                            case None => Stream.eval_(cb(Right(Some((result.offset, result.time)))))
                            case Some(err) => Stream.eval_(cb(Left(BrokerReportedFailure(leader.address, req, err))))
                          }
                        }

                      case _ => Stream.fail(UnexpectedResponse(leader.address, response))
                    }

                }}
              } onError { failure =>
                L.error(s"Unexpected failure while publishing to $topicId[$partition] at broker $leader", failure)
                Stream.eval_ {
                  // cancel all pending publishes with this error, wait safety interval and then try again
                  ref.modify(_ => Map.empty) map { _.previous.values.toSeq } flatMap { toCancel =>
                    val fail = Left(failure)
                    toCancel.traverse(_._2 (fail))
                  }
                } ++ time.sleep_(failureDelay)
              } onFinalize {
                // this seems that leader has hung up or connection failed.
                // We shall refresh metadata just to make sure leader is still the same and retry to establish connection again
                refreshMeta(topicId) as (())
              }
          } interruptWhen termSignal ) onFinalize completeNotProcessed


        F.start(runner.run) as {

          new PartitionPublishConnection[F] {

            def shutdown: F[Unit] = termSignal.set(true)

            def publish(messages: Vector[Message], timeout: FiniteDuration, acks: RequiredAcks.Value): F[Option[(Long @@ Offset, Option[Date])]] = {
              F.ref[Attempt[Response]] flatMap { cbRef =>
                val request = ProduceRequest(
                  requiredAcks = acks
                  , timeout = timeout
                  , messages = Vector((topicId, Vector((partition, messages))))
                )

                queue.enqueue1((request, cbRef.setPure)) >> cbRef.get flatMap {
                  case Left(err) => F.fail(err)
                  case Right(r) => F.pure(r)
                }

              }

            }
          }
        }
      }}}
    }


    /**
      * Produces a publisher that for every publishes partition-topic will spawn `PartitionPublishConnection`.
      * That connection is handling then all publish requests for given partition.
      * Connections are cached are re-used on next publish.
      *
      * @param createPublisher    Function to create single publish connection to given partition.
      *
      */
    def mkPublishers[F[_]](
      createPublisher: (String @@ TopicName, Int @@ PartitionId) => F[PartitionPublishConnection[F]]
    )(implicit F: Async[F]): F[Publisher[F]] = {
      case class PublisherState(shutdown: Boolean, connections: Map[TopicAndPartition, PartitionPublishConnection[F]])
      Async.refOf(PublisherState(false, Map.empty)) map { stateRef =>

        new Publisher[F] { 

          def shutdown: F[Unit] = {
            stateRef.modify { _.copy(shutdown = true) } flatMap { c =>
              c.previous.connections.values.toSeq.traverse(_.shutdown) as (())
            }
          }

          def publish(topic: String @@ TopicName, partition: Int @@ PartitionId, data: Vector[Message], timeout: FiniteDuration, acks: RequiredAcks.Value): F[Option[(Long @@ Offset, Option[Date])]] = {
            stateRef.get map { _.connections.get((topic, partition)) } flatMap {
              case Some(ppc) =>   ppc.publish(data, timeout, acks)
              case None =>
                // lets create a new connection and try to swap it in
                createPublisher(topic, partition) flatMap { ppc =>
                stateRef.modify { s => if (s.shutdown) s else s.copy(connections = s.connections + ((topic, partition) -> ppc)) } flatMap { c =>
                  if (c.modified) publish(topic, partition, data, timeout, acks)
                  else ppc.shutdown >> publish(topic, partition, data, timeout, acks)
                }}

            }
          }

        }

      }
    }


  }


}





