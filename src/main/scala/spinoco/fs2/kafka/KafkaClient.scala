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
import spinoco.fs2.kafka.failure._
import spinoco.fs2.kafka.network.BrokerConnection
import spinoco.fs2.kafka.state._
import spinoco.protocol.kafka.Request.{FetchRequest, MetadataRequest, OffsetsRequest, RequiredAcks}
import spinoco.protocol.kafka.{ProtocolVersion, _}
import spinoco.protocol.kafka.Response.{FetchResponse, MetadataResponse, OffsetResponse}

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
    * Emits empty stream, if :
    *
    *  - topic and/or partition does not exist
    *  - offset is larger than maximum offset known to topic/partition and `followTail` is set to false
    *
    *
    *  Note that user can fine-tune reads from topic by specifying `minChunkByteSize`, `maxChunkByteSize` and `maxWaitTime` parameters
    *  to optimize chunking and flow control of reads from Kafka. Default values provide polling each 1 minute whenever at least one message is available.
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
    * @param requiredAcks       Acknowledgements required before message is considered published.
    * @param serverAckTimeout   Timeout server waits for replicas to ack the request. If the publish request won't be acked by
    *                           server in this time, then the request fails to be published.
    * @return
    */
  def publish1(
    topicId: String @@ TopicName
    , partition: Int @@ PartitionId
    , key: ByteVector
    , message: ByteVector
    , requiredAcks: RequiredAcks.Value
    , serverAckTimeout: FiniteDuration
  ): F[Long] = publishN(topicId, partition, requiredAcks, serverAckTimeout, None)(Seq((key, message)))

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
  ): F[Unit] = publishUnsafeN(topicId, partition, None)(Seq((key, message)))

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
    * @param requiredAcks       Acknowledgements required before message is considered published.
    *
    * @return
    */
  def publishN(
    topicId: String @@ TopicName
    , partition: Int @@ PartitionId
    , requiredAcks: RequiredAcks.Value
    , serverAckTimeout: FiniteDuration
    , compress: Option[Compression.Value]
  )(messages: Seq[(ByteVector, ByteVector)]): F[Long]

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
  )(messages:Seq[(ByteVector, ByteVector)]): F[Unit]

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
    , brokerRetryDelay: FiniteDuration = 10.seconds
    , brokerControlQueueBound: Int = 10 * 1000
  )(implicit AG: AsynchronousChannelGroup, F: Async[F], S: Scheduler, Logger: Logger[F]): Stream[F,KafkaClient[F]] = {
    def brokerConnection(addr: BrokerAddress):Pipe[F,RequestMessage,ResponseMessage] = s =>
      Stream.eval(addr.toInetSocketAddress).flatMap { inetSocketAddress =>
        s through BrokerConnection(inetSocketAddress, brokerWriteTimeout, brokerReadMaxChunkSize)
      }


    Stream.eval(impl.mkClient(
      ensemble = ensemble
      , fetchMetadata = impl.requestReplyBroker[F, Request.MetadataRequest, Response.MetadataResponse](brokerConnection, protocol, s"$clientName-meta-rq")
      , fetchConnection = impl.fetchBrokerConnection(brokerConnection, protocol, s"$clientName-fetch")
      , offsetConnection =  impl.offsetConnection(brokerConnection, protocol, s"$clientName-offset")
      , queryOffsetTimeout = queryOffsetTimeout
    )) flatMap { case (client, runner) =>
      Stream.emit(client) mergeDrainR runner
    }
  }



  protected[kafka] object impl {


    /**
      * Creates a client and stream that runs client maintenance tasks
      * @param ensemble       Initial kafka clients to connect to
      * @param fetchMetadata  A function fo fetch metadata from client specified provided address and signal of state.
      * @return
      */
    def mkClient[F[_]](
      ensemble: Set[BrokerAddress]
      , fetchMetadata: (BrokerAddress, MetadataRequest) => F[MetadataResponse]
      , fetchConnection : BrokerAddress => Pipe[F, FetchRequest, (FetchRequest, FetchResponse)]
      , offsetConnection : BrokerAddress => Pipe[F, OffsetsRequest, OffsetResponse]
      , queryOffsetTimeout: FiniteDuration
    )(implicit F: Async[F], L: Logger[F], S: Scheduler): F[(KafkaClient[F], Stream[F, Unit])] =  {
      async.signalOf(ClientState.initial[F]) flatMap { stateSignal =>
      refreshFullMetadataFromBrokers(ensemble, fetchMetadata, stateSignal) as {

        val refreshMeta = refreshMetadata(fetchMetadata, stateSignal) _
        val queryOffsetRange =   impl.queryOffsetRange(stateSignal, offsetConnection, queryOffsetTimeout) _

        val client = new KafkaClient[F] {

          def subscribe(topicId: @@[String, TopicName], partition: @@[Int, PartitionId], offset: @@[Long, Offset], prefetch: Boolean, minChunkByteSize: Int, maxChunkByteSize: Int, maxWaitTime: FiniteDuration): Stream[F, TopicMessage] =
            subscribePartition(topicId, partition, offset, prefetch, minChunkByteSize, maxChunkByteSize, maxWaitTime, stateSignal, fetchConnection, refreshMeta, queryOffsetRange)

          def offsetRangeFor(topicId: @@[String, TopicName], partition: @@[Int, PartitionId]): F[(Long @@ Offset, Long @@ Offset)] =
            queryOffsetRange(topicId, partition)

          def publishN(topicId: @@[String, TopicName], partition: @@[Int, PartitionId], requiredAcks: RequiredAcks.Value,  serverAckTimeout: FiniteDuration, compress: Option[Compression.Value])(messages: Seq[(ByteVector, ByteVector)]): F[Long] = ???

          def publishUnsafeN(topic: @@[String, TopicName], partition: @@[Int, PartitionId], compress: Option[Compression.Value])(messages: Seq[(ByteVector, ByteVector)]): F[Unit] = ???

          def topics: Signal[F, Map[String @@ TopicName, Set[Int @@ PartitionId]]] =
            stateSignal.map { _.topics.keys.groupBy(_._1).mapValues(_.map(_._2).toSet) }

          def refreshTopology: F[Unit] =  refreshFullMetadataFromBrokers(ensemble, fetchMetadata, stateSignal)
        }

        client -> Stream.empty
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
      , signal: mutable.Signal[F, ClientState[F]]
    )(implicit F: Async[F], Logger: Logger[F]): F[Unit] = {
        println(s"XXXY SEEDS $seeds")
        signal.get.map(_.brokers.values.map(_.address).toSet ++ seeds) flatMap { brokers =>
          (concurrent.join(Int.MaxValue)(
            Stream.emits(brokers.toSeq) map { broker =>
              Stream.eval(fetchMetadata(broker, MetadataRequest(Vector.empty))).attempt.flatMap { r => Stream.emits(r.right.toOption.toSeq) }
            }
          ) take 1 runLast) flatMap {
            case Some(meta) => println(s"XXXY METADATA: $meta"); signal.modify(updateClientState(meta)) as (())
            case None => println(s"XXXR NO META"); F.fail(NoBrokerAvailable)
          }
        }
    }

    /**
      * Update client state with metadata from the response. Only updates metadata in response,
      * other values and state of brokers that already exists are preserved.
      */
    def updateClientState[F[_]](mr: MetadataResponse)(s: ClientState[F]): ClientState[F] = {
      val newBrokers =
        mr.brokers.map { broker =>
          broker.nodeId ->
            BrokerState.Ready[F](
              brokerId = broker.nodeId
              , address = BrokerAddress(broker.host, broker.port)
            )
        }.toMap


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
     , stateSignal: mutable.Signal[F, ClientState[F]]
    )(
    topicId: String @@ TopicName
    )(implicit F: Effect[F]): F[ClientState[F]] = {
      def go(remains: Seq[BrokerState[F]]): F[ClientState[F]] = {
        remains.headOption match {
          case None => F.fail(NoBrokerAvailable)
          case Some(broker) =>
            brokerConnection(broker.address, MetadataRequest(Vector(topicId))).attempt flatMap {
              case Left(err) => go(remains.tail)
              case Right(resp) => stateSignal.modify(updateClientState(resp)).map(_.now)
            }
        }
      }
      stateSignal.get.map { _.brokers.values.toSeq } flatMap go
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
      , stateSignal     : mutable.Signal[F, ClientState[F]]
      , brokerConnection: BrokerAddress => Pipe[F, FetchRequest, (FetchRequest, FetchResponse)]
      , refreshMeta     : (String @@ TopicName) => F[ClientState[F]]
      , queryOffsetRange : (String @@ TopicName, Int @@ PartitionId) => F[(Long @@ Offset, Long @@ Offset)]
    )(implicit F: Async[F], Logger: Logger[F]): Stream[F, TopicMessage] = {

      Stream.eval(F.refOf(firstOffset)) flatMap { startFromRef =>
        def fetchFromBroker(broker: BrokerState[F]): Stream[F, TopicMessage] = {
          Stream.eval(async.unboundedQueue[F, FetchRequest]) flatMap { requestQueue =>
            def requestNextChunk: F[Unit] = {
              startFromRef.get flatMap { startFrom =>
                requestQueue.enqueue1(
                  FetchRequest(consumerBrokerId, maxWaitTime, minChunkByteSize, Vector((topicId, Vector((partition, startFrom, maxChunkByteSize)))))
                )
              }
            }

            Stream.eval(requestNextChunk) >>
            (requestQueue.dequeue through brokerConnection(broker.address)) flatMap { case (request, fetch) =>
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
                        case Some(lastOffset) => Stream.eval_(startFromRef.modify(_ => offset(lastOffset + 1)))
                      }
                      updateLastKnown ++ {
                        if (prefetch) Stream.eval_(requestNextChunk) ++ Stream.emits(messages)
                        else Stream.emits(messages) ++ Stream.eval_(requestNextChunk)
                      }
                  }
              }
            }

          } onError {
            case err: LeaderNotAvailable =>
              // likely leader may have changed, lets query new leader and try to query one
              Stream.eval(refreshMeta(topicId) map { _.leaderFor(topicId, partition) }) flatMap {
                case None => Stream.fail(err)
                case Some(nextBroker) =>
                  if (nextBroker.brokerId == broker.brokerId) Stream.fail(err)
                  else fetchFromBroker(nextBroker)
              }

            case err: BrokerReportedFailure => err.failure match {
              case ErrorType.OFFSET_OUT_OF_RANGE =>
                Stream.eval(queryOffsetRange(topicId, partition)) flatMap { case (min, max) =>
                Stream.eval(startFromRef.get) flatMap { startFrom =>
                  println(s"XXXR $startFrom ($min, $max)")
                  if (startFrom < min) Stream.eval(startFromRef.modify(_ => min)) >> fetchFromBroker(broker)
                  else if (startFrom > max) Stream.eval(startFromRef.modify(_ => max)) >> fetchFromBroker(broker)
                  else Stream.fail(new Throwable(s"Offset supplied is in acceptable range, but still not valid: $startFrom ($min, $max)", err))
                }}
              case ErrorType.NOT_LEADER_FOR_PARTITION => Stream.fail(err)
              case ErrorType.LEADER_NOT_AVAILABLE => Stream.fail(err)
              case other => Stream.fail(err)
            }

            case other => Stream.fail(other)
          }
        }

        Stream.eval(stateSignal.get map { _.leaderFor(topicId, partition) }) flatMap {
          case None => Stream.fail(LeaderNotAvailable(topicId, partition)) // todo: Requery meta before failing finally,
          case Some(broker) => fetchFromBroker(broker)
        }
      }

    }


//    /**
//      * Selects broker for given topic and partition and increments broker subscriptions
//      *
//      * This also selects brokers that are not failed and picks one with least subscriptions.
//      * This will select partition leader only if there is no ISR available
//      *
//      * @param topicId      Id of topic
//      * @param partition    Id of partition
//      * @param state        state
//      */
//    def subscribeAtBroker[F[_]](
//       topicId: String @@ TopicName
//       , partition: Int @@ PartitionId
//       , failed: Set[Int @@ Broker]
//     )(
//      state: ClientState[F]
//     ): (ClientState[F], Option[(Int @@ Broker, BrokerAddress)]) = {
//      val eligible = eligibleSubscribeBrokers(topicId, partition)(state)
//      eligible.filterNot { case (id, _, _) => failed.contains(id) }
//      .toSeq.sortBy { case (_, _, subscribers) => subscribers }
//      .reverse
//      .headOption match {
//        case None => (state, None)
//        case Some((id, address, _)) =>
//          val broker = state.brokers(id) // we are sure broker exists in map
//          val ns = state.copy ( brokers = state.brokers + ( id -> broker.registerSubscription((topicId, partition))))
//          (ns, Some((id, address)))
//      }
//    }

//
//    /**
//      * Selects all eligible brokers from supplied state.
//      *
//      * This selects all ISRs and leader if available and not failed for given partition.
//      * note that leader has always number of connections (last in returnes triple) set to Int.MaxValue and is always
//      * last in list.
//      *
//      * @param topicId      Id of topic
//      * @param partition    Id of partition
//      * @param state        state of the client.
//      */
//    def eligibleSubscribeBrokers[F[_]](
//       topicId: String @@ TopicName
//       , partition: Int @@ PartitionId
//     )(
//       state: ClientState[F]
//     ): Seq[(Int @@ Broker, BrokerAddress, Int)] = {
//      state.topics.get((topicId, partition)).toSeq flatMap { tapState =>
//        (tapState.followers
//        .flatMap { id => state.brokers.get(id).toSeq } filterNot { _.isFailed })
//        .map { s => (s.brokerId, s.address, s.subscriptionCount) } ++
//        (tapState.leader flatMap { state.brokers.get } filterNot { _.isFailed })
//        .map {  s => (s.brokerId, s.address, Int.MaxValue) }
//      }
//    }


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
      stateSignal: mutable.Signal[F, ClientState[F]]
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
                if (offsets.isEmpty) F.fail(new Throwable(s"Invalid response. No offsets available: $responses"))
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
        println(s"XXXY SENDING request: $input, expecting ${T.describe}")
       F.start(((Stream.emit(RequestMessage(protocol, 1, clientId, input)) ++ Stream.eval(ref.get).drain) through f(address) take 1).runLast.attempt.flatMap { r => println(s"RESULT: $r"); ref.setPure(r) }) >>
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



    /////////////////////////////////////////////////////////////////////////////////////////////
    //
    //
    // DELETE FROM BELOW
    // ---------------------------------------------

//
//    /**
//      * Requests initial metadata from all seed brokers. All seeds are queried initially, and then first response is returned.
//      * If the query to one broker fails, that is silently ignored (logged). If all queries fail, this evaluates to None,
//      * indicating that kafka ensemble cannot be reached.
//      *
//      * Connections used in this phase are dropped once first response is delivered.
//      *
//      * @param seeds      Seed nodes. Must not be empty.
//      * @param protocol   Protocol that all seeds use. Protocol version must be supported by all seeds.
//      * @param clientName Name of the client
//      */
//    def initialMetadata[F[_]](
//      seeds:Set[InetSocketAddress]
//      , protocol: ProtocolVersion.Value
//      , clientName: String
//      , brokerConnection: InetSocketAddress => Pipe[F,RequestMessage,ResponseMessage]
//    )(implicit F:Async[F], Logger:Logger[F]):F[Option[MetadataResponse]] = {
//      concurrent.join(seeds.size)(
//        Stream.emits(seeds.toSeq).map { address =>
//          Stream.emit(RequestMessage(protocol,1, clientName + "-initial-meta-rq", MetadataRequest(Vector.empty)))
//          .through(brokerConnection(address)).map { address -> _ }
//          .onError { err =>
//            Logger.error_(s"Failed to query metadata from seed broker $address", thrown = err)
//          }
//        }
//      )
//      .flatMap {
//        case (brokerAddress, ResponseMessage(_,meta:MetadataResponse)) =>
//          Logger.debug_(s"Received initial metadata from $brokerAddress : $meta") ++ Stream.emit(meta)
//        case (brokerAddress, other) =>
//          Logger.error_(s"Received invalid response to metadata request from $brokerAddress : $other")
//      }
//      .take(1)
//      .runLast
//    }


    /**
      * Builds the stream that operates control connection to every known broker.
      * Also updates signal of client state, whenever the metadata are updated.
      * @tparam F
      * @return
      */
    def controller[F[_]](
      signal: Signal[F,ClientState[F]]
      , mkBroker: (Int @@ Broker, BrokerAddress) => Stream[F,Nothing]
    )(implicit F:Async[F]):Stream[F,Nothing] = {
      /*
       * This is based on discrete stream of signal changes.
       * That means we may miss intermediate changes from the signal.
       * This shall not be problem, as if the broker appears and disappears w/o this stream noticing, we won't just
       * start new broker stream for it that will be killed anyhow.
       *
       * Note that broker state has generation flag that is attached to it. This is used to make sure we won't have two
       * simultaneous control connection active to given broker, as control connection dies when broker of that
       * generation is no longer available.
       */
      concurrent.join(Int.MaxValue)(
        signal.discrete.zipWithPrevious
        .flatMap { case (maybePrevious, next) =>
          val prevBrokers = maybePrevious.map(_.brokers.keySet).getOrElse(Set.empty)
          val nextBrokers = next.brokers.keySet

          Stream.emits(
            (prevBrokers diff nextBrokers).toSeq
            .flatMap { id => next.brokers.get(id).toSeq }
            .map { broker =>  mkBroker(broker.brokerId, broker.address) }
          )

        }
      )
    }

//    /**
//      * Builds the control connection to defined broker.
//      * It updates itself in the supplied signal when the connection is successful or failed.
//      * When the broker is first connected to, this will query initial metadata from the broker.
//      *
//      * - If the connection succeeds and first metadata are obtained, this will register to connected state
//      * - If the connection fails, the broker state is updated, and connection will be retried after given timeout
//      * - If the broker is removed from the broker list in state then this stops
//      *
//      * This connection is also responsible for any publish request for topic and partition
//      * where this broker is leader of.
//      *
//      *
//      * @param id               Id of the broker
//      * @param address          Address of the broker
//      * @param signal           Signal of the state where the broker has to update its state
//      * @param brokerConnection A function that will result in tcp connection with broker at specified address
//      * @param retryTimeout     If broker fails, this indicates retry timeout for new attempt to connect with broker
//      * @param protocol         Protocol to use for this broker connection. Used when generating
//      * @param queueBound       Number of messages to keep in the queue for this broker, before holding new requests
//      *                         from being submitted.
//      * @param clientName       Id of the client
//      */
//    def controlConnection[F[_]](
//      signal: Signal[F,ClientState[F]]
//      , brokerConnection: InetSocketAddress => Pipe[F,RequestMessage,ResponseMessage]
//      , retryTimeout: FiniteDuration
//      , protocol: ProtocolVersion.Value
//      , queueBound: Int
//      , clientName: String
//    )(
//      id: Int @@ Broker
//      , address: BrokerAddress
//    )(implicit F: Async[F], Logger: Logger[F]):Stream[F,Nothing] = {
//      import BrokerState._
//      type ControlRequest =
//        Either[(MetadataRequest, Async.Ref[F,MetadataResponse]),(ProduceRequest, Async.Ref[F,ProduceResponse])]
//
//      def brokerInstanceGone:Stream[F,Boolean] = ???
//
//
//      Stream.eval(async.boundedQueue[F,ControlRequest](queueBound)).flatMap { incomingQ =>
//      Stream.eval(async.signalOf(Map[Int,ControlRequest]())).flatMap { open =>
//        val clientId = clientName + "-control"
//
//        def connectAndProcess:Stream[F,Nothing] = ???
//        def updateError(err:Throwable):Stream[F,Failed[F]] = ???
//        def sleepRetry(f:Failed[F]):Stream[F,Nothing] = ???
//        def failOpened(f:Failed[F]):Stream[F,Nothing] = ???
//
//        def go:Stream[F,Nothing] = {
//          connectAndProcess
//          .onError { err =>
//            Logger.error_(s"Connection with broker failed. Broker: $id, address: $address", thrown = err) ++
//            updateError(err).flatMap( err =>
//              // fail all and await signal for retry,
//              failOpened(err).mergeDrainL(sleepRetry(err)) // this will terminate when both streams terminate.
//            ) ++ go
//          }
//        }
//
//        go
//      }.interruptWhen(brokerInstanceGone)
//    }}


    /**
      * Whenever new metadata is received from any broker, this is used to update the state to new topology
      * received from the broker.
      * @param s      Current state of the client
      * @param meta   Received metatdata
      */
    def updateMetadata[F[_]](
      s: ClientState[F]
      , meta:MetadataResponse
    ): ClientState[F] = { ???
//      val gen = tag[BrokerGeneration](s.generation + 1)
//      val updatedBrokers = updateBrokers(s.brokers, meta.brokers, gen)
//      val updatedTopics = updateTopics(updatedBrokers,s.topics,meta.topics)
//      s.copy(brokers = updatedBrokers, topics = updatedTopics, generation = gen)
    }

    /**
      * With updated list of brokers, this will update them in the state.
      *  - Brokers that does not exists in new update are removed
      *  - Brokers that are not known in state, they are added to the state with `Connecting` initial state.
      * @param brokers  Current brokers
      * @param updated  Next updated brokers.
      */
    def updateBrokers[F[_]](
     brokers: Map[Int @@ Broker, BrokerState[F]]
     , updated: Vector[Broker]
     , generation: Long @@ BrokerGeneration
    ): Map[Int @@ Broker, BrokerState[F]] = {
//      val um = updated.map { b => b.nodeId -> b }.toMap
//      val added = um.keySet diff brokers.keySet
//      val removed = brokers.keySet diff um.keySet
//      val newBrokers = added.toSeq.flatMap { brokerId =>
//        um.get(brokerId).toSeq.map { broker =>
//          brokerId -> BrokerState.Ready[F](brokerId, BrokerAddress(broker.host, broker.port, generation))
//        }
//      }
//      (brokers -- removed) ++ newBrokers
      ???
    }

    /**
      * Updates topics state from received update.
      * Also consult new state of the brokers to eventually elect new leaders // topics for the partition.
      *
      * If the leader (or tail subscriber) for the topic/partition is not anymore in supplied brokers map,
      * then that leader has to be removed and eventually replaced by new leader.
      *
      * Any brokers not found in brokers map are removed from the `follower` list.
      *
      *
      * If the topics change
      *
      * @param brokers    Map of current brokers. If the metadata was updated, this must be brokers with metadata
      *                   already applied
      * @param topics     Current, configured topics
      * @param updated    Updated metadata received from the broker.
      */
    def updateTopics[F[_]](
     brokers: Map[Int @@ Broker, BrokerState[F]]
     , topics: Map[TopicAndPartition, TopicAndPartitionState]
     , updated: Vector[TopicMetadata]
     ): Map[TopicAndPartition, TopicAndPartitionState] = {
//      val updatedData = updated.flatMap { tm =>
//        if (tm.error.nonEmpty) Vector.empty
//        else tm.partitions.map { pm => (tm.name, pm.id) -> pm }
//      }.toMap
//
//      val added = (updatedData.keySet diff topics.keySet).flatMap { case tap@(topicId, partitionId) =>
//        updatedData.get(tap).map { pm =>
//          tap -> TopicAndPartitionState(
//            topic = topicId
//            , partition = partitionId
//            , leader = pm.leader.filter { brokers.isDefinedAt }
//            , followers = pm.isr.filter { brokers.isDefinedAt }.map { brokerId => brokerId -> FollowerState.Operational[F](brokerId) }.toMap
//          )
//        }
//      }
//
//      topics.flatMap { case (tap, state) =>
//        updatedData.get(tap).filter(_.error.isEmpty).map { pm =>
//          val newFollowers = (pm.isr.toSet diff state.followers.keySet).filter(brokers.isDefinedAt).map { brokerId =>
//            brokerId -> FollowerState.Operational[F](brokerId)
//          }.toMap
//
//          tap -> state.copy(
//            leader = pm.leader.filter { brokers.isDefinedAt }
//            , followers = state.followers.filter { case (brokerId,_) => pm.isr.contains(brokerId) && brokers.isDefinedAt(brokerId) } ++ newFollowers
//          )
//        }.toMap
//      } ++ added
      ???
    }


    /**
      * Refreshes the metadata of the cluster at periodic intervals. If there is no broker available in  metadata
      * that is known to be alive, this uses seeds to query metadata, and pouplates topology with any updates received.
      *
      * Supplied signal is mutated to add/remove brokers, change their role (leader, follower) for topics and partitions.
      *
      * KafkaClient then uses this updated signal to be notified about topology changes and adjust fetch/publish strategy
      * respectively.
      *
      * @param fromSeeds          Queries available seeds, returning first metadata response if available or None otherwise
      * @param signal             Signal of the client state
      * @param refreshInterval    How frequently to check for the updates
      * @return
      */
    def metadataRefresher[F[_]](
     fromSeeds: F[Option[MetadataResponse]]
     , signal: Signal[F,ClientState[F]]
     , refreshInterval: FiniteDuration
    )(implicit S:Scheduler, F:Async[F], Logger:Logger[F]):Stream[F,Nothing] = {   ???
//      import BrokerState._
//
//       time.awakeEvery(refreshInterval).flatMap { _ =>
//         Stream.eval(signal.get).flatMap { s =>
//           val queryFromConnected:Stream[F,Stream[F,MetadataResponse]] =
//             Stream.emits(
//               s.brokers.values.collect { case Connected(brokerId,address,getMetadata,_,_) =>
//                 Stream.eval(getMetadata).attempt.flatMap {
//                   case Left(rsn) => Logger.error_(s"Failed to query metadata from broker $brokerId ($address)", thrown = rsn)
//                   case Right(maybeMetadataResponse) => Stream.emits(maybeMetadataResponse.toSeq)
//                 }
//               }.toSeq
//             )
//
//           /*
//            * We assume only last response contains most up to date data.
//            * This may not be correct assumption, but likely the cluster will be synced at next state
//            * or whenever produce request will be directed to incorrect leader.
//            *
//            * Metadata requests serves also like sort of a health check on all the connected brokers.
//            */
//           Stream.eval(concurrent.join(Int.MaxValue)(queryFromConnected).runLog).flatMap { meta =>
//             if (meta.isEmpty) Stream.eval(fromSeeds).flatMap(r => Stream.emits(r.toSeq))
//             else Stream.emits(meta)
//           }
//           .last
//           .flatMap {
//             case None =>
//               Logger.error_("No single member or seed was able to respond with metadata, state of all members will be cleaned")
//               .append(Stream.emit(MetadataResponse(Vector.empty, Vector.empty)))
//             case Some(resp) => Stream.emit(resp)
//           }.flatMap { meta =>
//             Stream.eval(signal.modify(updateMetadata(_, meta))).flatMap { change =>
//               if (change.previous == change.now ) Stream.empty
//               else Stream.eval_(logStateChanges(change, meta))
//             }
//           }
//         }
//       }
    }


    def logStateChanges[F[_]](s:ClientState[F], meta:MetadataResponse)(implicit Logger:Logger[F]):Stream[F,Nothing] = {
      Logger.debug_(s"Updated client state with received metadata. New state is $s. Metadata: $meta")
    }





  }


}





