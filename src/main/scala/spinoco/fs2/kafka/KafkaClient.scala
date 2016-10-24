package spinoco.fs2.kafka

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.time.LocalDate
import java.util.concurrent.atomic.AtomicInteger

import fs2._
import fs2.util.Async
import fs2.async.mutable.{Queue, Signal}
import Stream.eval
import fs2.Chunk.Bytes
import scodec.bits.ByteVector
import shapeless.tag
import shapeless.tag._
import spinoco.fs2.kafka.network.BrokerConnection
import spinoco.fs2.kafka.state.BrokerState.{Connected, Connecting}
import spinoco.fs2.kafka.state._
import spinoco.protocol.kafka.Message.{CompressedMessages, SingleMessage}
import spinoco.protocol.kafka.Request.{FetchRequest, MetadataRequest, ProduceRequest, RequiredAcks}
import spinoco.protocol.kafka._
import spinoco.protocol.kafka.Response.{FetchResponse, MetadataResponse, PartitionProduceResult, ProduceResponse}

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
    * @param topic            Name of the topic to subscribe to
    * @param partition        Partition to subscribe to
    * @param offset           Offset of the topic to start to read from. First received message may have offset larger
    *                         than supplied offset only if the oldest message has offset higher than supplied offset.
    *                         Otherwise this will always return first message with this offset.
    * @param maxQueue         Maximum number of messages to en-queue, before this subscriber starts to block other subscribers to
    *                         consume from the topic and partition. If there are more messages to be processed by this subscriber than
    *                         maxQueue, then consumer from kafka awaits before they are processed.
    * @param followTail       If true, this won't terminate once last message in the offset is received, otherwise
    *                         This terminates once last message is received.
    * @return
    */
  def subscribe(
    topic: String
    , partition: Int
    , offset: Long
    , maxQueue: Int
    , followTail: Boolean
  ):Stream[F,TopicMessage]


  /**
    * Publishes single message to the supplied topic.
    * Returns None, if the message was not published due topic/partition not existent or
    * Some(offset) of published message.
    *
    * When `F` finishes its evaluation, message is guaranteed to be seen by the ensemble.
    *
    * @param topic              Topic to publish to
    * @param partition          Partition to publish to
    * @param key                Key of the message
    * @param message            Message itself
    * @param requireQuorum      True indicates that quorum of the brokers needs to confirm the reception
    *                           of this message before returning back
    * @param serverAckTimeout   Timeout server waits for replicas to ack the request. If the publish request won't be acked by
    *                           server in this time, then the request fails to be published.
    * @return
    */
  def publish1(
    topic: String
    , partition: Int
    , key: Chunk.Bytes
    , message: Chunk.Bytes
    , requireQuorum: Boolean
    , serverAckTimeout: FiniteDuration
  ):F[Option[Long]]

  /**
    * Like `publish` except this won't wait for the confirmation that message was published (fire'n forget).
    */
  def publishUnsafe1(
    topic:String
    , partition:Int
    , key:Chunk.Bytes
    , message:Chunk.Bytes
  ):F[Unit]

  /**
    * Publishes Chunk of messages to the ensemble. The messages are published as a whole batch, so when this
    * terminates, all messages are guaranteed to be processed by kafka server.
    *
    * Note that this returns a Chunk of `A` and offset of the message published. If this won't return `A`
    * then that message was not published, likely because the topic and partition does not exists.
    *
    * @param messages           Chunk of messages to publish. First is id of the topic, second is partition, then key and message itself.
    *                           Additionally `A` may be passed to pair the offset of the message in resulting chunk.
    * @param requireQuorum      True indicates that quorum of the brokers needs to confirm the reception
    *                           of this message chunk before returning back
    * @return
    */
  def publishN[A](
    messages: Chunk[(String, Int, Chunk.Bytes, Chunk.Bytes, A)]
    , requireQuorum: Boolean
    , serverAckTimeout: FiniteDuration
    , compress: Option[Compression.Value]
  ):F[Chunk[(A, Long)]]

  /**
    * Like `publishN` except this won't await for messages to be confirmed to be published successfully.
    */
  def publishUnsafeN(
    messages:Chunk[(String, Int, Chunk.Bytes, Chunk.Bytes)]
    , compress: Option[Compression.Value]
  ):F[Unit]

  /**
    * Provides discrete stream of topics and partitions available to this client.
    * Normally, this signals updates at beginning when client starts, and then there is change in topology detected
    * (i.e. broker becoming unavailable, leader changes).
    *
    * Additionally whenever you call `refreshTopology` then the client refreshes the topology which in result will turn
    * in this signal being updated.
    *
    * @return
    */
  def topics:Stream[F,Map[String,Set[Int]]]

  /**
    * When this is run, then brokers are queried for their topology view and topology is updated.in this client
    *
    * @return
    */
  def refreshTopology:F[Unit]
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
    * @param brokerReadMaxChunkSize   Max size of chunk that is read in single tcp operation from broker
    * @param brokerRetryDelay         Delay between broker reconnects if the connection to the broker failed.
    * @param brokerControlQueueBound  Max number of unprocessed messages to keep for broker, before stopping accepting new messages for broker.
    * @param clientTopologyRefresh    The periodic interval at which we refresh the known topology
    * @see [[spinoco.fs2.kafka.client]]
    */
  def apply[F[_]](
    ensemble:Set[InetSocketAddress]
    , protocol: ProtocolVersion.Value
    , clientName: String
    , brokerWriteTimeout: Option[FiniteDuration] = Some(10.seconds)
    , brokerReadMaxChunkSize: Int = 256 * 1024
    , brokerRetryDelay: FiniteDuration = 10.seconds
    , brokerControlQueueBound: Int = 10 * 1000
    , clientTopologyRefresh: FiniteDuration = 30.minutes
  )(implicit AG: AsynchronousChannelGroup, F: Async[F], S:Scheduler, Logger:Logger[F]):Stream[F,KafkaClient[F]] = {
    def brokerConnection(addr:InetSocketAddress):Pipe[F,RequestMessage,ResponseMessage] =
      BrokerConnection(addr, brokerWriteTimeout, brokerReadMaxChunkSize)


    eval(async.signalOf[F,ClientState[F]](ClientState.initial[F])).flatMap { state =>
    eval(impl.initialMetadata(ensemble,protocol,clientName,brokerConnection)).flatMap {
      case None => Stream.fail(new Throwable(s"Failed to query initial metadata from seeds, client will terminate ($clientName): $ensemble"))
      case Some(meta) =>
        eval(state.set(impl.updateMetadata(ClientState.initial[F],meta))).flatMap { _ =>
          concurrent.join(Int.MaxValue)(Stream(
            impl.controller(state,impl.controlConnection(state,brokerConnection,brokerRetryDelay,protocol,brokerControlQueueBound, clientName))
            , impl.metadataRefresher(impl.initialMetadata(ensemble,protocol,clientName,brokerConnection), state, clientTopologyRefresh)
            , Stream.eval(impl.mkClient(state, brokerConnection, clientName, protocol))
          ))
        }
    }}
  }



  protected[kafka] object impl {


    def mkClient[F[_]](
      signal: Signal[F,ClientState[F]]
      , brokerConnection: InetSocketAddress => Pipe[F, RequestMessage, ResponseMessage]
      , clientName: String
      , protocolVersion: ProtocolVersion.Value
    )(implicit F: Async[F], S:Scheduler):F[KafkaClient[F]] = F.delay {
      new KafkaClient[F] {
        def subscribe(topic: String, partition: Int, offset: Long, maxQueue: Int, followTail: Boolean): Stream[F, TopicMessage] = _subscribe(topic, partition, offset, maxQueue, followTail, clientName, signal, brokerConnection, protocolVersion)
        def publish1(topic: String, partition: Int, key: Bytes, message: Bytes, requireQuorum: Boolean, serverAckTimeout: FiniteDuration): F[Option[Long]] = _publish1(signal, tag[TopicName](topic), tag[PartitionId](partition), key, message, requireQuorum, serverAckTimeout)
        def publishUnsafe1(topic: String, partition: Int, key: Bytes, message: Bytes): F[Unit] = _publishUnsafe1(signal, tag[TopicName](topic), tag[PartitionId](partition), key, message)
        def publishN[A](messages: Chunk[(String, Int, Bytes, Bytes, A)], requireQuorum: Boolean, serverAckTimeout: FiniteDuration, compress: Option[Compression.Value]): F[Chunk[(A, Long)]] = _publishN(messages, compress, requireQuorum, serverAckTimeout, signal)
        def publishUnsafeN(messages: Chunk[(String, Int, Bytes, Bytes)], compress: Option[Compression.Value]): F[Unit] = _publishUnsafeN(messages, compress, signal)
        def topics: Stream[F, Map[String, Set[Int]]] = signal.discrete.map(_.topics.groupBy(_._1._1).map{case (topic, partitions) => (topic:String) -> partitions.keySet.map(_._2:Int)})
        def refreshTopology: F[Unit] = F.map(_refreshTopology(signal)){_ => ()}
      }
    }

    /**
      * The implementation of subscribe
      *
      * @param topic            The topic from which we want to read
      * @param partition        The partition from which we want to read
      * @param offset           The offset at which we want to start
      * @param maxQueue         The maximum number of pre-fetched messages
      * @param followTail       Whether we should follow await further updates after
      *                         the the end of current messages is reached
      * @param clientName       The name of this client that is connecting
      * @param clientSignal     The signal of this client state
      * @param brokerConnection Builder for a connection to a broker
      * @param protocolVersion  The protocol used against the current nodes
      */
    def _subscribe[F[_]](
      topic: String
      , partition: Int
      , offset: Long
      , maxQueue: Int
      , followTail: Boolean
      , clientName: String
      , clientSignal: Signal[F,ClientState[F]]
      , brokerConnection: InetSocketAddress => Pipe[F, RequestMessage, ResponseMessage]
      , protocolVersion: ProtocolVersion.Value
    )(implicit F: Async[F], S: Scheduler): Stream[F, TopicMessage] = {
      Stream.eval(async.boundedQueue[F, TopicMessage](maxQueue)).flatMap{queue =>

        val query = _subscribeLeader(
          topic = tag[TopicName](topic)
          , partition = tag[PartitionId](partition)
          , offset = tag[Offset](offset)
          , followTail = followTail
          , clientName = clientName
          , signal = clientSignal
          , brokerConnection = brokerConnection
          , protocolVersion = protocolVersion
        )

        concurrent.join(2)(
          Stream.emits(Seq(
            queue.dequeue
            , query.to(queue.enqueue).drain
          ))
        )
      }
    }

    /**
      * Collects fetch responses from server, and then it acts according
      * following rules:
      *   - In case the stream is exhausted, it emits on left last successful offset
      *     this offset should be used to restart the fetching
      *   - In case there is no response for partition and we do no follow tail
      *     then we assume that we have finished the fetch
      *   - In case there is an error for the partition fetch, we emit the last
      *     offset on the left and follow the restart procedure
      *   - Otherwise we emit topic messages on the right
      *   - In case we have reached the final water mark and we do not follow tail
      *     then we finish the pull
      *
      * @param initialLast  The offset with which this collect started
      * @param enqueue      Enqueue method to request another poll for long polling
      * @param followTail   Whether we follow tail
      * @param topic        The topic that this fetch is for
      * @param partition    The partition that this fetch is for
      */
    def collect[F[_]](
      initialLast: Long @@ Offset
      , enqueue: Long @@ Offset => F[Unit]
      , followTail: Boolean
      , topic: String @@ TopicName
      , partition: Int @@ PartitionId
    ): Handle[F, FetchResponse] => Pull[F, Either[Long @@ Offset, TopicMessage], Nothing] = {
      def go(last: Long @@ Offset): Handle[F, FetchResponse] => Pull[F, Either[Long @@ Offset, TopicMessage], Nothing] = {
        _.receive1Option {
          case None => Pull.output1(Left(last)) >> Pull.done
          case Some((fr, handle)) =>
            fr.data.find(_._1 == topic).flatMap(_._2.find(_.partitionId == partition)) match {
              case None =>
                if (followTail) Pull.eval(enqueue(last)) >> go(last)(handle)
                else Pull.done

              case Some(parResp) =>
                parResp.error match {
                  case Some(_) => Pull.output1(Left(last)) >> Pull.done
                  case None =>
                    val (nlast, messages) = makeTopicMessages(parResp.messages, last)
                    if (nlast == parResp.highWMOffset && !followTail) Pull.done
                    else {
                      Pull.output(Chunk.seq(messages.map(Right(_)))).flatMap(_ =>
                        Pull.eval(enqueue(nlast))
                      ) >> go(nlast)(handle)
                    }
                }
            }
        }
      }

      go(initialLast)
    }

    /**
      * The actuall implememntation of fetching, this updates number of connections
      * to a broker, and creates a long poll for fetching against current master for
      * this topic and partition
      *
      * @param topic            The topic from which we want to read
      * @param partition        The partition from which we want to read
      * @param offset           The offset at which we want to start
      * @param followTail       Whether we should follow await further updates after
      *                         the the end of current messages is reached
      * @param clientName       The name of this client that is connecting
      * @param signal           The signal of this client state
      * @param brokerConnection Builder for a connection to a broker
      * @param protocolVersion  The protocol used against the current nodes
      */
    def _subscribeLeader[F[_]](
      topic: String @@ TopicName
      , partition: Int @@ PartitionId
      , offset: Long @@ Offset
      , followTail: Boolean
      , clientName: String
      , signal: Signal[F,ClientState[F]]
      , brokerConnection: InetSocketAddress => Pipe[F, RequestMessage, ResponseMessage]
      , protocolVersion: ProtocolVersion.Value
    )(implicit F: Async[F], S: Scheduler): Stream[F, TopicMessage] = {

      /**
        * Creates a long poll for a given connected broker
        * @param conn The name of the
        */
      def longPoll(
        conn: Connected[F]
      ): Stream[F, Either[Long @@ Offset, TopicMessage]] = {
        Stream.eval(async.boundedQueue[F, Long @@ Offset](2)).flatMap{queue =>

          Stream.eval_(queue.enqueue1(offset)) ++
          queue.dequeue.map{ offset =>
            RequestMessage(
              version = protocolVersion
              , correlationId = 0
              , clientId = clientName
              , request = FetchRequest(
                replica = tag[Broker](-1)
                , maxWaitTime = 100.millis
                , minBytes = 4096
                , topics = Vector((topic, Vector((partition, offset, 8192))))
              )
            )
          }.through(brokerConnection(new InetSocketAddress(conn.address.host, conn.address.port)))
          .collect{ case ResponseMessage(_, fr: FetchResponse) => fr}
          .pull(collect(offset, queue.enqueue1, followTail, topic, partition))
        }
      }

      getLeaderFor(signal, topic, partition).flatMap{
        case None => _subscribeLeader(topic, partition, offset, followTail, clientName,  signal, brokerConnection, protocolVersion)
        case Some(conn) =>
          Stream.eval_(
            signal.modify{cs =>
              cs.copy(
                brokers = cs.brokers + (conn.brokerId -> conn.copy(connections = conn.connections + 1))
              )
          }) ++
          longPoll(conn).flatMap{
            case Right(msg) => Stream.emit(msg)
            case Left(lastFromThis) =>
              Stream.eval_(
                signal.modify{cs =>
                  val brokers =
                    cs.brokers.get(conn.brokerId)
                    .collect{case con: Connected[F] =>
                      con.copy(connections = con.connections - 1)
                    }.fold(cs.brokers){broker => cs.brokers + (broker.brokerId -> broker)}

                  cs.copy(brokers = brokers)
                }) ++
              Stream.eval_(_refreshTopology(signal)) ++
                _subscribeLeader(
                  topic = topic
                  , partition = partition
                  , offset = lastFromThis
                  , followTail = followTail
                  , clientName = clientName
                  , signal = signal
                  , brokerConnection = brokerConnection
                  , protocolVersion = protocolVersion
                )
          }
      }
    }

    /**
      * Makes topic messages out of kafka messages, this unwraps all of compressed messages
      * and drops all messages that are before `from`
      *
      * @param messages Kafka messages that were received with fetch
      * @param from     The last known offset for this fetch
      */
    def makeTopicMessages(messages: Vector[Message], from: Long @@ Offset): (Long @@ Offset, Vector[TopicMessage]) = {
      def expand(current: Vector[Message], expanded: Vector[SingleMessage]): Vector[SingleMessage] = {
        current.headOption match {
          case None => expanded
          case Some(single: SingleMessage) =>
            expand(current.drop(1), expanded :+ single)

          case Some(compressed: CompressedMessages) =>
            val updated =
              compressed.messages.collect{case s: SingleMessage => s}
              .map{m => m.copy(offset = compressed.offset + m.offset)}

            expand(current.drop(1), expanded ++ updated)
        }
      }

      val expanded = expand(messages, Vector())
      val fromIdx = expanded.indexWhere(_.offset > from)
      if (fromIdx < 0) (from, Vector.empty)
      else {
        val (_, wanted) = expanded.splitAt(fromIdx)
        val last = wanted.lastOption.fold(from){sm => tag[Offset](sm.offset)}
        (last, wanted.map(sm => TopicMessage(sm.offset, sm.key, sm.value)))
      }
    }


    /**
      * Implementation of publishing one message into given topic and partition.
      * Awaits an answer, it returns the offset of said message, or none
      * in case the publish failed
      *
      * @param signal           The signal of this client
      * @param topic            The destination topic for this publish
      * @param partition        The destination partition for this publish
      * @param key              The key of the message
      * @param message          The content of the message
      * @param requireQuorum    The required response from the server
      * @param serverAckTimeout The maximum time for ack from the server
      */
    def _publish1[F[_]](
      signal: Signal[F,ClientState[F]]
      , topic: String @@ TopicName
      , partition: Int @@ PartitionId
      , key: Bytes
      , message: Bytes
      , requireQuorum: Boolean
      , serverAckTimeout: FiniteDuration
    )(implicit F: Async[F], S:Scheduler): F[Option[Long]] = {
      val reqAck =
        if(requireQuorum) RequiredAcks.Quorum
        else RequiredAcks.LocalOnly

      F.map(
        _publishOne(
          signal
          , topic
          , partition
          , key
          , message
          , reqAck
          , serverAckTimeout
        )
      ){
        _.flatMap(_.data.find(_._1 == topic).flatMap(_._2.find(_._1 == partition).map(_._2.offset)))
      }
    }

    /**
      * Implementation of publishing one message into given topic and partition.
      * Does not await response from server
      *
      * @param signal           The signal of this client
      * @param topic            The destination topic for this publish
      * @param partition        The destination partition for this publish
      * @param key              The key of the message
      * @param message          The content of the message
      */
    def _publishUnsafe1[F[_]](
      signal: Signal[F,ClientState[F]]
      , topic: String @@ TopicName
      , partition: Int @@ PartitionId
      , key: Bytes
      , message: Bytes
    )(implicit F: Async[F], S:Scheduler): F[Unit] = {
      F.map(
        _publishOne(
          signal
          , topic
          , partition
          , key
          , message
          , RequiredAcks.NoResponse
          , 1.second
        )
      ){_ => ()}
    }

    /**
      * Implementation of publishing multiple messages into multiple topics and partitions.
      * Awaits an answer.
      *
      * @param messages           The messages to be send
      * @param compress           Whether the messages should be compressed
      * @param requireQuorum      Whether requires quorum confirmation
      * @param serverAckTimeout   The maximum ack timeout for the server
      * @param signal             The signal of this client
      */
    def _publishN[F[_], A](
      messages: Chunk[(String, Int, Bytes, Bytes, A)]
      , compress: Option[Compression.Value]
      , requireQuorum: Boolean
      , serverAckTimeout: FiniteDuration
      , signal: Signal[F,ClientState[F]]
    )(implicit F: Async[F], S:Scheduler): F[Chunk[(A, Long)]] = {
      import shapeless.syntax.std.tuple._
      val reqAck =
        if(requireQuorum) RequiredAcks.Quorum
        else RequiredAcks.LocalOnly

      F.map(
        _publishNWith(
          serverAckTimeout
          , createMessages(messages.map(_.take(4)), compress)
          , reqAck
          , signal
        )
      ){
        _.map{resp =>
          Chunk.seq(
            messages.toVector.flatMap{case (topic, partition, _, _, a) =>
              resp.get(tag[TopicName](topic))
              .flatMap(_.get(tag[PartitionId](partition)))
              .map(partitionResponse => a -> (partitionResponse.offset: Long))}
          )
        }.getOrElse(Chunk.empty)
      }
    }

    /**
      * Implementation of publishing multiple messages into multiple topics and partitions.
      * Does not await an answer.
      *
      * @param messages           The messages to be send
      * @param compress           Whether the messages should be compressed
      * @param signal             The signal of this client
      */
    def _publishUnsafeN[F[_]](
      messages: Chunk[(String, Int, Bytes, Bytes)]
      , compress: Option[Compression.Value]
      , signal: Signal[F,ClientState[F]]
    )(implicit F: Async[F], S:Scheduler): F[Unit] = {

      F.map(
        _publishNWith(
          1.second
          , createMessages(messages, compress)
          , RequiredAcks.NoResponse
          , signal
        )
      ){_ => ()}
    }

    /**
      * Implementation for both publish ones.
      *
      * Gets a leader for the partition, by consulting the state, in case
      * the leader in mid election it awaits the it finishing or in case
      * the broker is still connecting then we await connection
      *
      * @param signal           The signal of this client
      * @param topic            The destination topic for this publish
      * @param partition        The destination partition for this publish
      * @param key              The key of the message
      * @param message          The content of the message
      * @param ack              The type of the ack this publish requires
      * @param serverAckTimeout The maximum time for ack from the server
      */
    def _publishOne[F[_]](
      signal: Signal[F,ClientState[F]]
      , topic: String @@ TopicName
      , partition: Int @@ PartitionId
      , key: Bytes
      , message: Bytes
      , ack: RequiredAcks.Value
      , serverAckTimeout: FiniteDuration
    )(implicit F: Async[F], S:Scheduler): F[Option[ProduceResponse]] = {
      F.flatMap[Option[Connected[F]], Option[ProduceResponse]](
        F.map(
          getLeaderFor(signal, topic, partition)
          .runLast
        )(_.flatten)
      ){
        case None => F.pure(Option.empty)

        case Some(conn: Connected[F]) =>

          val msg =
            SingleMessage(
              offset = 0 //This is ignored for producer
              , version = MessageVersion.V0
              , timeStamp = None //None for producer
              , key = ByteVector(key.values)
              , value = ByteVector(message.values)
            )

          val req =
            ProduceRequest(
              ack
              , serverAckTimeout
              , Vector((topic, Vector((partition, Vector(msg)))))
            )

          conn.produce(req)
      }
    }

    /**
      * Groups messages by topic and partition, creates compression message in case
      * it is required
      *
      * @param messages     The messages to be grouped
      * @param compression  Whether compression is required if so what type
      */
    def createMessages(messages: Chunk[(String, Int, Bytes, Bytes)], compression: Option[Compression.Value]): Vector[((String @@ TopicName, Int @@ PartitionId), Vector[Message])] = {
      /** Creates single messages **/
      def makeSingles(messages: Vector[(String, Int, Bytes, Bytes)]): Vector[Message] = {
        messages.map{ case (_, _, key, message) =>
          SingleMessage(
            offset = 0 //This is ignored for producer
            , version = MessageVersion.V0
            , timeStamp = None //None for producer
            , key = ByteVector(key.values)
            , value = ByteVector(message.values)
          )
        }
      }

      /** Creates compressed message **/
      def makeCompressed(messages: Vector[(String, Int, Bytes, Bytes)])(compression: Compression.Value): CompressedMessages = {
        CompressedMessages(
          offset = 0 //This is ignored for producer
          , version = MessageVersion.V0
          , compression = compression
          , timeStamp = None //None for producer
          , messages = makeSingles(messages)
        )
      }

      messages.toVector.groupBy{case (topic, partition, _, _) => (tag[TopicName](topic), tag[PartitionId](partition))}
      .map{case (key, messages) =>
        key ->
          compression.fold(makeSingles(messages))(
            compression => Vector(makeCompressed(messages)(compression))
          )
      }.toVector
    }

    /**
      * Publishes messages to leaders for given topic and partition
      *
      * @param timeout    The server timeout for response
      * @param messages   The messages to be published
      * @param ack        The required ack from the server
      * @param signal     The signal of this client
      */
    def _publishNWith[F[_]](
      timeout: FiniteDuration
      , messages: Vector[((String @@ TopicName, Int @@ PartitionId), Vector[Message])]
      , ack: RequiredAcks.Value
      , signal: Signal[F,ClientState[F]]
    )(implicit F: Async[F], S:Scheduler): F[Option[Map[String @@ TopicName, Map[Int @@ PartitionId, PartitionProduceResult]]]] = {
      sendToLeaders(signal, messages){
        case (conn, data) =>
          val messageData =
            data.groupBy(_._1._1)
            .map{case (topic, partitions) =>
              topic -> partitions.map{case ((_, partition), messages) => partition -> messages }
            }

          val request =
            ProduceRequest(ack, timeout, messageData.toVector)

          conn.produce(request)
      }.fold(Map.empty[String @@ TopicName, Map[Int @@ PartitionId, PartitionProduceResult]]){
        case (acc, None) => acc
        case (acc, Some(resp)) =>
          acc ++
            resp.data.map{ case (topic, partitions) =>
              topic -> acc.get(topic).fold(partitions.toMap){_ ++ partitions.toMap}
            }.toMap
      }.runLast
    }

    /**
      * Refreshes the known topology of the client
      *
      * @param signal The signal of this client
      */
    def _refreshTopology[F[_]](
      signal: Signal[F,ClientState[F]]
    )(implicit F: Async[F]): F[ClientState[F]] = {
      F.flatMap(signal.get){cs =>
      F.flatMap{
        F.parallelTraverse(
          cs.brokers.values.collect{case c: Connected[F] => c}.toList
        ){_.getMetadata}
      }{metas =>
        F.map(
          signal.modify { currentCs =>
            metas.flatten.headOption.fold(
              currentCs
            ) { meta =>
              updateMetadata(currentCs, meta)
            }
          }
        ){_.now}
      }}
    }

    /**
      * Requests initial metadata from all seed brokers. All seeds are queried initially, and then first response is returned.
      * If the query to one broker fails, that is silently ignored (logged). If all queries fail, this evaluates to None,
      * indicating that kafka ensemble cannot be reached.
      *
      * Connections used in this phase are dropped once first response is delivered.
      *
      * @param seeds      Seed nodes. Must not be empty.
      * @param protocol   Protocol that all seeds use. Protocol version must be supported by all seeds.
      * @param clientName Name of the client
      */
    def initialMetadata[F[_]](
      seeds:Set[InetSocketAddress]
      , protocol: ProtocolVersion.Value
      , clientName: String
      , brokerConnection: InetSocketAddress => Pipe[F,RequestMessage,ResponseMessage]
    )(implicit F:Async[F], Logger:Logger[F]):F[Option[MetadataResponse]] = {
      concurrent.join(seeds.size)(
        Stream.emits(seeds.toSeq).map { address =>
          Stream.emit(RequestMessage(protocol,1, clientName + "-initial-meta-rq", MetadataRequest(Vector.empty)))
          .through(brokerConnection(address)).map { address -> _ }
          .onError { err =>
            Logger.error_(s"Failed to query metadata from seed broker $address", thrown = err)
          }
        }
      )
      .flatMap {
        case (brokerAddress, ResponseMessage(_,meta:MetadataResponse)) =>
          Logger.debug_(s"Received initial metadata from $brokerAddress : $meta") ++ Stream.emit(meta)
        case (brokerAddress, other) =>
          Logger.error_(s"Received invalid response to metadata request from $brokerAddress : $other")
      }
      .take(1)
      .runLast
    }


    /**
      * Builds the stream that operates control connection to every known broker.
      * Also updates signal of client state, whenever the metadata are updated.
      *
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

    /** Awaits connection of a given broker **/
    def awaitBrokerConnected[F[_]](
      signal: Signal[F,ClientState[F]]
      , brokerId: Int @@ Broker
    )(implicit F: Async[F]): Stream[F, Option[Connected[F]]] = {
      signal.discrete.flatMap{cs =>
        cs.brokers.get(brokerId) match {
          case Some(_: Connecting[F]) => Stream.empty
          case Some(c: Connected[F]) => Stream.emit(Some(c))
          case _ => Stream.emit(None)
        }
      }.take(1)
    }

    /**
      * Groups messages by leaders of their topic and partition.
      * Then we apply a function with the connected broker and messages to it.
      * In case the leader is not yet elected we await election.
      * In case the broker is not connected yet we await its connection.
      */
    def sendToLeaders[F[_], A, B](
      signal: Signal[F, ClientState[F]]
      , msges: Vector[((String @@ TopicName,Int @@ PartitionId), Vector[A])]
    )(
      doAction: (Connected[F], Vector[((String @@ TopicName, Int @@ PartitionId), Vector[A])]) => F[B]
    )(implicit F: Async[F], S:Scheduler): Stream[F, B] = {
      import shapeless.syntax.std.tuple._
      Stream.eval(signal.get).flatMap{cs =>
        val byBroker =
          msges.map{case (key, messages) => (cs.topics.get(key).flatMap(_.leader), key, messages)}.groupBy(_._1)

        concurrent.join(byBroker.size){
          Stream.emits(
            byBroker.toVector
          ).map{
            case (Some(brokerId), data) =>
              awaitBrokerConnected(signal, brokerId).flatMap{
                case None =>
                  time.sleep(1.second) ++
                  Stream.eval_(_refreshTopology(signal)) ++
                  sendToLeaders(signal, data.map{_.drop(1)})(doAction)

                case Some(conn) =>
                  Stream.eval(doAction(conn, data.map{_.drop(1)}))
              }

            case (None, data) =>
              time.sleep(1.second) ++
                Stream.eval_(_refreshTopology(signal)) ++
                sendToLeaders(signal, data.map{_.drop(1)})(doAction)
          }
        }

      }
    }

    /** Gets a leader for given topic and partition, while getting state from signal **/
    def getLeaderFor[F[_]](
      signal: Signal[F,ClientState[F]]
      , topic: String @@ TopicName
      , partitionId: Int @@ PartitionId
    )(implicit F: Async[F], S:Scheduler): Stream[F,Option[Connected[F]]] = {
      Stream.eval(signal.get)
      .flatMap{_getLeaderFor(signal, topic, partitionId)}
    }

    /** Gets a leader for given topic and partition **/
    def _getLeaderFor[F[_]](
      signal: Signal[F,ClientState[F]]
      , topic: String @@ TopicName
      , partitionId: Int @@ PartitionId
    )(cs: ClientState[F])(implicit F: Async[F], S:Scheduler): Stream[F,Option[Connected[F]]] = {
      cs.topics.get((topic, partitionId)).flatMap(_.leader) match {
        case None => awaitElection(signal, topic, partitionId)
        case Some(leaderId) => awaitBrokerConnected(signal, leaderId)
      }
    }

    /** Awaits selection leader selection for a given topic and partition **/
    def awaitElection[F[_]](
      signal: Signal[F,ClientState[F]]
      , topic: String @@ TopicName
      , partitionId: Int @@ PartitionId
    )(implicit F: Async[F], S:Scheduler): Stream[F,Option[Connected[F]]] = {
      def refresh: Stream[F, Option[Connected[F]]] = {
        Stream.eval(_refreshTopology(signal)).flatMap{
          _.topics.get((topic, partitionId)).flatMap(_.leader) match {
            case None => time.sleep(1.second) ++ refresh
            case Some(brokerId) => awaitBrokerConnected(signal, brokerId)
          }
        }
      }

      refresh
    }

    /**
      * Builds the control connection to defined broker.
      * It updates itself in the supplied signal when the connection is successful or failed.
      * When the broker is first connected to, this will query initial metadata from the broker.
      *
      * - If the connection succeeds and first metadata are obtained, this will register to connected state
      * - If the connection fails, the broker state is updated, and connection will be retried after given timeout
      * - If the broker is removed from the broker list in state then this stops
      *
      * This connection is also responsible for any publish request for topic and partition
      * where this broker is leader of.
      *
      * @param id               Id of the broker
      * @param address          Address of the broker
      * @param signal           Signal of the state where the broker has to update its state
      * @param brokerConnection A function that will result in tcp connection with broker at specified address
      * @param retryTimeout     If broker fails, this indicates retry timeout for new attempt to connect with broker
      * @param protocol         Protocol to use for this broker connection. Used when generating
      * @param queueBound       Number of messages to keep in the queue for this broker, before holding new requests
      *                         from being submitted.
      * @param clientName       Id of the client
      */
    def controlConnection[F[_]](
      signal: Signal[F,ClientState[F]]
      , brokerConnection: InetSocketAddress => Pipe[F,RequestMessage,ResponseMessage]
      , retryTimeout: FiniteDuration
      , protocol: ProtocolVersion.Value
      , queueBound: Int
      , clientName: String
    )(
      id: Int @@ Broker
      , address: BrokerAddress
    )(implicit F: Async[F], Logger: Logger[F], S:Scheduler):Stream[F,Nothing] = {
      import BrokerState._
      type ControlRequest =
        Either[(MetadataRequest, Async.Ref[F, Option[MetadataResponse]]),(ProduceRequest, Async.Ref[F, Option[ProduceResponse]])]

      /** Checks whether the is still active **/
      def brokerInstanceGone:Stream[F,Boolean] =
        signal.discrete.map(!_.brokers.contains(id))

      /** Creates a function handle produce request for this broker **/
      def produce(enqueue: ((ProduceRequest, Async.Ref[F, Option[ProduceResponse]])) => F[Unit])(req: ProduceRequest): F[Option[ProduceResponse]] =
        F.flatMap(Async.ref[F, Option[ProduceResponse]]){ref =>
        F.flatMap(enqueue((req, ref))){_ =>
          if (req.requiredAcks == RequiredAcks.NoResponse) F.pure(None)
          else ref.get
        }}

      /** Creates a function handle metadata request for this broker **/
      def getMetadata(enqueue: ((MetadataRequest, Async.Ref[F,Option[MetadataResponse]])) => F[Unit]): F[Option[MetadataResponse]] =
        F.flatMap(Async.ref[F, Option[MetadataResponse]]){ref =>
        F.flatMap(signal.get)(cs =>
        F.flatMap(enqueue((MetadataRequest(cs.topics.map(_._1._1).toVector), ref))){ _ =>
          ref.get
        })}

      /**
        * Processes produce response, tries to send again for all failed produces
        *
        * @param ref  The ref that passes through the final response to client
        * @param resp The response that was generated by by this broker
        * @param req  The request to which the response was generated
        */
      def processProduce(
        ref: Async.Ref[F, Option[ProduceResponse]]
        , resp: ProduceResponse
        , req: ProduceRequest
      ): Stream[F, Nothing] = {
        type ResultData = ((String @@ TopicName, Int @@ PartitionId), PartitionProduceResult)

        /** Splits the result data of the response into successful and failed ones **/
        def splitByComplete(
          remaining: Vector[ResultData]
          , completed: Vector[ResultData]
          , needResend: Vector[ResultData]
        ): (Vector[ResultData], Vector[ResultData]) = {
          remaining.headOption match {
            case None => completed -> needResend
            case Some(partitionResult) =>
              if (partitionResult._2.error.isDefined) {
                splitByComplete(remaining.drop(1), completed, needResend :+ partitionResult)
              } else {
                splitByComplete(remaining.drop(1), completed :+ partitionResult, needResend)
              }
          }
        }

        /** Re-sends given data to their respective new leaders **/
        def doResend(resend: Vector[ResultData]): Stream[F, Option[ProduceResponse]] = {
          val requests = resend.flatMap{case ((topic, partition), _) =>
            req.messages.find(_._1 == topic)
            .flatMap(_._2.find(_._1 == partition)).map{ case (_, messages) =>
              ((topic, partition), messages)
            }
          }

          _doResend(requests)
        }

        /**
          * Implementation of resend, awaits connected current leader for topic and partition
          * then it sends new requests to those brokers
          */
        def _doResend(requests: Vector[((String @@ TopicName, Int @@ PartitionId), Vector[Message])]): Stream[F, Option[ProduceResponse]] = {
          Stream.eval_(_refreshTopology(signal)) ++
          sendToLeaders(signal, requests){
            case (conn, data) =>
              val messageData =
                data.groupBy(_._1._1)
                .map{case (topic, partitions) =>
                  topic -> partitions.map{case ((_, partition), messages) => partition -> messages }
                }

              val request =
                ProduceRequest(req.requiredAcks, req.timeout, messageData.toVector)

              conn.produce(request)
          }
        }

        val sorted =
          resp.data.flatMap{case (topic, partitions) =>
            partitions.map{case (partition, resp)  => (topic, partition) -> resp}
          }.toMap.toVector

        Stream.emit(splitByComplete(sorted, Vector(), Vector()))
        .flatMap{case (completed, needResend) =>
          if (needResend.nonEmpty) {
            doResend(needResend)
            .fold(Vector.empty[ProduceResponse]){
              case (acc, nResp) => acc ++ nResp.toVector
            }.evalMap{resps =>
              ref.setPure(
                Some(resp.copy(
                  data = resp.data ++ resps.flatMap(_.data)
                ))
              )
            }.drain
          } else {
            Stream.eval_(ref.setPure(Some(resp)))
          }
        }
      }

      /**
        * Processes response from the server (produce and metadata)
        * Fetches the request for this response, then removes this request
        * from the opened requests, after that we pass the response
        */
      def processResponse(
        open: Signal[F, Map[Int,ControlRequest]]
      )(resp: ResponseMessage)(implicit F: Async[F]): Stream[F, Nothing] = {
        Stream.eval(open.get).flatMap{opened =>
          Stream.eval_(open.modify(_ - resp.correlationId))
          .map(_ => opened)
        }
        .map(_.get(resp.correlationId))
        .flatMap{
          case None => Stream.empty
          case Some(cReq) =>
            cReq match {
              case Left((_, ref)) =>
                resp.response match {
                  case resp: MetadataResponse => Stream.eval_(ref.setPure(Some(resp)))
                  case _ => Stream.eval_(ref.setPure(None))
                }

              case Right((req, ref)) =>
                resp.response match {
                  case resp: ProduceResponse =>
                    processProduce(ref, resp, req)

                  case _ => Stream.eval_(ref.setPure(None))
                }
            }
        }
      }

      /** Sets the state of this broker to connected **/
      def updateConnected(incomingQ: Queue[F, ControlRequest]): Stream[F, Nothing] = {
        Stream.eval_{
          signal.modify{state =>
            state.copy(brokers =
              state.brokers + (id ->
                Connected(
                  id
                  , address
                  , getMetadata(incomingQ.enqueue1 _ compose(Left(_)))
                  , produce(incomingQ.enqueue1 _ compose(Right(_)))
                  , 0
                )
              )
            )
          }
        }
      }

      Stream.eval(async.boundedQueue[F,ControlRequest](queueBound)).flatMap { incomingQ =>
      Stream.eval(async.signalOf(Map[Int,ControlRequest]())).flatMap { open =>
        val clientId = clientName + "-control"

        /** Connects the broker and processes requests **/
        def connectAndProcess:Stream[F, Nothing] = {
          val idx = new AtomicInteger(0)
          Stream.eval(
            F.delay(brokerConnection(new InetSocketAddress(address.host, address.port)))
          ).flatMap{ connection =>
            updateConnected(incomingQ).flatMap{_ =>
              incomingQ.dequeue.flatMap{ req =>
                val rIdx = idx.getAndIncrement()
                (if (req.fold(_ => false, _._1.requiredAcks == RequiredAcks.NoResponse)) {
                  Stream.empty
                } else {
                  Stream.eval_(open.modify(_ + (rIdx -> req)))
                }) ++
                  Stream.emit(RequestMessage(protocol, rIdx, clientId, req.fold(_._1, _._1)))
              }.through(connection)
            }
          }.flatMap{processResponse(open)}
        }

        /** Sets the state of this broker to Failed or increments number of tries **/
        def updateError(err: Throwable):Stream[F, Failed[F]] = {
          Stream.eval_(
            signal.modify{cs =>
              val failed =
                cs.brokers.get(id).map{
                  case failed: Failed[F] => failed.failures + 1
                  case _ => 1
                }.getOrElse(1)

              cs.copy(brokers = cs.brokers + (id -> Failed(id, address, LocalDate.now(), failed)))
            }
          )
        }

        /** Waits a retry timeout incremented by every consecutive failure**/
        def sleepRetry(f: Failed[F]):Stream[F,Nothing] = time.sleep(retryTimeout * f.failures)

        /** Fails all opened requests against this broker **/
        def failOpened(f: Failed[F]):Stream[F, Nothing] = {
          Stream.eval(open.get).flatMap{rqs => Stream.emits(rqs.values.toSeq)}
          .flatMap(rq => Stream.eval_(rq.fold(_._2.setPure(None), _._2.setPure(None))))
        }

        def go:Stream[F,Nothing] = {
          connectAndProcess
          .onError { err =>
            Logger.error_(s"Connection with broker failed. Broker: $id, address: $address", thrown = err) ++
            updateError(err).flatMap( err =>
              // fail all and await signal for retry,
              failOpened(err).mergeDrainL(sleepRetry(err)) // this will terminate when both streams terminate.
            ) ++ go
          }
        }

        go
      }.interruptWhen(brokerInstanceGone)
    }}


    /**
      * Whenever new metadata is received from any broker, this is used to update the state to new topology
      * received from the broker.
      *
      * @param s      Current state of the client
      * @param meta   Received metatdata
      */
    def updateMetadata[F[_]](
      s: ClientState[F]
      , meta:MetadataResponse
    ): ClientState[F] = {
      val gen = tag[BrokerGeneration](s.generation + 1)
      val updatedBrokers = updateBrokers(s.brokers, meta.brokers, gen)
      val updatedTopics = updateTopics(updatedBrokers,s.topics,meta.topics)
      s.copy(brokers = updatedBrokers, topics = updatedTopics, generation = gen)
    }

    /**
      * With updated list of brokers, this will update them in the state.
      *  - Brokers that does not exists in new update are removed
      *  - Brokers that are not known in state, they are added to the state with `Connecting` initial state.
      *
      * @param brokers  Current brokers
      * @param updated  Next updated brokers.
      */
    def updateBrokers[F[_]](
     brokers: Map[Int @@ Broker, BrokerState[F]]
     , updated: Vector[Broker]
     , generation: Long @@ BrokerGeneration
    ): Map[Int @@ Broker, BrokerState[F]] = {
      val um = updated.map { b => b.nodeId -> b }.toMap
      val added = um.keySet diff brokers.keySet
      val removed = brokers.keySet diff um.keySet
      val newBrokers = added.toSeq.flatMap { brokerId =>
        um.get(brokerId).toSeq.map { broker =>
          brokerId -> BrokerState.Connecting[F](brokerId, BrokerAddress(broker.host, broker.port, generation))
        }
      }
      (brokers -- removed) ++ newBrokers
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
     , topics: Map[TopicAndPartition, TopicAndPartitionState[F]]
     , updated: Vector[TopicMetadata]
     ): Map[TopicAndPartition, TopicAndPartitionState[F]] = {
      val updatedData = updated.flatMap { tm =>
        if (tm.error.nonEmpty) Vector.empty
        else tm.partitions.map { pm => (tm.name, pm.id) -> pm }
      }.toMap

      val added = (updatedData.keySet diff topics.keySet).flatMap { case tap@(topicId, partitionId) =>
        updatedData.get(tap).map { pm =>
          tap -> TopicAndPartitionState(
            topic = topicId
            , partition = partitionId
            , leader = pm.leader.filter { brokers.isDefinedAt }
            , followers = pm.isr.filter { brokers.isDefinedAt }.map { brokerId => brokerId -> FollowerState.Operational[F](brokerId) }.toMap
          )
        }
      }

      topics.flatMap { case (tap, state) =>
        updatedData.get(tap).filter(_.error.isEmpty).map { pm =>
          val newFollowers = (pm.isr.toSet diff state.followers.keySet).filter(brokers.isDefinedAt).map { brokerId =>
            brokerId -> FollowerState.Operational[F](brokerId)
          }.toMap

          tap -> state.copy(
            leader = pm.leader.filter { brokers.isDefinedAt }
            , followers = state.followers.filter { case (brokerId,_) => pm.isr.contains(brokerId) && brokers.isDefinedAt(brokerId) } ++ newFollowers
          )
        }.toMap
      } ++ added
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
    )(implicit S:Scheduler, F:Async[F], Logger:Logger[F]):Stream[F,Nothing] = {
      import BrokerState._

       time.awakeEvery(refreshInterval).flatMap { _ =>
         Stream.eval(signal.get).flatMap { s =>
           val queryFromConnected:Stream[F,Stream[F,MetadataResponse]] =
             Stream.emits(
               s.brokers.values.collect { case Connected(brokerId,address,getMetadata,_,_) =>
                 Stream.eval(getMetadata).attempt.flatMap {
                   case Left(rsn) => Logger.error_(s"Failed to query metadata from broker $brokerId ($address)", thrown = rsn)
                   case Right(maybeMetadataResponse) => Stream.emits(maybeMetadataResponse.toSeq)
                 }
               }.toSeq
             )

           /*
            * We assume only last response contains most up to date data.
            * This may not be correct assumption, but likely the cluster will be synced at next state
            * or whenever produce request will be directed to incorrect leader.
            *
            * Metadata requests serves also like sort of a health check on all the connected brokers.
            */
           Stream.eval(concurrent.join(Int.MaxValue)(queryFromConnected).runLog).flatMap { meta =>
             if (meta.isEmpty) Stream.eval(fromSeeds).flatMap(r => Stream.emits(r.toSeq))
             else Stream.emits(meta)
           }
           .last
           .flatMap {
             case None =>
               Logger.error_("No single member or seed was able to respond with metadata, state of all members will be cleaned")
               .append(Stream.emit(MetadataResponse(Vector.empty, Vector.empty)))
             case Some(resp) => Stream.emit(resp)
           }.flatMap { meta =>
             Stream.eval(signal.modify(updateMetadata(_, meta))).flatMap { change =>
               if (change.previous == change.now ) Stream.empty
               else logStateChanges(change.now, meta)
             }
           }
         }
       }
    }

    def logStateChanges[F[_]](s:ClientState[F], meta:MetadataResponse)(implicit Logger:Logger[F]):Stream[F,Nothing] = {
      Logger.debug_(s"Updated client state with received metadata. New state is $s. Metadata: $meta")
    }
  }
}
