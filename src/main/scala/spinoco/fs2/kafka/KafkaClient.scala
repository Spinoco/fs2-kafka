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
import spinoco.protocol.kafka.Request.{MetadataRequest, ProduceRequest, RequiredAcks}
import spinoco.protocol.kafka._
import spinoco.protocol.kafka.Response.{MetadataResponse, PartitionProduceResult, ProduceResponse}

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
            , impl.metadataRefresher(impl.initialMetadata(ensemble,protocol,clientName,brokerConnection), state, 1.minute)
            , Stream.eval(impl.mkClient(state))
          ))
        }
    }}
  }



  protected[kafka] object impl {


    def mkClient[F[_]](
      signal: Signal[F,ClientState[F]]
    )(implicit F: Async[F], S:Scheduler):F[KafkaClient[F]] = F.delay {
      new KafkaClient[F] {
        def subscribe(topic: String, partition: Int, offset: Long, maxQueue: Int, followTail: Boolean): Stream[F, TopicMessage] = ???
        def publish1(topic: String, partition: Int, key: Bytes, message: Bytes, requireQuorum: Boolean, serverAckTimeout: FiniteDuration): F[Option[Long]] = _publish1(signal, tag[TopicName](topic), tag[PartitionId](partition), key, message, requireQuorum, serverAckTimeout)
        def publishUnsafe1(topic: String, partition: Int, key: Bytes, message: Bytes): F[Unit] = _publishUnsafe1(signal, tag[TopicName](topic), tag[PartitionId](partition), key, message)
        def publishN[A](messages: Chunk[(String, Int, Bytes, Bytes, A)], requireQuorum: Boolean, serverAckTimeout: FiniteDuration, compress: Option[Compression.Value]): F[Chunk[(A, Long)]] = _publishN(messages, compress, requireQuorum, serverAckTimeout, signal)
        def publishUnsafeN(messages: Chunk[(String, Int, Bytes, Bytes)], compress: Option[Compression.Value]): F[Unit] = _publishUnsafeN(messages, compress, signal)
        def topics: Stream[F, Map[String, Set[Int]]] = signal.discrete.map(_.topics.groupBy(_._1._1).map{case (topic, partitions) => (topic:String) -> partitions.keySet.map(_._2:Int)})
        def refreshTopology: F[Unit] = F.map(_refreshTopology(signal)){_ => ()}
      }
    }


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
              offset = 0 //This is ignored for producer of single message
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

    def createMessages(messages: Chunk[(String, Int, Bytes, Bytes)], compression: Option[Compression.Value]): Vector[((String @@ TopicName, Int @@ PartitionId), Vector[Message])] = {
      def makeSingles(messages: Vector[(String, Int, Bytes, Bytes)]): Vector[Message] = {
        messages.map{ case (_, _, key, message) =>
          SingleMessage(
            offset = 0 //This is ignored for producer of single message
            , version = MessageVersion.V0
            , timeStamp = None //None for producer
            , key = ByteVector(key.values)
            , value = ByteVector(message.values)
          )
        }
      }

      def makeCompressed(messages: Vector[(String, Int, Bytes, Bytes)])(compression: Compression.Value): CompressedMessages = {
        CompressedMessages(
          offset = 0
          , version = MessageVersion.V0
          , compression = compression
          , timeStamp = None
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

    def sendToLeaders[F[_], A, B](
      signal: Signal[F, ClientState[F]]
      , msges: Vector[((String @@ TopicName,Int @@ PartitionId), Vector[A])]
    )(
      doAction: (Connected[F], Vector[((String @@ TopicName, Int @@ PartitionId), Vector[A])]) => F[B]
    )(implicit F: Async[F], S:Scheduler): Stream[F, B] = {
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
                  sendToLeaders(signal, data.map{case (_, key, messages) => key -> messages})(doAction)

                case Some(conn) =>
                  Stream.eval(doAction(conn, data.map{case (_, key, messages) => key -> messages}))
              }

            case (None, data) =>
              time.sleep(1.second) ++
                Stream.eval_(_refreshTopology(signal)) ++
                sendToLeaders(signal, data.map{case (_, key, messages) => key -> messages})(doAction)
          }
        }

      }
    }


    def getLeaderFor[F[_]](
      signal: Signal[F,ClientState[F]]
      , topic: String @@ TopicName
      , partitionId: Int @@ PartitionId
    )(implicit F: Async[F], S:Scheduler): Stream[F,Option[Connected[F]]] = {
      Stream.eval(signal.get)
      .flatMap{_getLeaderFor(signal, topic, partitionId)}
    }

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

      def brokerInstanceGone:Stream[F,Boolean] =
        signal.discrete.map(!_.brokers.contains(id))

      def produce(enqueue: ((ProduceRequest, Async.Ref[F, Option[ProduceResponse]])) => F[Unit])(req: ProduceRequest): F[Option[ProduceResponse]] =
        F.flatMap(Async.ref[F, Option[ProduceResponse]]){ref =>
        F.flatMap(enqueue((req, ref))){_ =>
          if (req.requiredAcks == RequiredAcks.NoResponse) F.pure(None)
          else ref.get
        }}

      def getMetadata(enqueue: ((MetadataRequest, Async.Ref[F,Option[MetadataResponse]])) => F[Unit]): F[Option[MetadataResponse]] =
        F.flatMap(Async.ref[F, Option[MetadataResponse]]){ref =>
        F.flatMap(signal.get)(cs =>
        F.flatMap(enqueue((MetadataRequest(cs.topics.map(_._1._1).toVector), ref))){ _ =>
          ref.get
        })}

      def processMultiple(
        ref: Async.Ref[F, Option[ProduceResponse]]
        , resp: ProduceResponse
        , req: ProduceRequest
      ): Stream[F, Nothing] = {
        type ResultData = ((String @@ TopicName, Int @@ PartitionId), PartitionProduceResult)

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

        def doResend(resend: Vector[ResultData]): Stream[F, Option[ProduceResponse]] = {
          val requests = resend.flatMap{case ((topic, partition), _) =>
            req.messages.find(_._1 == topic)
            .flatMap(_._2.find(_._1 == partition)).map{ case (_, messages) =>
              ((topic, partition), messages)
            }
          }

          _doResend(requests)
        }

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

      def processResponse(
        open: Signal[F, Map[Int,ControlRequest]]
      )(resp: ResponseMessage)(implicit F: Async[F]): Stream[F, Nothing] = {
        Stream.eval(open.get)
        .map(_.get(resp.correlationId))
        .flatMap{
          case None => Stream.empty
          case Some(cReq) =>
            cReq match {
              case Left((_, ref)) =>
                resp.response match {
                  case resp: MetadataResponse => Stream.eval_(ref.setPure(Some(resp)))
                  case _ => Stream.empty
                }

              case Right((req, ref)) =>
                resp.response match {
                  case resp: ProduceResponse =>
                    processMultiple(ref, resp, req)

                  case _ => Stream.empty
                }
            }

        }
      }

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

        def connectAndProcess:Stream[F,Nothing] = {
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

        def updateError(err:Throwable):Stream[F,Failed[F]] = {
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

        def sleepRetry(f:Failed[F]):Stream[F,Nothing] = time.sleep(retryTimeout)

        def failOpened(f:Failed[F]):Stream[F,Nothing] = {
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





