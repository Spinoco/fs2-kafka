package spinoco.fs2.kafka.state


import java.net.InetSocketAddress

import fs2.util.Effect
import shapeless.tag._
import spinoco.protocol.kafka.Request.ProduceRequest
import spinoco.protocol.kafka.Response.{MetadataResponse, ProduceResponse}
import spinoco.protocol.kafka.Broker


/**
  * Represents state of the broker.
  */
sealed trait BrokerState[F[_]] { self =>
  def brokerId: Int @@ Broker
  def address: BrokerAddress

}


object BrokerState {

  /**
    * Broker has publish connections for certain topics active.
    *
    * Each topic is having dediated connection ot broker. However partitions in same topic share connection to broker,
    * if that broker is leader for these partitions.
    *
    * When broker lost leadership, then any publish to that broker's partition will fail.
    *
    *
    * @param brokerId       Id of the broker
    * @param address        Remote address of the broker where tcp connections shall be established
    * @param getMetadata    Function, that when invoked will return response for given metadata request.
    *                       None indicates that connection is no longer established
    * @param produce        Function, that for given produce request will return produce response.
    *                       None indicates connection is no longer established.
    * @tparam F
    */
  case class Connected[F[_]](
    brokerId: Int @@ Broker
    , address: BrokerAddress
    , getMetadata: F[Option[MetadataResponse]]
    , produce: ProduceRequest => F[Option[ProduceResponse]]
  ) extends BrokerState[F]

  /**
    * Broker has no pending publish connections.
    *
    * @param brokerId     Id of the broker
    * @param address      Remote address where the broker establishes its connection
    */
  case class Ready[F[_]](
   brokerId: Int @@ Broker
   , address: BrokerAddress
  ) extends BrokerState[F]

}

/**
  * Address and generation of the broker
  * @param host           Name of the broker
  * @param port           Port of the broker
  */
case class BrokerAddress(
  host: String
  , port: Int
)  { self =>

  def toInetSocketAddress[F[_]](implicit F: Effect[F]): F[InetSocketAddress] =
    F.delay { new InetSocketAddress(self.host, self.port) }

}


sealed trait BrokerGeneration

