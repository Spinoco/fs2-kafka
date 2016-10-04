package spinoco.fs2.kafka.state


import java.time.LocalDate


import shapeless.tag._
import spinoco.protocol.kafka.Request.ProduceRequest
import spinoco.protocol.kafka.Response.{MetadataResponse, ProduceResponse}
import spinoco.protocol.kafka.Broker


/**
  * Represents state of the broker.
  */
sealed trait BrokerState[F[_]] {
  def brokerId: Int @@ Broker
  def address: BrokerAddress
}


object BrokerState {

  /**
    * Broker is fully operational,
    *
    * @param brokerId       Id of the broker
    * @param address        Remote address of the broker where tcp connections shall be established
    * @param getMetadata    Function, that when invoked will return response for given metadata request.
    *                       None indicates that connection is no longer established
    * @param produce        Function, that for given produce request will return produce response.
    *                       None indicates connection is no longer established.
    * @param connections    Number of established connections currently alive excluding the control connection.
    * @tparam F
    */
  case class Connected[F[_]](
    brokerId: Int @@ Broker
    , address: BrokerAddress
    , getMetadata: F[Option[MetadataResponse]]
    , produce: ProduceRequest => F[Option[ProduceResponse]]
    , connections: Int

  ) extends BrokerState[F]

  /**
    * Broker is just connecting its first control connection, but connection is not yet established
    * @param brokerId     Id of the broker
    * @param address      Remote address where the broker establishes its connection
    */
  case class Connecting[F[_]](
   brokerId: Int @@ Broker
   , address: BrokerAddress
  ) extends BrokerState[F]

  /**
    * Broker has failed to establish its control connection.
    * We may completely fine still operate, as the other brokers will likely be able to handle
    * requests. However we are attempting to establish connection to this broker with exponential
    * back-off mechanism.
    * @param brokerId         Id of the broker
    * @param address          Address of the broker
    * @param lastFailureAt    When the broker lastly failed
    * @param failures         Total number of failures since first failure
    */
  case class Failed[F[_]](
    brokerId: Int @@ Broker
    , address: BrokerAddress
    , lastFailureAt: LocalDate
    , failures: Int
   ) extends BrokerState[F]





}

/**
  * Address and generation of the broker
  * @param host           Name of the broker
  * @param port           Port of the broker
  * @param generation     Generation of the connection to this broker. This number is always refreshed when
  *                       Broker appears in configuration
  */
case class BrokerAddress(
  host:String
  , port:Int
  , generation: Long @@ BrokerGeneration
)


sealed trait BrokerGeneration

