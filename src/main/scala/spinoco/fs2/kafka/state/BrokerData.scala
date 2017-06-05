package spinoco.fs2.kafka.state


import java.net.InetSocketAddress

import fs2.util.Effect
import shapeless.tag._
import spinoco.protocol.kafka.Broker


/**
  * Represents state of the broker.
  */
case class  BrokerData(
  brokerId: Int @@ Broker
  , address: BrokerAddress
)



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

