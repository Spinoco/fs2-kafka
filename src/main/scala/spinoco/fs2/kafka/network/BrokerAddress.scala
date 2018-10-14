package spinoco.fs2.kafka.network

import java.net.InetSocketAddress

import cats.Show
import cats.effect.Sync


/**
  * Address and generation of the broker
  * @param host           Name of the broker
  * @param port           Port of the broker
  */
case class BrokerAddress(
  host: String
  , port: Int
)  { self =>

  def toInetSocketAddress[F[_] : Sync]: F[InetSocketAddress] =
    Sync[F].catchNonFatal { new InetSocketAddress(self.host, self.port) }

}


object BrokerAddress {

  implicit val showInstance: Show[BrokerAddress] =
    Show.show { addr => s"${addr.host}:${addr.port}" }

}



