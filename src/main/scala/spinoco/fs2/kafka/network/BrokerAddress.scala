package spinoco.fs2.kafka.network

import com.comcast.ip4s.{Host, Port, SocketAddress}


/**
  * Address and generation of the broker
  * @param host           Name of the broker
  * @param port           Port of the broker
  */
case class BrokerAddress(
  host: String
  , port: Int
)  { self =>

  def toSocketAddress: Either[String, SocketAddress[Host]] =
    for {
      host <- Host.fromString(self.host).toRight(s"Invalid host: ${self.host}")
      port <- Port.fromInt(self.port).toRight(s"Invalid port: ${self.port}")
    } yield SocketAddress(host, port)

}



