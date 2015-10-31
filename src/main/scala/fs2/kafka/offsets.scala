package fs2.kafka



case class OffsetQuery(replica: Int, topic: Map[String, Map[Int, PartitionOffsetQuery]])

sealed trait PartitionOffsetQuery

object PartitionOffsetQuery {

  case object Latest extends PartitionOffsetQuery

  case class Before(time: Long) extends PartitionOffsetQuery

  case object Earliest extends PartitionOffsetQuery

}


case class OffsetQueryResponse(topic: Map[String, Map[Int, Either[ErrorCode.Value, Seq[Long]]]])