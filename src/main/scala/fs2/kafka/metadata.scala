package fs2.kafka


case class Broker(id: Int, host: String, port: Int)

case class TopicMetadata(partitions: Map[Int, Either[ErrorCode.Value, PartitionMetadata]])

case class PartitionMetadata(leader: Int, replicas: Seq[Int], isr: Seq[Int])

case class MetadataResponse(brokers:Seq[Broker], topics:Map[String,TopicMetadata])