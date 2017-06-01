package spinoco.fs2.kafka.network

import spinoco.fs2.kafka.Fs2KafkaRuntimeSpec
import shapeless.tag
import spinoco.protocol.kafka.TopicName

/**
  * Created by pach on 03/09/16.
  */
class BrokerConnectionKafkaSpecBase extends Fs2KafkaRuntimeSpec {

  val testTopic1 = tag[TopicName]("test-topic-1")
  val testTopic2 = tag[TopicName]("test-topic-2")

}
