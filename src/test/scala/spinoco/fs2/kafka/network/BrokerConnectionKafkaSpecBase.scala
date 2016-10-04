package spinoco.fs2.kafka.network

import spinoco.fs2.kafka.Fs2KafkaRuntimeSpec
import fs2._
import shapeless.tag
import shapeless.tag.@@
import spinoco.fs2.kafka.DockerSupport.{DockerId, _}
import spinoco.fs2.kafka.Fs2KafkaRuntimeSpec._
import spinoco.protocol.kafka.{PartitionId, TopicName}

/**
  * Created by pach on 03/09/16.
  */
class BrokerConnectionKafkaSpecBase extends Fs2KafkaRuntimeSpec {

  object KafkaRuntimeRelease extends Enumeration {
    val V_8_2_0 = Value
    val V_0_9_0_1 = Value
    val V_0_10_0 = Value
  }

  val testTopic1 = tag[TopicName]("test-topic-1")
  val testTopic2 = tag[TopicName]("test-topic-2")
  val part0 = tag[PartitionId](0)


  /** process emitting once docker id of zk and kafka in singleton (one node) **/
  def withKafka8Singleton(version:KafkaRuntimeRelease.Value):Stream[Task,(String @@ DockerId, String @@ DockerId)] = {
    def startK:Task[String @@ DockerId] = {
      version match {
        case KafkaRuntimeRelease.V_8_2_0 => startKafka(Kafka8Image, port=9092)
        case KafkaRuntimeRelease.V_0_9_0_1 => startKafka(Kafka9Image, port = 9092)
        case KafkaRuntimeRelease.V_0_10_0 => startKafka(Kafka10Image, port = 9092)
      }
    }

    def awaitZKStarted(zkId: String @@ DockerId):Stream[Task,Nothing] = {
      followImageLog(zkId).takeWhile(! _.contains("binding to port")).map(println).drain
    }

    def awaitKStarted(kafkaId: String @@ DockerId):Stream[Task,Nothing] = {
      version match {
        case KafkaRuntimeRelease.V_8_2_0 =>
          followImageLog(kafkaId).takeWhile(! _.contains("New leader is "))
          .map(println).drain

        case KafkaRuntimeRelease.V_0_9_0_1 =>
          followImageLog(kafkaId).takeWhile(! _.contains("New leader is "))
            .map(println).drain

        case KafkaRuntimeRelease.V_0_10_0 =>
          followImageLog(kafkaId).takeWhile(! _.contains("New leader is "))
            .map(println).drain
      }
    }


    Stream.bracket(startZk())(
      zkId => awaitZKStarted(zkId) ++ Stream.bracket(startK)(
        kafkaId => awaitKStarted(kafkaId) ++ Stream.emit(zkId -> kafkaId)
        , stopImage
      )
      , stopImage
    )

  }




}
