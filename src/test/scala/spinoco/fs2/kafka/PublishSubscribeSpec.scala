package spinoco.fs2.kafka




/**
  * Created by pach on 21/05/16.
  */
class PublishSubscribeSpec  extends Fs2KafkaClientSpec {


  "Publish And Subscribe" - {

    "on tail, will receive messages as they are published" in {
//      cluster(3).flatMap { case (zkPort,brokers) =>
//
//        println(("XXXY", brokers))
//
//        val zkUtils = ZkUtils(s"127.0.0.1:$zkPort", 3000, 3000, false)
//        val options = new TopicCommandOptions(Array(
//          "--topic", TestTopic
//          , "--partitions", "1"
//          , "--replication-factor", "3"
//        ))
//        TopicCommand.createTopic(zkUtils,options)
//
//        time.sleep[Task](10.seconds) ++
//        spinoco.fs2.kafka.client[Task](brokers.values.map(_.bindAddress).toSet)
//        .flatMap { client =>
//          time.sleep[Task](10.seconds) ++
//          eval(client.publish1(TestTopic, 0, Chunk.bytes(Array(1,2,3)), Chunk.bytes(Array(10,11,12)), None, 3.seconds, None)).map {
//            r => println(s"XXXG GOT RESPONSE: $r")
//          } ++
//          time.sleep[Task](10.seconds)
//        }
//
//
//      }.run.unsafeRun


    }

  }


}
