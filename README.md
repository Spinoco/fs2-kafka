# fs2-kafka
Kafka client implemented with [fs2](https://github.com/scalaz/scalaz-stream) library
 
## Overview 

fs2-kafka is simple client for consuming and publishing messages from / top Apache Kafka. It has minimalistic functionality and is fully implemented w/o any depenednecy on kafka driver itself. It is fully asynchronous, nonblocking and streaming. 

Thanks to fs2, it comes with full backpressure and streaming support out of the box, and plays well with other fs2 libraries such as [fs2-http](https://github.com/Spinoco/fs2-http). 

The fs2-kafka was build with mninimal depenedencies (apart from fs2 only scodec is used). 

fs2-kafka only support subset of features compare to original kafka client. There is for example no support for creating topic, or for Kafka Connect / Kafka Streams. Reasons for it is that we wanted client to be as simple as possible leaving all non-essesntial functionality to other solutions. 



## Features

- Subscribe to topic / parition, with confiugrable pre-fetch behaviour.
- Publish to topic / partition 
- Query metadata from kafka cluster
- Query offset range for topic / partition 

For the compression of messages fs2-kafka supports GZIP and Snappy compression alghrothms. 

fs2-kafka supports versions since 0.8.2 until 0.10.2 of kafka cluster with protocols. The protocols are crosstested against the different versions of kafka (i.e. 0.10.2 is tested to wrok ok with 0.8.2, 0.9.x, 0.10, 0.10.1 and 0.10.2 protocols). 

## SBT

Add this to your build file

```
libraryDependencies += "com.spinoco" %% "fs2-kafka" % "0.1.0" 
```

## Dependencies

version  |    scala  |   fs2  |  scodec | shapeless      
---------|-----------|--------|---------|-----------
0.1.1    | 2.11, 2.12| 0.9.7  | 1.10.3  | 2.3.2 


## Usage 

Through this simple usage guide please consider having following imports on your classpath:

```scala
import fs2.util.syntax._
import spinoco.fs2.kafka
import spinoco.fs2.kafka._
import spinoco.protocol.kafka._
import scala.concurrent.duration._
```
If you type console from fs2-kafka's project sbt, these imports are added for you automatically. 

### Basics

Client can be used with any `F` (effectfull type) that confroms to `Async[F]`. As a reference throuhg the guide we will use fs2.Task. 

To obtain Kafka client this code can be used :

```scala

kafka.client(
  ensemble = Set(broker("kafka-broker1-dns-name", port = 9092))
  , protocol = ProtocolVersion.Kafka_0_10_2
  , clientName = "my-client-name"
) flatMap { kafkaClient =>
  /* your code using Kafka Client **/
  ???
}

```

Note that resulting type of this program is Stream[F, ???], that means stream needs to be run, and once it finishes the kafka client will terminate. 


### Publishing to topics

fs2-kafka has 4 possible ways to publish to topic. 

- publish1 - publishes one message to topic and partition and awaits confirmation from leader 
- publishN - publishes chunk of messages to topic / partition and awaits confrimtation from leader 

- publish1Unsafe - like publish1, except this won't confirm that publish was susccessfull 
- publishNUnsafe - like publishN, except this won't confirm that publish was susccessfull 

First two methods (`publish1` and `publishN`) allows to publish messages safely, that means once the message is published, the result contains the offset of first succesfully published message. Each of these two methods have two important parameters that has to be specified: 

- `requireQuorum` : When set to true, this indicates that quorum (majority) of ISR (In sync replicas) must aknowledge the publish operation in order for it to be succesfull. When set to `false` only leader must acknowledge the publish. 
- `serverAckTimeout` : A timeout at server (broker) to confirm the message publish operation. Note that this timeout is only relevant when publish is sent to kafka broker. There is no timeout locally within the client, so for example if there are no brokers available, message will be queued up until the leader will become avaialble. That local timeout is subject to be implemented with `F` (see examples below). 

The second methods to publish to topic (`publish1Unsafe` and `publishNUnsafe`) allows to publish w/o confirmation from the server. Locally, there is only minimal verification that publish was succesfull, essentially once the leader is avaiailble and connection with leader has been established, the publish is considered to be successfull. 

Please note there is a difference from how kafka native client behaves when publishing. Kafka client retries (3x) when there was unsuccesfull publish. fs2-kafka instead fails as soon as possible and leaves up to user to choose from any retry mechanics. 

### Examples of publishing to topic: 

```scala

/// assuming val kafkaClient: KafkaClient = ... 

// publishes safely 1 message to topic-A partition 0, with Key 1, and value [1, 2, 3]  
// returns when the publish was accepted by majority (quorum) of the servers. Fails, when server won't get acknowledgements from ISRs in 10s or less. 
kafkaClient.publish1(topic("topic-A"), partition(0), ByteVector(1), ByteVector(1,2,3), requireQuorum = true, serverAckTimeout = 10 seconds)

// consumes stream of key, value pairs and publishes them in chunks to broker. Note that this publishes the stream of messages in chunks utilitizing the kafka batch processing.   
// returns when the publish was accepted by majority (quorum) of the servers. Fails, when server won't get acknowledgements from ISRs in 10s or less. 
// Additionally the chunks are not compressed, but GZIP or Snappy can be plugged in by specifying the compress attribute. 
// The resulting stream will contain starting offset of very first message in chunk. 
val streamOfValues: Stream[Task, (ByteVector, ByteVector)] = ???
streamOfValues.chunk.evalMap {
  kafkaClient.publishN(topic("topic-A"), partition(0), requireQuorum = true, serverAckTimeout = 10 seconds, compress = None)
}

// publishes s1 message to topic-A partition 0, with Key 1, and value [1, 2, 3]  
// returns immediatelly, when there is leader known for topic/ partition 
kafkaClient.publish1Unsafe(topic("topic-A"), partition(0), ByteVector(1), ByteVector(1,2,3))

// consumes stream of key, value pairs and publishes them in chunks to broker. Note that this publishes the stream of messages in chunks utilitizing the kafka batch processing.   
// returns immediatelly. 
// Additionally the chunks are not compressed, but GZIP or Snappy can be plugged in by specifying the compress attribute. 
val streamOfValues: Stream[Task, (ByteVector, ByteVector)] = ???
streamOfValues.chunk.evalMap {
  kafkaClient.publishNUnsafe(topic("topic-A"), partition(0),compress = None)
}

```

### Querying kafka for available offsets

In some scenarios, it is usefull to know kafka first and last offset at any given time. As you will see below, the fs2-kafka makes available the "tail" of the topic with every message (tail - last message in topic). However you may need to know available range before you will start consuming the topic. 

fs2-kafka has `offsetRangeFor` api for that purpose. When evaluated, this will return offset of first and next message to be published in every topic/partition. When the offsets are equal, that indicates the topic/partition is empty. 

exmaple: 

```scala 

// Queries first and next-to-be-published offset of messages in topic `topic-A` partition 0
kafkaClient.offsetRangeFor(topic("topic-A"), partition(0)) flatMap { case (first, next) =>
   ???
}

```

### Subscription

fs2-kafka client has only one method of subscriptions for the topic. Subscription to topic is utilizing `subscribe` method with few paramteres that can finetune its exact behaviour. 

Subscription to kafka topic always subscribes with first message specified in `offset` parameter and always awaits next messages to be published to the topic (won't terminate when there are no more messages in topic available). That behaviour in combination with `offsetRangeFor` shall give user posibility to express any subscription pattern necessary. 

### Subscribe at invalid offset range 

fs2-kafka client is able to recover itself when there is invalid offset specified for `offset` parameter. The rules to recover from invalid offset are: 

 - When the offset is lower than offset of first message available in the topic, then, the subscription will start at very first message available
 - When the offset is greater than offset of last message availble in the topic, then, the subscription will start at very next message arriving to topic since the stream was evaluated. 
 
 
### Error Recovery

fs2-kafka client may recover from broker failures. It also allows the recovery to be handled externally by user logic, however simple machanics have been built in. 

Client's recovery of `subscribe` method is controlled by `leaderFailureTimeout` and `leaderFailureMaxAttempts` parameters. Default behaviour is to allow for three consecutive leader failures and sleep for 5s between these failures before giving up and failing the subscription. When user wants to control failure recoveryy manually, please use `0 millis` for `leaderFailureTimeout` and `0` for `leaderFailureMaxAttempts` parameters, causing subscription to fail at any error (except for invalid offsets, that are handled always).

fs2-kafka client `subscribe` will recover from any failures, and will start next subscription from the next offset to last received message. 

### Further subscription fine-tuning 

fs2-kafka subscription allows to further finetune behaviour of the subscriptions. When subscribing the fs2-kafka client may receive more messages in single chunk, and in that case fs2-kafka fully utiliizes "chunk" optimozation of fs2 library. Furthermore following parameters may control the behaviour of subscription : 

- `minChunkByteSize` - Controls how much bytes of new messages must be ready at kafka leader in order to complete single fetch chunk. If there is less than that amout of bytes, the susbcriber will wait up to that amount of bytes before emitting any messages. 
- `maxChunkByteSize` - Controls how much bytes is allowed in single chunk at maximum. If there is more bytes than this number available at leader, the client will split results to more chunks // requests. 
- `maxWaitTime` - Controls how much time we wait before next `fetch` attempt is performed at leader. 

### Configurable pre-fetching

To improve performance the fs2-kafka allows to pre-fetch data from the kafka topic by setting `prefetch` parameter to `true` (default). This allows to pre-fetch nech chunk of messages from the topic while the current chunk of messages is processed at client. This may significantly increase performance, specifically when dealing with large chunks of messages. 


### Subscribe from head

In fs2-kafka `Head` is considered to be first message ever published to topic. Typically, user may want to either consume all messages in the topic and process any new messages arriving to topic or, finish subscription on very last message available in topic. 

To subscribe for all messages and await next messages to be published in topic use: 

```scala

  kafkaClient.subscribe(topic("topic-A"), parition(0), HeadOffset) : Stream[F, TopicMessage] 

```

More complex example is when you want to subscribe for messages and stop subscription at very last message published to topic: 

```scala

  kafkaClient.offsetRangeFor(topic("topic-A"), partition(0)) flatMap { case (first, next) =>
    kafkaClient.subscribe(topic("topic-A"), parition(0), first) takeWhile { _.offset <= next }
  }: Stream[F, TopicMessage] 

```

### Subscribe from exact offset 

Very similar to `Head` subscription offset can be specified when subscribing from exact position: 

```scala
  val desiredOffsetToSubscribeFrom: Long = ???
  kafkaClient.subscribe(topic("topic-A"), parition(0), offset(desiredOffsetToSubscribeFrom)) : Stream[F, TopicMessage] 

```

### Subscribe at tail 

When subscribing at `Tail`, the subscriber will receive any messages that haven not been yet published to the topic:

```scala

  kafkaClient.subscribe(topic("topic-A"), parition(0), TailOffset) : Stream[F, TopicMessage] 

```


## Design 

fs2-kafka is very simple when it comes to internal architecture. Eventhough kafka supports a variaty of patterns, we have tried to choose simple architecture that in our opinion will fit most real-world use-cases. 

### Subscribers

Every subscriber in fs2-kafka has own dedicated TCP connection with leader. We don't do any connection pooling. Reason for it is that we want to have predictable performance for every topic / partition combinations. Kafka inherently blocks fetch requests serving them in FIFO order. That effectivelly disallows for reusing tcp connections for topics with mixed message types (i.e. short and long messages) and reusing TCP connections will lead to unpredictable performance. 


### Publishers

Unlike subscribers publishers do share TCP connections. Each topic/partition combination has only one dedicated tcp connection that is opened with very first publish attempt to topic/partition and is kept open to be reused for successive publish attempts. fs2-kafka does not reuse connections between topic/partition for very same reasons like the subscribers don't : FIFO ordering and predictable performance. 

 
 
