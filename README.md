# fs2-kafka
Kafka client implemented with [fs2](https://github.com/functional-streams-for-scala/fs2) library

[![Build Status](https://travis-ci.org/Spinoco/fs2-kafka.svg?branch=master)](https://travis-ci.org/Spinoco/fs2-kafka)
[![Gitter Chat](https://badges.gitter.im/functional-streams-for-scala/fs2.svg)](https://gitter.im/fs2-http/Lobby)
 
## Overview 

fs2-kafka is a simple client for consuming and publishing messages from / to Apache Kafka. It has minimalistic functionality and is fully implemented without any dependency on kafka native driver. It is fully asynchronous and non-blocking. 

Thanks to fs2, it comes with full backpressure and streaming support out of the box, and plays well with other fs2 libraries such as [fs2-http](https://github.com/Spinoco/fs2-http). 

fs2-kafka was built with minimal dependencies (apart from fs2, only scodec and shapeless is used). 

fs2-kafka only supports a subset of features compared to the native kafka client. There is for example no support for creating/administering topics and partitions, or for Kafka Connect / Kafka Streams. The reason for that is that we wanted the client to be as simple as possible, thus leaving all non-essential functionality to other solutions. Please note there is also no support for SASL Kafka protocol now. 



## Features

- Subscribe to topic / parition, with configurable pre-fetch behaviour.
- Publish to topic / partition 
- Query metadata from kafka cluster
- Query offset range for topic / partition 

For the compression of messages fs2-kafka supports GZIP and Snappy compression. 

fs2-kafka supports versions between 0.8.2 and 0.10.2 of kafka cluster with respective protocols. The protocols are cross-tested against the different versions of Kafka (i.e. 0.10.2 is tested to wrok ok with 0.8.2, 0.9.1, 0.10, 0.10.1 and 0.10.2 protocols). 

## SBT

Add this to your build file

```
libraryDependencies += "com.spinoco" %% "fs2-kafka" % "0.1.2" 
```

## Dependencies

version  |    scala  |   fs2  |  scodec | shapeless      
---------|-----------|--------|---------|-----------
0.1.2    | 2.11, 2.12|  0.9.7 | 1.10.3  | 2.3.2 


## Usage 

Throughout this simple usage guide, please consider having the following imports on your classpath:

```scala
import fs2.util.syntax._
import spinoco.fs2.kafka
import spinoco.fs2.kafka._
import spinoco.protocol.kafka._
import scala.concurrent.duration._
```
If you type console from fs2-kafka's project sbt, these imports are added for you automatically. 

### Basics

The client can be used with any `F` (effectfull type) that conforms to `Async[F]`. As a reference throughout the guide we will use fs2.Task. 

To obtain a Kafka client the following code can be used :

```scala

kafka.client(
  ensemble = Set(broker("kafka-broker1-dns-name", port = 9092))
  , protocol = ProtocolVersion.Kafka_0_10_2
  , clientName = "my-client-name"
) flatMap { kafkaClient =>
  /* your code using Kafka Client **/
  ???.asInstanceOf[Stream[F, A]]
}

```

Note that the resulting type of this program is `Stream[F, A]`, which means that stream needs to be run, and once it finishes the kafka client will terminate. 

The `protocol` parameter allows to explicitly specify the protocol to be used with Kafka ensemble, to make production migrations and upgrades easy. 


### Publishing to topics

fs2-kafka has 4 possible ways to publish to a topic. 

- `publish1` - publishes one message to topic and partition and awaits confirmation from the leader 
- `publishN` - publishes a chunk of messages to topic / partition and awaits confirmation from the leader 

- `publish1Unsafe` - like publish1, except this won't confirm that publish was successful 
- `publishNUnsafe` - like publishN, except this won't confirm that publish was successful 

The first two methods (`publish1` and `publishN`) allow to publish messages safely, meaning that once the message is published, the result contains the offset of the first successfully published message. Each of these two methods has two important parameters that have to be specified: 

- `requireQuorum` : When set to true, this indicates that quorum (majority) of ISR (In sync replicas) must aknowledge the publish operation in order for it to be successful. When set to `false`, only the leader must acknowledge the publish. 
- `serverAckTimeout` : A timeout at server (broker) to confirm the message publish operation. Note that this timeout is only relevant when publish is sent to kafka broker. There is no timeout locally within the client, so for example if there are no brokers available, the message will be queued up until the leader will become avaialble. That local timeout is subject to be implemented with `F` (see examples below). 

The second two methods to publish to topic (`publish1Unsafe` and `publishNUnsafe`) allow to publish without confirmation from the server. Locally, there is only minimal verification that publish was successful, essentially once the leader is avaiailble and connection with the leader has been established, the publish is considered to be successful. 

Please note there is a difference from how the kafka native client behaves when publishing. Kafka client retries (3x) when there was an unsuccesful publish. fs2-kafka instead fails as soon as possible and leaves it up to the user to choose from any retry mechanics. 

### Examples of publishing to topic: 

```scala

/// assuming val kafkaClient: KafkaClient = ... 

// publishes safely 1 message to topic-A partition 0, with Key 1, and value [1, 2, 3]  
// returns when the publish was accepted by a majority (quorum) of the servers. Fails, when server doesn't get acknowledgements from ISRs in 10s or less. 
kafkaClient.publish1(topic("topic-A"), partition(0), ByteVector(1), ByteVector(1,2,3), requireQuorum = true, serverAckTimeout = 10 seconds)

// consumes a stream of key value pairs and publishes them in chunks to a broker. Note that this publishes the stream of messages in chunks utilitizing the kafka batch processing.   
// returns when the publish was accepted by a majority (quorum) of the servers. Fails, when server doesn't get acknowledgements from ISRs in 10s or less. 
// Additionally the chunks are not compressed, but GZIP or Snappy can be plugged in by specifying the compress attribute. 
// The resulting stream will contain the starting offset of the very first message in chunk. 
val streamOfValues: Stream[Task, (ByteVector, ByteVector)] = ???
streamOfValues.chunk.evalMap {
  kafkaClient.publishN(topic("topic-A"), partition(0), requireQuorum = true, serverAckTimeout = 10 seconds, compress = None)
}

// publishes an s1 message to topic-A partition 0, with Key 1, and value [1, 2, 3]  
// returns immediately, when there is a leader known for topic/ partition 
kafkaClient.publish1Unsafe(topic("topic-A"), partition(0), ByteVector(1), ByteVector(1,2,3))

// consumes a stream of key value pairs and publishes them in chunks to a broker. Note that this publishes the stream of messages in chunks utilitizing the kafka batch processing.   
// returns immediately. 
// Additionally the chunks are not compressed, but GZIP or Snappy can be plugged in by specifying the compress attribute. 
val streamOfValues: Stream[Task, (ByteVector, ByteVector)] = ???
streamOfValues.chunk.evalMap {
  kafkaClient.publishNUnsafe(topic("topic-A"), partition(0),compress = None)
}

```

### Querying kafka for available offsets

In some scenarios, it is useful to know kafka first and last offset at any given time. As you will see below, the fs2-kafka makes the "tail" of the topic with every message (tail - last message in topic) available. However, you may need to know the available range before you start consuming the topic. 

fs2-kafka has `offsetRangeFor` API for that purpose. When evaluated, this will return the offset of the first and the next message to be published in every topic/partition. When the offsets are equal, the topic/partition is empty. 

exmaple: 

```scala 

// Queries the first and the next-to-be-published offset of messages in topic `topic-A` partition 0
kafkaClient.offsetRangeFor(topic("topic-A"), partition(0)) flatMap { case (first, next) =>
   ???
}

```

### Subscription

fs2-kafka client only has one method of subscriptions for the topic. Subscription to topic is utilizing the `subscribe` method with several parameters that can finetune its exact behaviour. 

Subscription to kafka topic always subscribes with the first message specified in the `offset` parameter and always awaits the next messages to be published to the topic (won't terminate when there are no more messages in topic available). That behaviour, in combination with `offsetRangeFor`, shall give the user the posibility to express any subscription pattern necessary. 

### Subscribe at invalid offset range 

fs2-kafka client is able to recover itself when there is an invalid offset specified for the `offset` parameter. The rules to recover from an invalid offset are: 

 - When the offset is lower than the offset of the first message available in the topic, the subscription will start at the very first message available
 - When the offset is greater than the offset of the last message availble in the topic, the subscription will start at the very next message arriving to topic since the stream was evaluated. 
 
 
### Error Recovery

fs2-kafka client may recover from broker failures. It also allows the recovery to be handled externally by user logic. However, simple machanics have been built in. 

Client's recovery of the `subscribe` method is controlled by `leaderFailureTimeout` and `leaderFailureMaxAttempts` parameters. Default behaviour is to allow for three consecutive leader failures and sleep for 5s between these failures before giving up and failing the subscription. When the user wants to control failure recovery manually, `0 millis` should be used for `leaderFailureTimeout` and `0` for `leaderFailureMaxAttempts` parameters, causing the subscription to fail at any error (except for invalid offsets, which are always handled).

fs2-kafka client `subscribe` will recover from any failures, and will start the next subscription from the next offset to the last received message. 

### Further subscription fine-tuning 

fs2-kafka subscription allows to further finetune the behaviour of the subscriptions. When subscribing, the fs2-kafka client may receive more messages in a single chunk, and in that case fs2-kafka fully utiliizes the "chunk" optimization of the fs2 library. Furthermore, the following parameters may control the behaviour of the subscription : 

- `minChunkByteSize` - Controls the minimum number of bytes that must be ready at kafka leader in order to complete a single fetch chunk. If there is less than that amout of bytes, the susbcriber will wait up to that amount of bytes before emitting any messages. 
- `maxChunkByteSize` - Controls the maximum number of bytes that is allowed in a single chunk. If there is more bytes than this number available at leader, the client will split the results to more chunks // requests. 
- `maxWaitTime` - Controls how much time we wait before the next `fetch` attempt is performed at the leader. 

### Configurable pre-fetching

To improve performance, fs2-kafka allows to pre-fetch data from the kafka topic by setting the `prefetch` parameter to `true` (default). This allows to pre-fetch the next chunk of messages from the topic while the current chunk of messages is processed at client. This may significantly increase performance, specifically when dealing with large chunks of messages. 


### Subscribe from head

In fs2-kafka `Head` is considered to be the first message ever published to a topic. Typically, the user may want to either consume all messages in the topic and process any new messages arriving to the topic, or finish subscription on the very last message available in topic. 

To subscribe for all messages and await the next messages to be published in a topic use: 

```scala

  kafkaClient.subscribe(topic("topic-A"), parition(0), HeadOffset) : Stream[F, TopicMessage] 

```

A more complex example occurs when you want to subscribe for messages and stop the subscription at the very last message published to a topic: 

```scala

  kafkaClient.offsetRangeFor(topic("topic-A"), partition(0)) flatMap { case (first, next) =>
    kafkaClient.subscribe(topic("topic-A"), parition(0), first) takeWhile { _.offset <= next }
  }: Stream[F, TopicMessage] 

```

### Subscribe from an exact offset 

Very similar to `Head`, the subscription offset can be specified when subscribing from an exact position: 

```scala
  val desiredOffsetToSubscribeFrom: Long = ???
  kafkaClient.subscribe(topic("topic-A"), parition(0), offset(desiredOffsetToSubscribeFrom)) : Stream[F, TopicMessage] 

```

### Subscribe at tail 

When subscribing at `Tail`, the subscriber will receive any messages that have not been published to the topic yet: 

```scala

  kafkaClient.subscribe(topic("topic-A"), parition(0), TailOffset) : Stream[F, TopicMessage] 

```


## Design 

fs2-kafka is very simple when it comes to internal architecture. Even though kafka supports a variety of patterns, we chose a simple, minimalistic architecture that in our opinion will fit most real-world use-cases. 

### Subscribers

Every subscriber in fs2-kafka has their own dedicated TCP connection with the leader. We don't do any connection pooling. The reason for this is that we want to have a predictable performance for every topic / partition combination. Kafka inherently blocks fetch requests serving them in FIFO order. That effectively disallows for reusing TCP connections for topics with mixed message types (i.e. short and long messages). Reusing TCP connections will lead to unpredictable performance. 


### Publishers

Unlike subscribers, publishers do share TCP connections. Each topic/partition combination has only one dedicated TCP connection that is opened with the very first publish attempt to topic/partition and is kept open to be reused for successive publish attempts. fs2-kafka does not reuse connections between topic/partition for the same reasons as the subscribers: FIFO ordering and predictable performance. 

 
 
