# fs2-kafka
Kafka driver implemented with [fs2](https://github.com/scalaz/scalaz-stream) library

## Some Assumptions:

- No dependency on Kafka driver except for tests (we need server runtime)
- JDK 7+
- Scala 2.11 + 
- Only dependency on fs2 (0.8 // scalaz-stream now) and scodec.
- No scalaz dependecy (after 0.9)
- Generic `F` instead of specializing on `Task` (after 0.9)
- Fully non-blocking // asynchronous design
- Simple client functionality only 
- One `Client` per program

## List of todo:

- [ ] Implement Binary scodec parser for kafka protocol
- [ ] Implement Asynchronous IO for Client (exchange/tcp)
- [ ] Implement Publish
- [ ] Implement Subscribe
- [ ] Implement Metadata + Offsets
- [ ] Implement Kafka Runtime for tests
