# vertx-rocketmq-client

The Vert.x RocketMQ client provides proper bindings to run callbacks on Vert.x event loops.
Also, it supports Open Tracing.

## Getting started

Kotlin version:

```Kotlin
val client = RocketMQClient.create(vertx, RocketMQOptions("10.0.3.3:9877"))
// If you like suspend functions:
client.createCoroutineConsumer("mainConsumer", "test-topic", object : CoroutineHandler<MessageExt> {
  override suspend fun handle(msg: MessageExt) {
    println(msg.msgId)
  }
}) // we have to create the object due to KT-40978

// Or, a simple handler:
client.createConsumer("anotherConsumer", "another-topic") { msg: MessageExt ->
  println(msg.msgId)
}
```

## Download

TBD

## Contributors

Any contribution is appreciated. See the contributors list in: https://github.com/dangfan/vertx-rocketmq-client/graphs/contributors
