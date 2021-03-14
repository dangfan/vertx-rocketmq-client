package com.github.dangfan.rocketmq.impl

import com.github.dangfan.rocketmq.RocketMQProducer
import io.vertx.core.*
import org.apache.rocketmq.client.producer.DefaultMQProducer
import org.apache.rocketmq.common.message.Message


class RocketMQProducerImpl(vertx: Vertx, private val producer: DefaultMQProducer) : RocketMQProducer {

  private val executor = vertx.createSharedWorkerExecutor("rocketmq-producer")

  override fun send(messages: List<Message>, handler: Handler<AsyncResult<Unit>>) {
    executor.executeBlocking({ promise ->
      producer.send(messages)
      promise.complete()
    }, handler)
  }

  override fun send(messages: List<Message>): Future<Unit> {
    val promise = Promise.promise<Unit>()
    send(messages, promise)
    return promise.future()
  }

}
