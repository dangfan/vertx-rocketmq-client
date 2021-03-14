package com.github.dangfan.rocketmq

import com.github.dangfan.rocketmq.impl.RocketMQClientImpl
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.apache.rocketmq.common.message.MessageExt


interface RocketMQClient {

  fun createConsumer(consumerGroup: String, topic: String, handler: Handler<MessageExt>)

  fun createConsumer(consumerGroup: String, topic: String, tag: String?, handler: Handler<MessageExt>)

  fun createCoroutineConsumer(consumerGroup: String, topic: String, handler: CoroutineHandler<MessageExt>)

  fun createCoroutineConsumer(consumerGroup: String, topic: String, tag: String?, handler: CoroutineHandler<MessageExt>)

  fun createBatchConsumer(consumerGroup: String, topic: String, batchSize: Int, handler: Handler<List<MessageExt>>)

  fun createBatchConsumer(consumerGroup: String, topic: String, tag: String?, batchSize: Int, handler: Handler<List<MessageExt>>)

  fun createCoroutineBatchConsumer(consumerGroup: String, topic: String, batchSize: Int, handler: CoroutineHandler<List<MessageExt>>)

  fun createCoroutineBatchConsumer(consumerGroup: String, topic: String, tag: String?, batchSize: Int, handler: CoroutineHandler<List<MessageExt>>)

  fun createProducer(producerGroup: String, resultHandler: Handler<AsyncResult<RocketMQProducer>>)

  fun createProducer(producerGroup: String): Future<RocketMQProducer>

  companion object {

    fun create(vertx: Vertx, options: RocketMQOptions): RocketMQClient = RocketMQClientImpl(vertx, options)

    fun create(vertx: Vertx, options: JsonObject): RocketMQClient = RocketMQClientImpl(vertx, RocketMQOptions(options))

  }
}
