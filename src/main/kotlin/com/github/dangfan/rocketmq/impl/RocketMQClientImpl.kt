package com.github.dangfan.rocketmq.impl

import com.github.dangfan.rocketmq.CoroutineHandler
import com.github.dangfan.rocketmq.RocketMQClient
import com.github.dangfan.rocketmq.RocketMQOptions
import com.github.dangfan.rocketmq.RocketMQProducer
import io.vertx.core.*
import io.vertx.core.impl.ContextInternal
import io.vertx.core.spi.tracing.SpanKind
import io.vertx.core.spi.tracing.TagExtractor
import io.vertx.core.tracing.TracingPolicy
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus
import org.apache.rocketmq.client.producer.DefaultMQProducer
import org.apache.rocketmq.common.message.MessageExt
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit


@Suppress("DuplicatedCode")
class RocketMQClientImpl(private val vertx: Vertx, private val options: RocketMQOptions) : RocketMQClient {

  private val logger = KotlinLogging.logger {}

  private val executor = vertx.createSharedWorkerExecutor("rocketmq-consumer")

  override fun createConsumer(consumerGroup: String, topic: String, handler: Handler<MessageExt>) {
    createConsumer(consumerGroup, topic, null, handler)
  }

  override fun createConsumer(consumerGroup: String, topic: String, tag: String?, handler: Handler<MessageExt>) {
    val tracer = (vertx.orCreateContext as ContextInternal).tracer()
    val consumer = buildConsumer(consumerGroup, topic, tag)
    consumer.registerMessageListener { messages: List<MessageExt>, _: ConsumeConcurrentlyContext ->
      logger.debug { "received ${messages.size} messages" }
      messages.forEach { msg ->
        val cf = CompletableFuture<Unit>()
        val ctx = (vertx.orCreateContext as ContextInternal).duplicate()
        val span = tracer?.receiveRequest<MessageExt>(ctx, SpanKind.MESSAGING, TracingPolicy.ALWAYS, msg, "consume: $topic", MultiMap.caseInsensitiveMultiMap(),
          MessageTagExtractor.INSTANCE
        )
        ctx.runOnContext {
          try {
            handler.handle(msg)
            tracer?.sendResponse<Nothing>(ctx, null, span, null, TagExtractor.empty<Nothing>())
            cf.complete(Unit)
          } catch (failure: Throwable) {
            tracer?.sendResponse<Nothing>(ctx, null, span, failure, TagExtractor.empty<Nothing>())
            logger.error(failure) { "error when consuming message: ${msg.msgId}" }
            cf.completeExceptionally(failure)
          }
        }
        cf.get(options.consumeTimeout, TimeUnit.SECONDS)
      }
      ConsumeConcurrentlyStatus.CONSUME_SUCCESS
    }
    executor.executeBlocking<Nothing> { promise ->
      consumer.start()
      promise.complete()
    }
  }

  override fun createCoroutineConsumer(consumerGroup: String, topic: String, handler: CoroutineHandler<MessageExt>) {
    createCoroutineConsumer(consumerGroup, topic, null, handler)
  }

  override fun createCoroutineConsumer(consumerGroup: String, topic: String, tag: String?, handler: CoroutineHandler<MessageExt>) {
    val tracer = (vertx.orCreateContext as ContextInternal).tracer()
    val consumer = buildConsumer(consumerGroup, topic, tag)
    consumer.registerMessageListener { messages: List<MessageExt>, _: ConsumeConcurrentlyContext ->
      logger.debug { "received ${messages.size} messages" }
      messages.forEach { msg ->
        val cf = CompletableFuture<Unit>()
        val ctx = (vertx.orCreateContext as ContextInternal).duplicate()
        val span = tracer?.receiveRequest<MessageExt>(ctx, SpanKind.MESSAGING, TracingPolicy.ALWAYS, msg, "consume: $topic", MultiMap.caseInsensitiveMultiMap(),
          MessageTagExtractor.INSTANCE
        )
        GlobalScope.launch(ctx.dispatcher()) {
          try {
            handler.handle(msg)
            tracer?.sendResponse<Nothing>(ctx, null, span, null, TagExtractor.empty<Nothing>())
            cf.complete(Unit)
          } catch (failure: Throwable) {
            tracer?.sendResponse<Nothing>(ctx, null, span, failure, TagExtractor.empty<Nothing>())
            logger.error(failure) { "error when consuming message: ${msg.msgId}" }
            cf.completeExceptionally(failure)
          }
        }
        cf.get(options.consumeTimeout, TimeUnit.SECONDS)
      }
      ConsumeConcurrentlyStatus.CONSUME_SUCCESS
    }
    executor.executeBlocking<Nothing> { promise ->
      consumer.start()
      promise.complete()
    }
  }

  override fun createBatchConsumer(consumerGroup: String, topic: String, batchSize: Int, handler: Handler<List<MessageExt>>) {
    createBatchConsumer(consumerGroup, topic, null, batchSize, handler)
  }

  override fun createBatchConsumer(consumerGroup: String, topic: String, tag: String?, batchSize: Int, handler: Handler<List<MessageExt>>) {
    val tracer = (vertx.orCreateContext as ContextInternal).tracer()
    val consumer = buildConsumer(consumerGroup, topic, tag, batchSize)
    consumer.registerMessageListener { messages: List<MessageExt>, _: ConsumeConcurrentlyContext ->
      logger.debug { "received ${messages.size} messages" }
      val cf = CompletableFuture<Unit>()
      val ctx = (vertx.orCreateContext as ContextInternal).duplicate()
      val span = tracer?.receiveRequest<List<MessageExt>>(ctx, SpanKind.MESSAGING, TracingPolicy.ALWAYS, messages, "consume: $topic", MultiMap.caseInsensitiveMultiMap(),
        MessageListTagExtractor.INSTANCE
      )
      ctx.runOnContext {
        try {
          handler.handle(messages)
          tracer?.sendResponse<Nothing>(ctx, null, span, null, TagExtractor.empty<Nothing>())
          cf.complete(Unit)
        } catch (failure: Throwable) {
          tracer?.sendResponse<Nothing>(ctx, null, span, failure, TagExtractor.empty<Nothing>())
          logger.error(failure) { "error when consuming message: ${messages.joinToString { it.msgId }}" }
          cf.completeExceptionally(failure)
        }
      }
      cf.get(options.consumeTimeout, TimeUnit.SECONDS)
      ConsumeConcurrentlyStatus.CONSUME_SUCCESS
    }
    executor.executeBlocking<Nothing> { promise ->
      consumer.start()
      promise.complete()
    }
  }

  override fun createCoroutineBatchConsumer(consumerGroup: String, topic: String, batchSize: Int, handler: CoroutineHandler<List<MessageExt>>) {
    createCoroutineBatchConsumer(consumerGroup, topic, null, batchSize, handler)
  }

  override fun createCoroutineBatchConsumer(consumerGroup: String, topic: String, tag: String?, batchSize: Int, handler: CoroutineHandler<List<MessageExt>>) {
    val tracer = (vertx.orCreateContext as ContextInternal).tracer()
    val consumer = buildConsumer(consumerGroup, topic, tag, batchSize)
    consumer.registerMessageListener { messages: List<MessageExt>, _: ConsumeConcurrentlyContext ->
      logger.debug { "received ${messages.size} messages" }
      val cf = CompletableFuture<Unit>()
      val ctx = (vertx.orCreateContext as ContextInternal).duplicate()
      val span = tracer?.receiveRequest<List<MessageExt>>(ctx, SpanKind.MESSAGING, TracingPolicy.ALWAYS, messages, "consume: $topic", MultiMap.caseInsensitiveMultiMap(),
        MessageListTagExtractor.INSTANCE
      )
      GlobalScope.launch(ctx.dispatcher()) {
        try {
          handler.handle(messages)
          tracer?.sendResponse<Nothing>(ctx, null, span, null, TagExtractor.empty<Nothing>())
          cf.complete(Unit)
        } catch (failure: Throwable) {
          tracer?.sendResponse<Nothing>(ctx, null, span, failure, TagExtractor.empty<Nothing>())
          logger.error(failure) { "error when consuming message: ${messages.joinToString { it.msgId }}" }
          cf.completeExceptionally(failure)
        }
      }
      cf.get(options.consumeTimeout, TimeUnit.SECONDS)
      ConsumeConcurrentlyStatus.CONSUME_SUCCESS
    }
    executor.executeBlocking<Nothing> { promise ->
      consumer.start()
      promise.complete()
    }
  }

  override fun createProducer(producerGroup: String, resultHandler: Handler<AsyncResult<RocketMQProducer>>) {
    val producer = DefaultMQProducer(producerGroup)
    producer.namesrvAddr = options.nameserver
    executor.executeBlocking<Nothing>({ promise ->
      producer.start()
      promise.complete()
    }) {
      if (it.succeeded()) {
        resultHandler.handle(Future.succeededFuture(RocketMQProducerImpl(vertx, producer)))
      } else {
        resultHandler.handle(Future.failedFuture(it.cause()))
      }
    }
  }

  override fun createProducer(producerGroup: String): Future<RocketMQProducer> {
    val promise = Promise.promise<RocketMQProducer>()
    createProducer(producerGroup, promise)
    return promise.future()
  }

  private fun buildConsumer(consumerGroup: String, topic: String, tag: String?, batchSize: Int = 1): DefaultMQPushConsumer {
    val consumer = DefaultMQPushConsumer(consumerGroup)
    consumer.namesrvAddr = options.nameserver
    consumer.instanceName = options.instanceName
    consumer.consumeThreadMin = options.consumerThreads
    consumer.consumeThreadMax = options.consumerThreads
    consumer.changeInstanceNameToPID()
    consumer.consumeMessageBatchMaxSize = batchSize
    consumer.subscribe(topic, tag)
    return consumer
  }

}
