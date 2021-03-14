package com.github.dangfan.rocketmq.example

import com.github.dangfan.rocketmq.CoroutineHandler
import com.github.dangfan.rocketmq.RocketMQClient
import com.github.dangfan.rocketmq.RocketMQOptions
import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.delay
import mu.KotlinLogging
import org.apache.rocketmq.common.message.MessageExt
import java.lang.IllegalArgumentException
import java.util.concurrent.atomic.AtomicInteger


class ConsumerVerticle(private val isBatch: Boolean) : CoroutineVerticle() {

  private val logger = KotlinLogging.logger {}

  override suspend fun start() {
    val counter = AtomicInteger(0)

    val eb = vertx.eventBus()
    eb.consumer<String>("test") {
      logger.info { "${counter.incrementAndGet()} ${it.body()}" }
    }

    val client = RocketMQClient.create(vertx, RocketMQOptions("10.0.3.3:9877"))
    if (!isBatch)
      client.createCoroutineConsumer("mainConsumer", "test", object : CoroutineHandler<MessageExt> {
        override suspend fun handle(msg: MessageExt) {
          if (msg.reconsumeTimes < 2) {
            delay(100)
            logger.info { "we drop this" }
            throw IllegalArgumentException("oops")
          }
          logger.info { msg.msgId + " " + String(msg.body) }
          delay(100)
          eb.send("test", String(msg.body))
        }
      })
    else
      client.createCoroutineBatchConsumer("mainConsumer", "test", 15, object : CoroutineHandler<List<MessageExt>> {
        override suspend fun handle(messages: List<MessageExt>) {
          val total = counter.addAndGet(messages.size)
          logger.info { "total: $total, id list: ${messages.joinToString { it.msgId }}" }
        }
      })
  }

}

fun main() {
  Vertx.vertx().deployVerticle(ConsumerVerticle(false))
}
