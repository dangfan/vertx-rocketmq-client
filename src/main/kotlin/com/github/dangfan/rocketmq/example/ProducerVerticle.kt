package com.github.dangfan.rocketmq.example

import com.github.dangfan.rocketmq.RocketMQClient
import com.github.dangfan.rocketmq.RocketMQOptions
import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import org.apache.rocketmq.common.message.Message


class ProducerVerticle : CoroutineVerticle() {

  override suspend fun start() {
    val client = RocketMQClient.create(vertx, RocketMQOptions("10.0.3.3:9877"))
    val producer = client.createProducer("mainProducer").await()
    val msgs = (1..100).map { Message("test", "test".toByteArray()) }
    producer.send(msgs).await()
    println("done")
  }

}

fun main() {
  Vertx.vertx().deployVerticle(ProducerVerticle())
}
