package com.github.dangfan.rocketmq

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import org.apache.rocketmq.common.message.Message


interface RocketMQProducer {

  fun send(messages: List<Message>, handler: Handler<AsyncResult<Unit>>)

  fun send(messages: List<Message>): Future<Unit>

}
