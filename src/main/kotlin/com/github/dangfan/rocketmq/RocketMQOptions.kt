package com.github.dangfan.rocketmq

import io.vertx.core.json.JsonObject


data class RocketMQOptions(
  val nameserver: String,
  val instanceName: String = "DEFAULT",
  val consumerThreads: Int = 20,
  val consumeTimeout: Long = 30
) {
  constructor(options: JsonObject) :
    this(
      options.getString("nameserver"),
      options.getString("instanceName", "DEFAULT"),
      options.getInteger("consumerThreads", 20),
      options.getLong("consumeTimeout", 30L)
    )
}
