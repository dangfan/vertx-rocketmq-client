package com.github.dangfan.rocketmq.impl

import io.vertx.core.spi.tracing.TagExtractor
import org.apache.rocketmq.common.message.MessageExt


internal class MessageTagExtractor : TagExtractor<MessageExt> {
  override fun len(obj: MessageExt): Int {
    return 6
  }

  override fun name(obj: MessageExt, index: Int): String = when (index) {
    0 -> "msg.id"
    1 -> "msg.born-host"
    2 -> "msg.born-timestamp"
    3 -> "msg.broker"
    4 -> "msg.offset"
    5 -> "msg.reconsume-times"
    else -> throw IndexOutOfBoundsException("Invalid tag index $index")
  }

  override fun value(obj: MessageExt, index: Int): String = when (index) {
    0 -> obj.msgId
    1 -> obj.bornHostString
    2 -> obj.bornTimestamp.toString()
    3 -> obj.brokerName
    4 -> obj.commitLogOffset.toString()
    5 -> obj.reconsumeTimes.toString()
    else -> throw IndexOutOfBoundsException("Invalid tag index $index")
  }

  companion object {
    val INSTANCE = MessageTagExtractor()
  }
}
