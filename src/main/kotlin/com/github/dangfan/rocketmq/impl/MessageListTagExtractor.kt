package com.github.dangfan.rocketmq.impl

import io.vertx.core.spi.tracing.TagExtractor
import org.apache.rocketmq.common.message.MessageExt


internal class MessageListTagExtractor : TagExtractor<List<MessageExt>> {
  override fun len(obj: List<MessageExt>): Int {
    return 7
  }

  override fun name(obj: List<MessageExt>, index: Int): String = when (index) {
    0 -> "count"
    1 -> "msg.id"
    2 -> "msg.born-host"
    3 -> "msg.born-timestamp"
    4 -> "msg.broker"
    5 -> "msg.offset"
    6 -> "msg.reconsume-times"
    else -> throw IndexOutOfBoundsException("Invalid tag index $index")
  }

  override fun value(obj: List<MessageExt>, index: Int): String = when (index) {
    0 -> obj.size.toString()
    1 -> obj.joinToString { it.msgId }
    2 -> obj.joinToString { it.bornHostString }
    3 -> obj.joinToString { it.bornTimestamp.toString() }
    4 -> obj.joinToString { it.brokerName }
    5 -> obj.joinToString { it.commitLogOffset.toString() }
    6 -> obj.joinToString { it.reconsumeTimes.toString() }
    else -> throw IndexOutOfBoundsException("Invalid tag index $index")
  }

  companion object {
    val INSTANCE = MessageListTagExtractor()
  }
}
