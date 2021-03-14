package com.github.dangfan.rocketmq


interface CoroutineHandler<E> {

  suspend fun handle(event: E)

}
