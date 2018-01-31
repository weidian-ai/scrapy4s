package com.scrapy4s.queue
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.scrapy4s.http.Request

/**
  * Created by sheep3 on 2018/1/31.
  */
class StandAloneQueue(queue: BlockingQueue[Request]) extends Queue {
  override def pull(timeOut: Long): Request = queue.poll(timeOut, TimeUnit.MILLISECONDS)

  override def push(request: Request): Unit = queue.put(request)

  override def close(): Unit = {
    // TODO
  }
}

object StandAloneQueue {
  def apply(queue: BlockingQueue[Request] = new LinkedBlockingQueue[Request]()): StandAloneQueue = new StandAloneQueue(queue)
}
