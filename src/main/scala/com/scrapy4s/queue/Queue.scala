package com.scrapy4s.queue

import com.scrapy4s.http.Request

/**
  * Created by sheep3 on 2018/1/30.
  */
trait Queue {

  def pull(timeOut: Long): Request

  def push(request: Request)

  def close()
}