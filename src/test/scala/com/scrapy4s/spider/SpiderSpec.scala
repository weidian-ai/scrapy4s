package com.scrapy4s.spider

import com.scrapy4s.monitor.CountLogMonitor
import com.scrapy4s.pipeline.{HtmlSavePipeline, MultiLinePipeline}
import com.scrapy4s.scheduler.HashSetScheduler
import org.scalatest.FunSuite

/**
  * Created by sheep3 on 2018/1/31.
  */
class SpiderSpec extends FunSuite {

  test("test spider") {

  }

}
object Main{
  def main(args: Array[String]): Unit = {
    Spider("example")
      // 设置超时时间
      .setTimeOut(1000 * 5)
      // 设置线程数
      .setThreadCount(1)
      // 设置调度器
      .setScheduler(HashSetScheduler())
      // 设置请求成功的测试方法
      .setTestFunc(_.statusCode == 200)
      // 设置请求重试次数
      .setTryCount(3)
      // 设置起始Url
      .setStartUrl("https://toutiao.io/")
      // 设置保存进度
      //.setHistory(true)
      // 设置进度监控
      .setMonitor(CountLogMonitor())
      // 设置解析器，存入/Users/admin/data/tmp/v2ex.txt
      .pipe(MultiLinePipeline("/Users/admin/data/tmp/toutiao.txt")(r => {
      r.xpath("""//div[@class='content']/h3[@class='title']/a/text()""")
    }))
      // 设置数据处理器
      .pipe(HtmlSavePipeline("/Users/admin/data/tmp/"))
      .start()
  }
}