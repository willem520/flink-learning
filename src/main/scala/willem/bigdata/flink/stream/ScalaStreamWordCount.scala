package willem.bigdata.flink.stream

import java.io.File

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author weiyu
  * @Description
  * @Date 2019/3/7 16:29
  */
object ScalaStreamWordCount {
  val TEXT_PATH = "example" + File.separator + "word.txt"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(TEXT_PATH)

    val counts = text.flatMap({
      _.toLowerCase.split("\\s+") filter {
        _.nonEmpty
      }
    }).map((_,1)).keyBy(0).timeWindow(Time.seconds(1)).sum(1)

    counts.print()

    env.execute("scalaWordCount")
  }
}
