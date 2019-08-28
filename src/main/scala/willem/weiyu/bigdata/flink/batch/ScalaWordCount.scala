package willem.weiyu.bigdata.flink.batch

import java.io.File

import org.apache.flink.api.scala._

/**
  * @Author weiyu
  * @Description
  * @Date 2019/3/7 16:29
  */
object ScalaWordCount {
  val TEXT_PATH = "example" + File.separator + "word.txt"

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(TEXT_PATH)

    val counts = text.flatMap({
      _.toLowerCase.split("\\s+") filter {
        _.nonEmpty
      }
    }).map{(_, 1)}.groupBy(0).sum(1)

    counts.print()

    env.execute("scalaWordCount")
  }
}
