package willem.bigdata.flink.stream.transaction

import org.apache.flink.streaming.api.scala._

/**
  * @Author weiyu
  * @Description
  * @Date 2019/5/5 12:14
  */
object ScalaStreamMap {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.generateSequence(1, 10)
    source.map(x=>x*2).print
    env.execute("scalaStreamMap")
  }
}
