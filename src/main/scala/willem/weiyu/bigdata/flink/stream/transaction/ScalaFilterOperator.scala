package willem.weiyu.bigdata.flink.stream.transaction

import org.apache.flink.streaming.api.scala._

/**
  * @Author weiyu
  * @Description
  * @Date 2019/5/5 12:21
  */
object ScalaFilterOperator {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.generateSequence(1,10)
    source.filter(x=>x%2==0).print
    env.execute("scalaFilterOperator")
  }
}
