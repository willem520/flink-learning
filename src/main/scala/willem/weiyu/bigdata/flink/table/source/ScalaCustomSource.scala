package willem.weiyu.bigdata.flink.table.source

import org.apache.flink.streaming.api.scala._
import willem.weiyu.bigdata.flink.stream.StringLineEventSource

object ScalaCustomSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new StringLineEventSource)
    text.print

    env.execute("scala custom source")
  }
}
