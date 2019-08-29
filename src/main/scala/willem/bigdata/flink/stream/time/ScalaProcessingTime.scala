package willem.bigdata.flink.stream.time

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import willem.bigdata.flink.stream.StringLineEventSource

object ScalaProcessingTime {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //处理时间(默认)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //line格式 ts|channel|id|behavior
    val line = env.addSource(new StringLineEventSource)

    val inputMap = line.map(word => {
      val arr = word.split("\\|")
      (arr(0).toLong, arr(1), arr(2), arr(3))
    })

    val window = inputMap
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .apply(new MyWindowFunction)

    window.print()

    env.execute()
  }

  class MyWindowFunction extends WindowFunction[(Long, String, String, String), (Long, String, String, String), Long, TimeWindow] {

    override def apply(key: Long, window: TimeWindow, input: Iterable[(Long, String, String, String)], out: Collector[(Long, String, String, String)]): Unit = {
      val list = input.toList.sortBy(_._1)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      out.collect(key, format.format(list.head._1), format.format(list.last._1), "[" + format.format(window.getStart) + "," + format.format(window.getEnd) + ")")
    }
  }

}
