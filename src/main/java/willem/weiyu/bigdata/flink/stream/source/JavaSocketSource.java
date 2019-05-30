package willem.weiyu.bigdata.flink.stream.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author weiyu
 * @Description 基于socket的数据源
 * @Date 2019/2/27 10:30
 */
public class JavaSocketSource {
    public static final String URL = "10.26.27.81";
    public static final int PORT = 9999;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream(URL, PORT);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);

        sum.print();
        env.execute("javaSocketSource");
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.toLowerCase().split("\\W+");
            for (String word : words) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
