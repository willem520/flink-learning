package willem.weiyu.bigdata.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.File;

/**
 * @author: willem
 * @create: 2019/08/28 23:24
 * @description:
 */
public class JavaWordCount {
    private static final String TEXT_PATH = "example" + File.separator + "word.txt";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lines = env.readTextFile(TEXT_PATH);
        lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split("\\s+")) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> w1, Tuple2<String, Integer> w2) throws Exception {
                return new Tuple2<>(w1.f0, w1.f1+w2.f1);
            }
        }).print();
        env.execute("javaWordCount");
    }
}
