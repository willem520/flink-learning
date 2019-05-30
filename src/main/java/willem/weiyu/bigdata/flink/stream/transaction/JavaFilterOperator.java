package willem.weiyu.bigdata.flink.stream.transaction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author weiyu
 * @Description map操作
 * @Date 2019/3/7 16:05
 */
public class JavaFilterOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.generateSequence(1,10);
        DataStream<Long> result = source.filter((x)->x%2==0);
        result.print();
        env.execute("JavaFilterOperator");
    }
}
