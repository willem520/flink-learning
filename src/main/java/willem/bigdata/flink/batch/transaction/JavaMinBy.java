package willem.bigdata.flink.batch.transaction;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author: willem
 * @create: 2019/08/29 23:17
 * @description:
 */
public class JavaMinBy {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3<Integer, String, Double>> input = env.fromElements(
                new Tuple3<>(1, "Hello", 5.0),
                new Tuple3<>(1, "Hello", 4.0),
                new Tuple3<>(2, "Hello", 5.0),
                new Tuple3<>(3, "World", 7.0),
                new Tuple3<>(4, "World", 6.0)
        );
        //根据第二个元素做分组后，现对第一个元素求最小值，如果相同，再对第三个元素求最小值，maxBy与之类似
        ReduceOperator output = input.groupBy(1).minBy(0,2);
        output.print();
        env.execute("javaMinBy");
    }
}
