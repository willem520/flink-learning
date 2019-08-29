package willem.bigdata.flink.batch.transaction;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author: willem
 * @create: 2019/08/29 22:39
 * @description:
 */
public class JavaAggregate {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3<Integer, String, Double>> input = env.fromElements(
                new Tuple3<>(1, "Hello", 4.0),
                new Tuple3<>(1, "Hello", 5.0),
                new Tuple3<>(2, "Hello", 5.0),
                new Tuple3<>(3, "World", 6.0),
                new Tuple3<>(3, "World", 6.0)
        );

        //根据String属性分组后，根据Integer属性求和，在对Double属性求最小值
        AggregateOperator result = input.groupBy(1).aggregate(Aggregations.SUM, 0).and(Aggregations.MIN, 2);

        result.print();

        env.execute("javaAggregate");
    }
}
