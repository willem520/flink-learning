package willem.weiyu.bigdata.flink.cep;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author weiyu
 * @Description 利用cep处理模拟登陆事件
 * @Date 2019/5/7 16:52ø
 */
public class JavaLoginOn {

    protected static class LoginEventSource extends RichParallelSourceFunction<LoginEvent> {
        private volatile boolean running = false;
        private String[] state = new String[]{"success","fail"};

        @Override
        public void run(SourceFunction.SourceContext sourceContext) throws Exception {
            running = true;
            Random ipRand = new Random();
            Random idRand = new Random();
            Random timeRand = new Random();

            while (running){
                LoginEvent loginEvent = new LoginEvent();
                loginEvent.setUserId(String.valueOf(idRand.nextInt(11)));
                loginEvent.setIp(ipRand.nextInt(256)+"."+ipRand.nextInt(256)+"."+ipRand.nextInt(256)+"."+ipRand.nextInt(256));
                loginEvent.setType(state[new Random().nextInt(2)]);
                loginEvent.setTimestamp(System.currentTimeMillis()-timeRand.nextInt(1001));
                sourceContext.collect(loginEvent);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<LoginEvent> loginEventStream = streamEnv.addSource(new LoginEventSource());
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("begin").where(
                new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        return "fail".equalsIgnoreCase(loginEvent.getType());
                    }
                }).next("next")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        return "fail".equalsIgnoreCase(loginEvent.getType());
                    }
                }).within(Time.seconds(3));

        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId),
                loginFailPattern);

        DataStream<LoginWarning> loginWarningDataStream =
                patternStream.select((Map<String, List<LoginEvent>> pattern) -> {
                    List<LoginEvent> first = pattern.get("begin");
                    List<LoginEvent> second = pattern.get("next");
                    return new LoginWarning(second.get(0).getUserId(), second.get(0).getIp(), second.get(0).getType()
                            , second.get(0).getTimestamp());
                });
        loginWarningDataStream.print();

        streamEnv.execute("javaLoginOn");
    }
}
