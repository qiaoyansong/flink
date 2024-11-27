package union.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/27 20:55
 * description：
 */
public class EventTimeIntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> source1 = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2),
                Tuple2.of("b", 1),
                Tuple2.of("c", 1)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps().withTimestampAssigner((s1, ts) -> s1.f1 * 1000L));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> source2 = env.fromElements(
                Tuple3.of("a", 1, 1),
                Tuple3.of("a", 11, 1),
                Tuple3.of("b", 2, 1),
                Tuple3.of("b", 12, 1),
                Tuple3.of("c", 14, 1),
                Tuple3.of("d", 15, 1)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps().withTimestampAssigner((s2, ts) -> s2.f1 * 1000L));


        source1.keyBy(s1 -> s1.f0)
                .intervalJoin(source2.keyBy(s2 -> s2.f0))
                .between(Time.seconds(-2), Time.seconds(1))
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * first second 都有值，才会调用此方法，类似于inner join
                     */
                    @Override
                    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3, Context context, Collector<String> collector) throws Exception {
                        collector.collect(stringIntegerTuple2 + "--->" + stringIntegerIntegerTuple3);
                    }
                }).print();

        env.execute();
    }
}
