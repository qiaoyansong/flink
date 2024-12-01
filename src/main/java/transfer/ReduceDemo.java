package transfer;

import bean.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/22 15:52
 * description：
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);


        DataStreamSource<Event> inputDataStream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Bob", "./cart", 3000L)
        );

        KeyedStream<Event, String> keyedStream = inputDataStream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        });

        SingleOutputStreamOperator<Event> reduce1 = keyedStream.reduce(new AggregationFunction<Event>() {
            @Override
            public Event reduce(Event event, Event t1) throws Exception {
                System.out.println("reduce1" + "event1=" + event);
                System.out.println("reduce1" + "event2=" + t1);
                return new Event(event.user, t1.url, event.timestamp + t1.timestamp);
            }
        });

        // 为每一条数据分配同一个 key，将聚合结果发送到一条流中去
        SingleOutputStreamOperator<Event> reduce2 = reduce1.keyBy(r -> true).reduce(new AggregationFunction<Event>() {
            @Override
            public Event reduce(Event value1, Event value2) throws Exception {
                System.out.println("reduce2" + "event1=" + value1);
                System.out.println("reduce2" + "event2=" + value2);
                // 将累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
                return value1.timestamp > value2.timestamp ? value1 : value2;
            }
        });

        DataStreamSink<Event> sink = reduce2.print();

        // 执行任务
        env.execute();
    }
}
