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

        SingleOutputStreamOperator<Event> reduce = keyedStream.reduce(new AggregationFunction<Event>() {
            @Override
            public Event reduce(Event event, Event t1) throws Exception {
                System.out.println("event1=" + event);
                System.out.println("event2=" + t1);
                return new Event(event.user, t1.url, event.timestamp + t1.timestamp);
            }
        });

        DataStreamSink<Event> sink = reduce.print();

        // 执行任务
        env.execute();
    }
}
