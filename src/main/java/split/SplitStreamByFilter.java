package split;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/22 20:52
 * description：
 */
public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<String> stream1 = source.filter(num -> Integer.valueOf(num) % 2 == 0).disableChaining();
        SingleOutputStreamOperator<String> stream2 = source.filter(num -> Integer.valueOf(num) % 2 == 1).disableChaining();


        stream1.print("偶数流");
        stream2.print("奇数流");

        env.execute();
    }
}
