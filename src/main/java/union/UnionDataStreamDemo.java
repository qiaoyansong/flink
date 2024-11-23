package union;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/22 22:33
 * description：
 */
public class UnionDataStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<String> source1 = env.fromElements("1", "2", "3");
        DataStreamSource<String> source2 = env.fromElements("11", "22", "33");

        DataStream<String> union = source1.union(source2);


        union.print("主流");

        env.execute();
    }
}
