package partition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/22 20:32
 * description：
 */
public class CustomerPartitionDemo {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 因为要路由到不同分区，因此并行度不能是1
        env.setParallelism(2);


        DataStreamSource<String> inputDataStream = env.socketTextStream("localhost", 7777);

        DataStream<String> shuffle = inputDataStream.shuffle();

        DataStreamSink<String> sink = shuffle.print();

        // 执行任务
        env.execute();
    }
}
