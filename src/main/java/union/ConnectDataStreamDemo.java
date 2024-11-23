package union;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/22 22:49
 * description：
 */
public class ConnectDataStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<String> source2 = env.fromElements("a", "b", "b");

        ConnectedStreams<Integer, String> connectedStreams = source1.connect(source2);

        /**
         * 三个泛型 分别是 调用connect的流的数据类型 被调用connect的数据类型 以及输出流的数据类型
         */
        SingleOutputStreamOperator<String> outputStreamOperator = connectedStreams.map(new CoMapFunction<Integer, String, String>() {

            /**
             * map1 处理调用方
             */
            @Override
            public String map1(Integer integer) throws Exception {
                return integer.toString();
            }

            /**
             * map2 处理被调用方
             */
            @Override
            public String map2(String s) throws Exception {
                return s;
            }
        });


        outputStreamOperator.print();

        env.execute();
    }
}
