package wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/31 12:49 下午
 * description：
 */
public class TimingStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        // env.setMaxParallelism(32);


        // 生产环境下我们往往上传jar包，获取jar包入参可以使用下面的工具类
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String hostName = parameterTool.get("hostName");
//        Integer port = parameterTool.getInt("port");
//        DataStream<String> inputDataStream = env.socketTextStream(hostName, port);

        // 从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(item -> item.f0)
                .sum(1);

        resultStream.print();

        // 执行任务
        env.execute();
    }
}
