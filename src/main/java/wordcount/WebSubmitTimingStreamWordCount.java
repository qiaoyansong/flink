package wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/31 5:25 下午
 * description：web端提交的job
 */
public class WebSubmitTimingStreamWordCount {

    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        // 生产环境下我们往往上传jar包，获取jar包入参可以使用下面的工具类
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostName = parameterTool.get("hostName");
        Integer port = parameterTool.getInt("port");
        DataStream<String> inputDataStream = env.socketTextStream(hostName, port);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(item -> item.f0)
                .sum(1).setParallelism(3);

        resultStream.print().setParallelism(2);

        // 执行任务
        env.execute();
    }

}
