package wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
        SingleOutputStreamOperator<Tuple2<String, Long>> line2WordCountTupleOp = inputDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 按空格分词
            String[] words = line.split(" ");
            // 遍历所有word，包成二元组输出
            for (String str : words) {
                out.collect(Tuple2.of(str, 1L));
            }
        })
                // 如果flatmap使用lambda表达式 必须显式只订购返回值类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> word2CountMap = line2WordCountTupleOp.keyBy(item -> item.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = word2CountMap.sum(1).setParallelism(3);

        resultStream.print().setParallelism(2);

        // 执行任务
        env.execute();
    }

}
