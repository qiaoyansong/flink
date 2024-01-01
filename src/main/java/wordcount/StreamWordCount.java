package wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/31 11:12 上午
 * description：
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        // env.setMaxParallelism(32);

        // 从文件中读取数据
        String inputPath = "/Users/didi/Documents/ideaWorkPlace/test_workplace/flink/src/main/resources/hello.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Long>> resultStream = inputDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 按空格分词
            String[] words = line.split(" ");
            // 遍历所有word，包成二元组输出
            for (String str : words) {
                out.collect(Tuple2.of(str, 1L));
            }
        })
                // 如果flatmap使用lambda表达式 必须显式只订购返回值类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(item -> item.f0)
                // 按照二元组第二个位置上的数据求和
                .sum(1);

        resultStream.print();

        // 执行任务
        env.execute();
    }
}
