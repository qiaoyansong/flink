package split;

import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/22 22:03
 * description：
 */
public class SideOutputDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<String> source = env.fromElements("1", "2", "3", "4");


        /**
         * 泛型代表 侧输出流中元素的类型
         * id 代表侧输出流的名字
         */
        OutputTag<String> s1Tag = new OutputTag<>("s1", Types.STRING());

        /**
         * 第一个泛型代表输入的类型
         * 第二个泛型代表输出流 主流的类型
         */
        SingleOutputStreamOperator<String> process = source.process(new ProcessFunction<String, String>() {

            /**
             * collector 是输出流主流的类型
             */
            @Override
            public void processElement(String s, Context ctx, Collector<String> collector) throws Exception {
                if (Integer.valueOf(s) % 2 == 1) {
                    // 奇数流
                    // 参数1 代表侧输出流的tag
                    // 参数2 代表侧输出流的数据
                    ctx.output(s1Tag, s);
                } else {
                    // 主流 放偶数
                    collector.collect(s);
                }
            }
        });

        process.print("主流");
        process.getSideOutput(s1Tag).print("奇数流");

        env.execute();
    }
}
