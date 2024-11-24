package window;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/24 11:22
 * description：
 */
public class WindowAggrDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
            }
        });

        // 每5s进行一次开窗聚合
        SingleOutputStreamOperator<String> aggr = waterSensorSingleOutputStreamOperator.keyBy(s -> s.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                /**
                 * 三个泛型分别代表 输入数据的类型  中间计算结果的类型 输出数据的类型
                 */
                .aggregate(new AggregateFunction<WaterSensor, Integer, String>() {

                    /**
                     * 初始化累加器，创建窗口时调用
                     */
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("创建累加器");
                        return 0;
                    }

                    /**
                     * 聚合逻辑
                     */
                    @Override
                    public Integer add(WaterSensor value, Integer accumulator) {
                        System.out.println("调用add方法");
                        return accumulator + value.getVc();
                    }

                    /**
                     * 获取最终结果，窗口触发时输出
                     */
                    @Override
                    public String getResult(Integer accumulator) {
                        System.out.println("调用getResult方法");
                        return accumulator.toString();
                    }

                    /**
                     * 只有会话窗口会用到
                     */
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        System.out.println("调用merge方法");
                        return null;
                    }
                });

        aggr.print();

        env.execute();
    }
}
