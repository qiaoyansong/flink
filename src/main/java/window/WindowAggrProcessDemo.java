package window;

import bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/24 11:24
 * description：
 */
public class WindowAggrProcessDemo {
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
                 * 四个泛型 分别代表输入数据的类型  输出数据的类型 分组key的类型 窗口的类型
                 */
                .aggregate(new AggrFunction(), new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
                        // 上下文可以拿到window对象，侧输出流等等
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");

                        // 获取迭代器的元素个数
                        long estimateSize = iterable.spliterator().estimateSize();

                        collector.collect("key=" + key + "的窗口[" + windowStart + ","+ windowEnd + ")包含" + estimateSize + "条数据，分别是" + iterable.toString());
                    }
                });

        aggr.print();

        env.execute();
    }
}

/**
 * 三个泛型分别代表 输入数据的类型  中间计算结果的类型 输出数据的类型
 */
class AggrFunction implements AggregateFunction<WaterSensor, Integer, String> {

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
}

