package watermark;

import bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/24 14:54
 * description：
 */
public class WaterMarkMonotonousDemo {
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
        })
                .assignTimestampsAndWatermarks(
                        // 指定watermark生成，升序的waterMark，没有等待时间
                        WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        // 指定时间戳分配器 从数据中提取
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                                return element.getTs();
                            }
                        }));

        // 每10s进行一次开窗聚合
        SingleOutputStreamOperator<String> process = waterSensorSingleOutputStreamOperator.keyBy(s -> s.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                /**
                 * 四个泛型 分别代表输入数据的类型  输出数据的类型 分组key的类型 窗口的类型
                 */
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                    /**
                     * @param s 分组的key
                     * @param collector 输出数据的采集器
                     * @param context 上下文
                     * @param iterable 一个窗口所存储的数据
                     */
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                        // 上下文可以拿到window对象，侧输出流等等
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");

                        // 获取迭代器的元素个数
                        long estimateSize = iterable.spliterator().estimateSize();

                        collector.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + estimateSize + "条数据，分别是" + iterable.toString());
                    }
                });

        process.print();

        env.execute();
    }
}
