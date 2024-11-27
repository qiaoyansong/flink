package watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/26 22:32
 * description：
 */
public class WaterMarkIdleDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 设置全局并行度为2
        env.setParallelism(2);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        // 将数据发放到下游算子 map的指定分区
        DataStream<String> stringDataStream = dataStreamSource.partitionCustom(new Partitioner<Integer>() {

            @Override
            public int partition(Integer key, int numPartitions) {
                return key % 2;
            }
        }, new KeySelector<String, Integer>() {
            @Override
            public Integer getKey(String value) throws Exception {
                return Integer.valueOf(value);
            }
        });

        // map算子生成水位线
        SingleOutputStreamOperator<Integer> waterSensorSingleOutputStreamOperator = stringDataStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Integer>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Integer>() {
                            @Override
                            public long extractTimestamp(Integer element, long recordTimestamp) {
                                return element * 1000L;
                            }
                        }).withIdleness(Duration.ofSeconds(5))); // 空闲等待5s

        // 每10s进行一次开窗聚合
        SingleOutputStreamOperator<String> process = waterSensorSingleOutputStreamOperator.keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {

                    /**
                     * @param s 分组的key
                     * @param collector 输出数据的采集器
                     * @param context 上下文
                     * @param iterable 一个窗口所存储的数据
                     */
                    @Override
                    public void process(Integer s, Context context, Iterable<Integer> iterable, Collector<String> collector) throws Exception {
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
