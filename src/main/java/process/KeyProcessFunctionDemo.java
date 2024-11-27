package process;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/27 23:07
 * description：
 */
public class KeyProcessFunctionDemo {
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
                                        return element.getTs() * 1000;
                                    }
                                }));

        // 每10s进行一次开窗聚合
        waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    /**
                     * 每一个数据到来的时候处理
                     * @param collector
                     * @param context
                     *
                     */
                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        // 数据提取出来的事件时间，如果没定义事件时间的提取方法 为null
                        Long timestamp = context.timestamp();

                        TimerService timerService = context.timerService();
                        // 注册事件时间定时器
                        timerService.registerEventTimeTimer(5000);
                        // 注册处理事件定时器
//                        timerService.registerProcessingTimeTimer();

                        // 删除定时器
//                        timerService.deleteEventTimeTimer();
//                        timerService.deleteProcessingTimeTimer();
                        System.out.println("当前key=" + context.getCurrentKey() + "时间戳=" + context.timestamp() + "注册了一个5s定时器");
                    }

                    /**
                     * 时间进展到定时器注册的时间的时候，触发
                     * @param ctx 上下文
                     * @param out 采集器
                     * @param timestamp 当前时间进展
                     */
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("现在时间是" + timestamp + "定时器触发");
                    }

                }).print();


        env.execute();
    }
}
