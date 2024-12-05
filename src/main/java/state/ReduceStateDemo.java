package state;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2024/12/5 14:49
 * description：
 */
public class ReduceStateDemo {
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
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                // 指定时间戳分配器 从数据中提取
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                                        return element.getTs() * 1000;
                                    }
                                }));


        waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    ReducingState<Integer> vsSumState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vsSumState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("vsSumState",
                                new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                                        return value1 + value2;
                                    }
                                }, Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        vsSumState.add(waterSensor.getVc());
                        collector.collect("传感器ID为" + context.getCurrentKey() + "的水位和为" + vsSumState.get());
                    }
                }).print();

        env.execute();
    }
}
