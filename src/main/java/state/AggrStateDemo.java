package state;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2024/12/5 15:06
 * description：
 */
public class AggrStateDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8888);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
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

                    AggregatingState<Integer, Double> vcAvgState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcAvgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>("vcAvgState",
                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0, 0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                        return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        return accumulator.f0 * 1.0D / accumulator.f1;
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                        return null;
                                    }
                                }, Types.TUPLE(Types.INT, Types.INT)));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        vcAvgState.add(waterSensor.getVc());
                        collector.collect("传感器ID为" + context.getCurrentKey() + "的平均水位为" + vcAvgState.get());
                    }
                }).print();

        env.execute();
    }
}
