package state;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2024/12/5 11:39
 * description：
 */
public class MapStateDemo {
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

                    MapState<Integer, Integer> vc2CountMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vc2CountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("vc2CountMapState", Types.INT, Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        vc2CountMapState.put(waterSensor.getVc(), Optional.ofNullable(vc2CountMapState.get(waterSensor.getVc())).orElse(0) + 1);
                        StringBuilder sb = new StringBuilder();
                        sb.append("========================================\n");
                        sb.append("传感器ID=" + context.getCurrentKey() + "的数据为\n");
                        for (Map.Entry<Integer, Integer> entry : vc2CountMapState.entries()) {
                            sb.append(entry.toString()+"\n");
                        }
                        sb.append("========================================\n");
                        collector.collect(sb.toString());
                    }
                }).print();

        env.execute();
    }
}
