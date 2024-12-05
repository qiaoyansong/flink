package state;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2024/12/5 11:16
 * description：
 */
public class ListStateDemo {
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

                    ListState<Integer> top3VcList;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        top3VcList = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3VcList", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        // 排序
                        Iterator<Integer> iterator = top3VcList.get().iterator();
                        List<Integer> vcs = new ArrayList<>();
                        while (iterator.hasNext()) {
                            vcs.add(iterator.next());
                        }
                        vcs.add(waterSensor.getVc());

                        // 保留top3
                        vcs = vcs.stream()
                                .sorted((vc1, vc2) -> vc2 - vc1)
                                .collect(Collectors.toList());

                        // 因为数据是一条条进入的，因此每次删掉第四个元素即可
                        if (vcs.size() > 3) {
                            vcs.remove(3);
                        }

                        collector.collect("传感器id=" + context.getCurrentKey() + "的传感器，最高的三个水位值是" + vcs.toString());

                        // 覆盖
                        top3VcList.update(vcs);
                    }
                }).print();

        env.execute();
    }
}
