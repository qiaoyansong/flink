package top_n;

import bean.Event;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/12/1 15:40
 * description：
 */
public class ProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8993);

        SingleOutputStreamOperator<Event> waterSensorSingleOutputStreamOperator = dataStreamSource.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                String[] split = value.split(",");
                return new Event(split[0], split[1], Long.valueOf(split[2]));
            }
        })
                .assignTimestampsAndWatermarks(
                        // 指定watermark生成，最大乱序程度3s
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                // 指定时间戳分配器 从数据中提取
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp * 1000;
                                    }
                                }));

        // 滑动窗口 窗口大小10s 步长5s
        waterSensorSingleOutputStreamOperator.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))).process(new ProcessAllWindowFunction<Event, String, TimeWindow>() {

            @Override
            public void process(Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
                // 全窗口函数 窗口触发的时候调用一次，因此只会初始化一次
                Map<String, Integer> url2ClickCountMap = new HashMap<>();

                iterable.forEach(event -> {
                    if (!url2ClickCountMap.containsKey(event.url)) {
                        url2ClickCountMap.put(event.url, 1);
                    } else {
                        url2ClickCountMap.put(event.url, url2ClickCountMap.get(event.url) + 1);
                    }
                });

                List<Tuple2<String, Integer>> tempResult = url2ClickCountMap.keySet().stream()
                        .map(url -> Tuple2.of(url, url2ClickCountMap.get(url)))
                        .sorted(new Comparator<Tuple2<String, Integer>>() {
                            @Override
                            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                                return o2.f1 - o1.f1;
                            }
                        })
                        .limit(2)
                        .collect(Collectors.toList());

                StringBuilder result = new StringBuilder();
                result.append("========================================\n");
                for (int i = 0; i < tempResult.size(); i++) {
                    Tuple2<String, Integer> temp = tempResult.get(i);
                    String info = "浏览量 No." + (i + 1) +
                            " url：" + temp.f0 +
                            " 浏览量：" + temp.f1 +
                            " 窗 口 结 束 时 间 ： " + DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH-mm-ss.SSS") +
                            "url2ClickCountMap:" + url2ClickCountMap.toString() +
                            "\n";
                    result.append(info);
                }

                result.append("========================================\n");
                collector.collect(result.toString());
            }

        }).print();


        env.execute();
    }
}
