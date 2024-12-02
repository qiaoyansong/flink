package top_n;

import bean.Event;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2024/12/2 15:02
 * description：统计 需要统计最近10 秒钟内最热门的两个 url 链接，并且每 5 秒钟更新一次。
 */
public class AggrWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 全局并行度设置为1，否则可能窗口不会触发(水位线的传递，多个分组watermark 取最小，当然可以设置空闲时间)
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


        // 因为需要统计热门的URL，因此需要将相同URL分到同组，方便聚合
        // 不过需要注意的是 keyBy会将数据分组，可能相同窗口的数据 会被分发到不同的并行子任务
        // 因此后续需要将相同窗口的 输出数据进行汇总 然后取最大值 输出
        SingleOutputStreamOperator<UrlClickInfoBO> urlClickDataStream = waterSensorSingleOutputStreamOperator.keyBy(event -> event.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlClickAgg(), new ProcessWindowFunction<Integer, UrlClickInfoBO, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Integer> iterable, Collector<UrlClickInfoBO> collector) throws Exception {
                        TimeWindow window = context.window();
                        System.out.println("key=" + key + "的[" + window.getStart() + "," + window.getEnd() + ")的窗口触发");
                        UrlClickInfoBO urlClickInfoBO = new UrlClickInfoBO(key, iterable.iterator().next(), window.getStart(), window.getEnd());
                        collector.collect(urlClickInfoBO);
                    }
                });

        // 将相同窗口的，路由到相同的并行子任务。
        // 上面 窗口算子触发后 理论上需要立即输出
        urlClickDataStream.keyBy(urlClickBO -> urlClickBO.windowEnd)
                .process(new KeyedProcessFunction<Long, UrlClickInfoBO, String>() {

                    Map<Long, List<UrlClickInfoBO>> windowEndTime2ClickBOListMap = new HashMap<>();

                    /**
                     * 因为是每来一条数据，处理一下，因此需要把相同窗口的数据存储起来
                     */
                    @Override
                    public void processElement(UrlClickInfoBO urlClickInfoBO, Context context, Collector<String> collector) throws Exception {
                        // 放数据
                        if (windowEndTime2ClickBOListMap.containsKey(urlClickInfoBO.windowEnd)) {
                            windowEndTime2ClickBOListMap.get(urlClickInfoBO.windowEnd).add(urlClickInfoBO);
                        } else {
                            List<UrlClickInfoBO> temp = new ArrayList<>();
                            temp.add(urlClickInfoBO);
                            windowEndTime2ClickBOListMap.put(urlClickInfoBO.windowEnd, temp);
                        }

                        // 什么时候触发？可以使用定时器 过一段时间之后将map的数据取出，进行求最大值
                        TimerService timerService = context.timerService();
                        // 设置当前窗口end + 1ms，主要原因是如果当前并行子任务的 watermark到达了 xms。就可以认为x ms之前的数据都到了。
                        timerService.registerEventTimeTimer(urlClickInfoBO.windowEnd + 1);
                        System.out.println("key=" + context.getCurrentKey() + "设置事件时间为" + (urlClickInfoBO.windowEnd + 1) + "的定时器");
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("key=" + ctx.getCurrentKey() + "timestamp=" + timestamp + "的定时器触发");
                        List<UrlClickInfoBO> urlClickInfoBOS = windowEndTime2ClickBOListMap.get(ctx.getCurrentKey());

                        List<UrlClickInfoBO> tempResult = urlClickInfoBOS.stream()
                                .sorted(new Comparator<UrlClickInfoBO>() {
                                    @Override
                                    public int compare(UrlClickInfoBO o1, UrlClickInfoBO o2) {
                                        return o2.clinkCount - o1.clinkCount;
                                    }
                                })
                                .limit(2)
                                .collect(Collectors.toList());

                        StringBuilder result = new StringBuilder();
                        result.append("========================================\n");
                        for (int i = 0; i < tempResult.size(); i++) {
                            UrlClickInfoBO temp = tempResult.get(i);
                            String info = "浏览量 No." + (i + 1) +
                                    " url：" + temp.url +
                                    " 浏览量：" + temp.clinkCount +
                                    " 窗 口 结 束 时 间 ： " + DateFormatUtils.format(temp.windowEnd, "yyyy-MM-dd HH-mm-ss.SSS") +
                                    "\n";
                            result.append(info);
                        }

                        result.append("========================================\n");
                        out.collect(result.toString());
                    }
                }).print();

        env.execute();
    }
}

/**
 * 统计某个URL的点击量
 */
class UrlClickAgg implements AggregateFunction<Event, Integer, Integer> {

    /**
     * 初始化累加器，创建窗口时调用
     */
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    /**
     * 聚合逻辑,相同key
     */
    @Override
    public Integer add(Event value, Integer accumulator) {
        return accumulator + 1;
    }

    /**
     * 获取最终结果，窗口触发时输出
     */
    @Override
    public Integer getResult(Integer accumulator) {
        return accumulator;
    }

    /**
     * 只有会话窗口会用到
     */
    @Override
    public Integer merge(Integer a, Integer b) {
        return null;
    }
}

class UrlClickInfoBO {

    public String url;

    public Integer clinkCount;

    public Long windowStart;

    public Long windowEnd;

    public UrlClickInfoBO() {
    }

    public UrlClickInfoBO(String url, Integer clinkCount, Long windowStart, Long windowEnd) {
        this.url = url;
        this.clinkCount = clinkCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UrlClickInfoBO)) return false;

        UrlClickInfoBO that = (UrlClickInfoBO) o;

        if (!url.equals(that.url)) return false;
        if (!clinkCount.equals(that.clinkCount)) return false;
        if (!windowStart.equals(that.windowStart)) return false;
        return windowEnd.equals(that.windowEnd);
    }

    @Override
    public int hashCode() {
        int result = url.hashCode();
        result = 31 * result + clinkCount.hashCode();
        result = 31 * result + windowStart.hashCode();
        result = 31 * result + windowEnd.hashCode();
        return result;
    }
}
