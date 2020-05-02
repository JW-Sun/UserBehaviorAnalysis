package com.jw;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

// 输入数据样例类
class MarketingUserBehavior {
    private String userId; // 用户Id
    private String behavior; // 用户行为
    private String channel; // 渠道
    private Long timeStamp; // 时间戳

    public MarketingUserBehavior() {}

    public MarketingUserBehavior(String userId, String behavior, String channel, Long timeStamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timeStamp = timeStamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }
}

// 输出结果的样例类
class MarketingViewCount {
    private String windowStart;
    private String windowEnd;
    private String channel;
    private String behavior;
    private Long count;

    public MarketingViewCount() {
    }

    public MarketingViewCount(String windowStart, String windowEnd, String channel, String behavior, Long count) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.channel = channel;
        this.behavior = behavior;
        this.count = count;
    }

    @Override
    public String toString() {
        return "MarketingViewCount{" +
                "windowStart='" + windowStart + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", channel='" + channel + '\'' +
                ", behavior='" + behavior + '\'' +
                ", count=" + count +
                '}';
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}

/**/
public class AppMarketingAggr01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<MarketingViewCount> res = env.addSource(new SimulatedEventSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimeStamp();
                    }
                })
                // 在1个小时内所有渠道的值
                .filter(new FilterFunction<MarketingUserBehavior>() {
                    public boolean filter(MarketingUserBehavior value) throws Exception {
                        return !"uninstall".equals(value.getBehavior());
                    }
                })
                // 转换成(_, 1) 的形式
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<>("key", 1L);
                    }
                })
                // 以渠道和行为类型作为key进行分组
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.hours(1), Time.seconds(10))
                .aggregate(new CountAgg(), new MarketingCountTotal());

        res.print();

        env.execute();
    }


    // 自定义数据源
    private static class SimulatedEventSource extends RichSourceFunction<MarketingUserBehavior> {

        private volatile boolean running = true;

        // 定义用户行为的集合
        String[] behaviors = {"click", "download", "install", "uninstall"};

        //定义渠道的集合
        String[] channels = {"wechat", "weibo", "appstore", "qq", "android", "huawei", "xiaomi"};

        // 随机数发生器
        Random random = new Random();

        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            // 定义一个生成数据的上限
            Long maxv = Long.MAX_VALUE;
            long count = 0L;


            // 随机生成所有数据
            while (running && count < maxv) {
                String userId = UUID.randomUUID().toString();
                String behavior = behaviors[random.nextInt(behaviors.length)];
                String channel = channels[random.nextInt(channels.length)];
                Long timeStamp = System.currentTimeMillis();
                ctx.collect(new MarketingUserBehavior(userId, behavior, channel, timeStamp));
                count++;
                TimeUnit.MILLISECONDS.sleep(10);
            }
        }

        public void cancel() {
            running = false;
        }
    }

    // 自定义的聚合函数的累加器行为
    private static class CountAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class MarketingCountTotal implements WindowFunction<Long, MarketingViewCount, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> in, Collector<MarketingViewCount> out) throws Exception {
            String start = new Timestamp(window.getStart()).toString();
            String end = new Timestamp(window.getEnd()).toString();
            long count = in.iterator().next();
            out.collect(new MarketingViewCount(start, end, "channel", "total", count));
        }
    }
}
