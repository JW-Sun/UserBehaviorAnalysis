package com.jw.app;

import com.google.common.collect.Iterables;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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
public class AppMarketingByChannel01 {
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
                // 转换成((channel, behavior), 1) 的形式
                .map(new MapFunction<MarketingUserBehavior, Tuple2<Tuple2<String, String>, Long>>() {
                    public Tuple2<Tuple2<String, String>, Long> map(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<Tuple2<String, String>, Long>(new Tuple2<String, String>(value.getChannel(), value.getBehavior()), 1L);
                    }
                })
                // 以渠道和行为类型作为key进行分组
                .keyBy(new KeySelector<Tuple2<Tuple2<String, String>, Long>, Tuple2<String, String>>() {
                    public Tuple2<String, String> getKey(Tuple2<Tuple2<String, String>, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.hours(1), Time.seconds(10))
                .process(new MarketingCountByChannel());

        res.print();

        env.execute();
    }

    // 自定义的处理函数
    private static class MarketingCountByChannel extends ProcessWindowFunction<Tuple2<Tuple2<String, String>, Long>, MarketingViewCount, Tuple2<String, String>, TimeWindow> {

        public void process(Tuple2<String, String> key,
                            Context context,
                            Iterable<Tuple2<Tuple2<String, String>, Long>> in,
                            Collector<MarketingViewCount> out) throws Exception {
            String start = String.valueOf(context.window().getStart());
            String end = String.valueOf(context.window().getEnd());
            String channel = key.f0;
            String behavior = key.f1;
            long count = Iterables.size(in);
            out.collect(new MarketingViewCount(start, end, channel, behavior, count));
        }
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
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }

        public void cancel() {
            running = false;
        }
    }
}
