package com.jw.app;



import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;

// 输入的广告点击事件样例类(对输入数据的样例类)
class AdClickEvent {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;

    public AdClickEvent(){}

    public AdClickEvent(Long userId, Long adId, String province, String city, Long timestamp) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AdClickEvent{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}

// 按照省份统计的输出结果样例类
class CountByProvince {
    private String windowEnd;
    private String province;
    private Long count;

    public CountByProvince() {
    }

    public CountByProvince(String windowEnd, String province, Long count) {
        this.windowEnd = windowEnd;
        this.province = province;
        this.count = count;
    }

    @Override
    public String toString() {
        return "CountByProvince{" +
                "windowEnd=" + windowEnd +
                ", province='" + province + '\'' +
                ", count=" + count +
                '}';
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}

// 黑名单的报警信息
class BlackListWarning {
    private Long userId;
    private Long adId;
    private String msg;

    public BlackListWarning() {}

    public BlackListWarning(Long userId, Long adId, String msg) {
        this.userId = userId;
        this.adId = adId;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "BlackListWarning{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", msg='" + msg + '\'' +
                '}';
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
/*
 *  广告按照省份进行划分的点击量的统计, 用户，广告，省份，时间。
 *
 *  需要有黑名单的机制，在每天的凌晨12点前将黑名单清除，利用状态的编程
 *
 *  黑名单的机制主要就是（用户，广告）这样的元组出现次数超过限制后会将其用户加入到黑名单中，使用keyedProcessFunction
 *  里面使用三种状态分别是点击次数状态，是否发黑名单状态，第二天重置黑名单状态
 *
 *  这个黑名单机制使用的是processingTime的流程，之后的滑动窗口使用的是EventTime。
 *  当（用户，广告）元组第一次来的时候，初始化countState为0，并设置第二天零点的计时器
 *
 *  如果count已经超过限制，这时如果黑名单没有发送过，则进行黑名单的侧输出outputTag。ctx.output() 并且return不继续
 *
 *  如果没超过限制则countState状态+1，并且继续后序的流程。
 *
 *
 *  */
public class AdStatisticByGeo02 {

    // 定义侧输出流的Tag，作用就是将刷点击用户的警告进行侧输出
    private static final OutputTag<BlackListWarning> blackListOutputTag = new OutputTag<BlackListWarning>("blacklist"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // DataStreamSource<String> dataSource = env.readTextFile("E:\\JavaIDEAProject\\ProjectGroup\\UserBehaviorAnalysis\\MarketAnalysis03\\src\\main\\resources\\AdClickLog.csv");
        URL resource = AdStatisticByGeo02.class.getResource("/AdClickLog.csv");
        DataStreamSource<String> source = env.readTextFile(resource.getPath());

        DataStream<AdClickEvent> sourceStream = source
                .map(new MapFunction<String, AdClickEvent>() {
                    @Override
                    public AdClickEvent map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new AdClickEvent(Long.parseLong(split[0]), Long.parseLong(split[1]), split[2], split[3], Long.parseLong(split[4]));
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        /*自定义processFunction，过滤大量的刷点击行为 这里的主要的判断行为就是(用户+广告)的组合点击*/
        SingleOutputStreamOperator<AdClickEvent> process = sourceStream
                // 对用户Id和广告ID进行keyBy
                .keyBy(new KeySelector<AdClickEvent, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdClickEvent value) throws Exception {
                        return new Tuple2<>(value.getUserId(), value.getAdId());
                    }
                })
                // 还需要定义一个侧输出流来对keyedStream进行一个复杂版的filter,设置上限为100
                .process(new FilterBlackListUser(100));

        // 根据省份进行分组，开窗聚合
        SingleOutputStreamOperator<CountByProvince> adCountStream = process
                .keyBy(new KeySelector<AdClickEvent, String>() {
                    @Override
                    public String getKey(AdClickEvent value) throws Exception {
                        return value.getProvince();
                    }
                }) // 按照省份进行keyBy操作
                .timeWindow(Time.hours(1), Time.seconds(5)) // 窗口跨度为1小时，每过5秒钟
                .aggregate(new AdCountAgg(), new AdCountResult());


        adCountStream.print();

        process.getSideOutput(blackListOutputTag).print("blackList");

        System.out.println("start working");
        env.execute();

    }

    // 自定义与聚合函数
    private static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
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

    // 窗口函数的作用就是每一个省的广告数量的具体的操作。
    private static class AdCountResult implements WindowFunction<Long, CountByProvince, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<CountByProvince> out) throws Exception {
            out.collect(new CountByProvince(new Timestamp(window.getEnd()).toString(), key, input.iterator().next()));
        }
    }

    /***
     * 用于对刷点击的用户的过滤
     */
    private static class FilterBlackListUser extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent> {

        private int maxv;


        public FilterBlackListUser(int i) {
            maxv = i;
        }

        // 用于保存状态，点击的
        private ValueState<Long> countState = null;
        // 保存是否发送过黑名单的状态
        private ValueState<Boolean> isSentBlackList = null;
        // 保存定时器出发的时间戳
        private ValueState<Long> resetTimestamp = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            // 定义状态，保存当前用户对当前广告的点击量
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState", Long.class));
            isSentBlackList = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSentBlackList", Boolean.class));
            resetTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<Long>("resetTimestamp", Long.class));
        }

        // 每一条数据来的操作
        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            // 取出count状态
            Long curCount = countState.value();
            if (curCount == null) curCount = 0L;
            if (isSentBlackList.value() == null) isSentBlackList.update(false);


            // 如果当前为0，就是第一次处理，需要定义计时器,每天00：00触发
            if (curCount == 0L) {
                // 得到明天0点的时间戳,并且注册processingTimeTimer定时器,到达明天的时候直接触发定时器
                Long ts = (ctx.timerService().currentProcessingTime() / (1000*60*60*24) + 1) * (1000*60*60*24);
                resetTimestamp.update(ts);
                ctx.timerService().registerProcessingTimeTimer(ts);
            }

            // 计数是否达到上限，达到就加入黑名单
            if (curCount >= maxv) {
                // 判断是否发送过黑名单，只发送一次
                if (!isSentBlackList.value()) {
                    isSentBlackList.update(true);
                    // 输出到侧输出流
                    ctx.output(blackListOutputTag, new BlackListWarning(value.getUserId(), value.getAdId(), "click over " + maxv + " times today"));
                }
                return;
            }
            // 当前的技术状态加1，输出数据到主流中
            countState.update(curCount + 1);
            // 没有超过上限就直接进行输出。
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            // 定时器触发，清空状态
            if (timestamp == resetTimestamp.value()) {
                isSentBlackList.clear();
                countState.clear();
                resetTimestamp.clear();
            }
        }
    }
}
