package com.jw.app;

import com.jw.bean.UserBehavior;
import com.jw.utils.RedisUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

/*运用布隆过滤器（位图）来判断用户是否已经计算过了*/
public class UniqueVisitorWithBuLongFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //相对路径定义数据源

        DataStream<String> input = env.readTextFile("E:\\JavaIDEAProject\\ProjectGroup\\UserBehaviorAnalysis\\NetworkFlowAnalysis02\\src\\main\\java\\com\\jw\\data\\UserBehavior.csv");

        SingleOutputStreamOperator<UvCount> res = input.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] splitStr = value.split(",");
                return new UserBehavior(Long.parseLong(splitStr[0].trim()),
                        Long.parseLong(splitStr[1].trim()),
                        Integer.parseInt(splitStr[2].trim()),
                        splitStr[3].trim(),
                        Long.parseLong(splitStr[4].trim()));
            }
        })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimeStamp() * 1000L;
                    }
                })
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return "pv".equals(value.getBehavior());
                    }
                })
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return new Tuple2<>("dummyKey", value.getUserId());
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.hours(1))
                // 如果要存入redis, 在窗口关闭的时候进行调用redis的连接，放到bitmap中。窗口无法全部保存数据
                // 因为窗口数据放不下所以要进行来一次数据触发一次窗口操作，所以要自定义触发器。
                .trigger(new MyTrigger())
                .process(new UvCountWithBloom());

        res.print();

        env.execute();
    }

    static class UvCount {
        private Long windowEnd;
        private Long uvCount;

        public UvCount() {
        }

        public UvCount(Long windowEnd, Long uvCount) {
            this.windowEnd = windowEnd;
            this.uvCount = uvCount;
        }

        public Long getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(Long windowEnd) {
            this.windowEnd = windowEnd;
        }

        public Long getUvCount() {
            return uvCount;
        }

        public void setUvCount(Long uvCount) {
            this.uvCount = uvCount;
        }

        @Override
        public String toString() {
            return "UvCount{" +
                    "windowEnd=" + windowEnd +
                    ", uvCount=" + uvCount +
                    '}';
        }
    }

    // 自定义窗口触发器Trigger传入的参数就是传入的元素和TimeWindow
    private static class MyTrigger extends Trigger<Tuple2<String, Long>, TimeWindow> {
        @Override
        public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每来一条数据就直接出发窗口操作并清空窗口状态
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class UvCountWithBloom extends ProcessWindowFunction<Tuple2<String, Long>, UvCount, String, TimeWindow> {

        // 定义redis连接
        /*redis 一方面存的是位图，key是windowEnd，offset的值就是经过哈希计算之后的值，offset对应的数代表是否存在
        * 哈希表 名为count， key为windowEnd， value为count
        * */

        private Jedis jedis;
        private Bloom bloom;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.jedis = new Jedis("localhost", 6379, 100000);
            this.bloom = new Bloom(1 << 29);
        }

        public UvCountWithBloom() {

        }

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple2<String, Long>> elements,
                            Collector<UvCount> out) throws Exception {
            // 在redis中位图的存储方式 key是windowEnd value是bitmap
            // 每个窗口的end时间都当做一个redis的操作
            String storeKey = String.valueOf(context.window().getEnd());
            long count = 0L;
            // 把每个窗口的uv count值存入名为count的redis哈希表，存放内容为(windowEnd -> uvCount)所以先从redis中进行读取
            if (jedis.hget("count", storeKey) != null) {
                count = Long.parseLong(jedis.hget("count", storeKey));
            }

            // 用布隆过滤器判断当前用户是否已经存在
            String userId = String.valueOf(elements.iterator().next().f1);
            long offset = bloom.hash(userId, 61);

            // 定义一个标志位，判断redis位图中有没有这一位
            boolean isExist = jedis.getbit(storeKey, offset);
            // 不存在那么久将对应位图的对应位置setbit为1
            if (!isExist) {
                jedis.setbit(storeKey, offset, true);
                jedis.hset("count", storeKey, String.valueOf(count + 1));
                out.collect(new UvCount(Long.parseLong(storeKey), count + 1));
            } else {
                // 如果存在，位图也不变化，哈希表也不发生变化
                out.collect(new UvCount(Long.parseLong(storeKey), count));
            }
        }
    }

    // 定义一个布隆过滤器
    private static class Bloom implements Serializable {
        // 位图的总大小，位的数量
        private long size;
        private long cap;

        public Bloom() {}

        public Bloom(long size) {
            this.size = size;
            this.cap = size > 0 ? size : 1 << 27;
        }

        // 定义哈希函数
        public long hash(String value, int seed) {
            long res = 0;
            for (int i = 0; i < value.length(); i++) {
                res = res * seed + value.charAt(i);
            }
            return res & (cap - 1);
        }
    }

}
