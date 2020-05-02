package com.jw.app;

import com.jw.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/*网站独立访客数*/
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //相对路径定义数据源

        DataStream<String> input = env.readTextFile("E:\\JavaIDEAProject\\ProjectGroup\\UserBehaviorAnalysis\\NetworkFlowAnalysis02\\src\\main\\java\\com\\jw\\data\\UserBehavior.csv");

        DataStream<UvCount> res = input.map(new MapFunction<String, UserBehavior>() {
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
                .timeWindowAll(Time.hours(1)).apply(new UvCountByWindow());

        res.print();

        env.execute();
    }

    /*这里是直接定义了一个window的function函数*/
    private static class UvCountByWindow implements AllWindowFunction<UserBehavior, UvCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> input, Collector<UvCount> out) throws Exception {
            //定义HashSet来保存所有数据并且去重
            Set<Long> idSet = new HashSet<>();

            //将当前窗口的所有数据收集到Set,最后输出set的size
            for (UserBehavior userBehavior : input) {
                idSet.add(userBehavior.getUserId());
            }

            out.collect(new UvCount(window.getEnd(), (long) idSet.size()));

            /*如果一个窗口内的数据有几亿条，就需要使用到布隆过滤器*/
        }
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
}


