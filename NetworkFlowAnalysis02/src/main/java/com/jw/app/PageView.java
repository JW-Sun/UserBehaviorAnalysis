package com.jw.app;

import com.jw.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.InputStream;
import java.net.URL;

/*计算页面的浏览*/
public class PageView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //相对路径定义数据源

        DataStream<String> input = env.readTextFile("E:\\JavaIDEAProject\\ProjectGroup\\UserBehaviorAnalysis\\NetworkFlowAnalysis02\\src\\main\\java\\com\\jw\\data\\UserBehavior.csv");

        DataStream<Tuple2<String, Integer>> pv = input.map(new MapFunction<String, UserBehavior>() {
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
        .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>("pv", 1);
            }
        })
        .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        })
        .timeWindow(Time.hours(1))
        .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });

        pv.print("pv");

        env.execute("pv");
    }
}
