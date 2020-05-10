package com.jw.app;

import com.jw.entity.LoginEvent;
import com.jw.entity.Warning;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class CEPLoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        DataStreamSource<String> source = env.readTextFile(resource.getPath());

        KeyedStream<LoginEvent, Long> loginEventStream = source
                .map(new MapFunction<String, LoginEvent>() {
                    public LoginEvent map(String s) throws Exception {
                        String[] split = s.split(",");
                        Long userId = Long.valueOf(split[0].trim());
                        String ip = split[1].trim();
                        String eventType = split[2].trim();
                        Long eventTime = Long.valueOf(split[3].trim());
                        return new LoginEvent(userId, ip, eventType, eventTime);
                    }
                })
                // 当前数据可能有乱序数据，设置watermaker延迟
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                })
                .keyBy(new KeySelector<LoginEvent, Long>() {
                    @Override
                    public Long getKey(LoginEvent value) throws Exception {
                        return value.getUserId();
                    }
                });

        // 定义匹配模式
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern
                .<LoginEvent>begin("begin")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .next("next")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .within(Time.seconds(2));

        /* 时间流上进行应用，得到一个pattern stream */
                // 使用CEP的相关模式进行操作
                // 定义匹配模式
        PatternStream<LoginEvent> pattern = CEP.pattern(loginEventStream, loginFailPattern);

        /* 从pattern stream上应用select function检测出事件序列 */
        SingleOutputStreamOperator<Warning> failDataStream = pattern.select(new LoginFailMatch());
        failDataStream.print();
        env.execute();
    }

    private static class LoginFailMatch implements PatternSelectFunction<LoginEvent, Warning> {
        @Override
        public Warning select(Map<String, List<LoginEvent>> pattern) throws Exception {
            // 从map中按照名称取出对应的事件
            LoginEvent begin = pattern.get("begin").iterator().next();
            LoginEvent next = pattern.get("next").iterator().next();
            return new Warning(begin.getUserId(), begin.getEventTime(), next.getEventTime(), "login fail");
        }
    }
}
