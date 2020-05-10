package com.jw.app;

import com.jw.entity.LoginEvent;
import com.jw.entity.Warning;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/* 两秒内同一个人登录失败超过两次就进行报警 */
public class LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        DataStreamSource<String> source = env.readTextFile(resource.getPath());

        DataStream<LoginEvent> loginEventStream = source
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
                });

        /*判断每一个userId做分组*/
        DataStream<Warning> loginFailUserStream = loginEventStream
                .keyBy(new KeySelector<LoginEvent, Long>() {
                    public Long getKey(LoginEvent value) throws Exception {
                        return value.getUserId();
                    }
                })
                // 设置登录失败次数为2
                .process(new LoginWarning(2));

        loginFailUserStream.print();

        env.execute();

    }

    private static class LoginWarning extends KeyedProcessFunction<Long, LoginEvent, Warning> {
        private int maxi;

        public LoginWarning(int i) {
            this.maxi = i;
        }

        // 来一个登录失败就放入List中，第一个放进来的时候就设置定时器
        // 定义状态，保存2s内的所有登录失败事件
        private ListState<LoginEvent> loginFailState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            loginFailState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-state", LoginEvent.class));
        }

        /*来的数据带着时间戳，直接与前一个数据进行时间比较，如果时间差在2s内就直接报警，超过两秒就不管了*/

        public void processElement(LoginEvent input, Context ctx, Collector<Warning> out) throws Exception {
            /*
            Iterable<LoginEvent> loginEvents = loginFailState.get();

            // 判断事件是否为fail
            if ("fail".equals(input.getEventType())) {
                // 判断状态List是否有数据
                if (!loginEvents.iterator().hasNext()) {
                    // 如果没有就设置定时器, 触发的时间就设置在2s后
                    ctx.timerService().registerEventTimeTimer(input.getEventTime() * 1000L + 2000L);
                }
                loginFailState.add(input);
            } else {
                // 如果是成功直接清空状态
                loginFailState.clear();
            }
             */

            // 失败进行处理，判断之前是否有登陆失败事件，存在loginFailState
            if ("fail".equals(input.getEventType())) {
                Iterator<LoginEvent> iterator = loginFailState.get().iterator();
                if (iterator.hasNext()) {
                    // 之前有过登录失败的事件，就比较事件的时间的间距
                    LoginEvent pre = iterator.next();
                    if (Math.abs(input.getEventTime() - pre.getEventTime()) < 2) {
                        // 间隔小于2s输出报警信息
                        out.collect(new Warning(
                                input.getUserId(),
                                pre.getEventTime(),
                                input.getEventTime(),
                                "login fail in 2 times"));
                    }
                    loginFailState.clear();
                    loginFailState.add(input);
                } else {
                    // 之前没有过登录失败的事件，那么就直接添加到状态
                    loginFailState.add(input);
                }
            } else {
                // 清空状态
                loginFailState.clear();
            }
        }

        /***
         * 触发定时器
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        /*@Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Warning> out) throws Exception {
            // 根据定时器的时候，根据状态中的失败个数来决定是否输出报警
            List<LoginEvent> allLoginFails = new ArrayList<>();
            Iterator<LoginEvent> iterator = loginFailState.get().iterator();
            while (iterator.hasNext()) {
                allLoginFails.add(iterator.next());
            }
            // 判断个数
            if (allLoginFails.size() >= maxi) {
                Long userId = ctx.getCurrentKey();
                out.collect(new Warning(
                        userId,
                        allLoginFails.get(0).getEventTime(),
                        allLoginFails.get(allLoginFails.size() - 1).getEventTime(),
                        "login fail in 2 seconds for " + allLoginFails.size() + " times."));
            }

            // 清空
            loginFailState.clear();
        }*/
    }
}
