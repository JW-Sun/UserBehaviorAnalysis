package com.jw.app;

import com.jw.entity.OrderEvent;
import com.jw.entity.OrderResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class OrderTimeOut {

    // 侧输出流输出orderResult的报警信息
    public static final OutputTag<OrderResult> orderTimeoutOutputTag = new OutputTag<OrderResult>("orderTimeout"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        URL resource = OrderTimeOut.class.getResource("/OrderLog.csv");
        DataStreamSource<String> source = env.readTextFile(resource.getPath());

        KeyedStream<OrderEvent, String> orderEventStream = source
                .map(new MapFunction<String, OrderEvent>() {
                    public OrderEvent map(String s) throws Exception {
                        String[] split = s.split(",");
                        String orderId = split[0].trim();
                        String type = split[1].trim();
                        String txId = split[2].trim();
                        Long eventTime = Long.valueOf(split[3].trim());
                        return new OrderEvent(orderId, type, txId, eventTime);
                    }
                })
                // 都是升序
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                })
                .keyBy(new KeySelector<OrderEvent, String>() {
                    public String getKey(OrderEvent value) throws Exception {
                        return value.getOrderId();
                    }
                });

        // 开始要是create，非严格后面接这pay，在15分钟内
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern
                .<OrderEvent>begin("begin")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("follow")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        // 3. 把模式应用道stream上，得到一个pattern stream
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream, orderPayPattern);

        // 4. 调用select方法，提取时间序列，超时时间要报警提示
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream
                .select(orderTimeoutOutputTag, new MyOrderTimeoutFunction(), new MyPatternSelectFunction());

        resultStream.print("payed");
        resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout");

        env.execute();
    }

    /***
     * 实现超时事件序列处理函数
     * 也就是提取转换操作
     */
    private static class MyOrderTimeoutFunction implements PatternTimeoutFunction<OrderEvent, OrderResult> {
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            String orderId = pattern.get("begin").iterator().next().getOrderId();
            String resultMsg = "timeout";
            return new OrderResult(orderId, resultMsg);
        }
    }


    /***
     * 自定义正常支付事件序列处理函数
     */
    private static class MyPatternSelectFunction implements PatternSelectFunction<OrderEvent, OrderResult> {
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            String follow = pattern.get("follow").iterator().next().getOrderId();
            return new OrderResult(follow, "payed success");
        }
    }
}
