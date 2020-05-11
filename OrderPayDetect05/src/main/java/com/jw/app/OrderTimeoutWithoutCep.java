package com.jw.app;

import com.jw.entity.OrderEvent;
import com.jw.entity.OrderResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;

import java.net.URL;

public class OrderTimeoutWithoutCep {

    private static final OutputTag<OrderResult> orderTimeoutOutputTag = new OutputTag<OrderResult>("order-timeout"){};

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

        // process function处理超时检测
        // SingleOutputStreamOperator<OrderResult> process = orderEventStream.process(new OrderTimeoutWarning());

        /* 检测所有的数据 */
        SingleOutputStreamOperator<OrderResult> process = orderEventStream.process(new OrderPayMatch());


        process.print("payed");
        process.getSideOutput(orderTimeoutOutputTag).print("timeout");

        env.execute();
    }

    private static class OrderPayMatch extends KeyedProcessFunction<String, OrderEvent, OrderResult> {

        private ValueState<Boolean> isPayState = null;

        private ValueState<Long> timerState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-pay-state", Boolean.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
            if (isPayState.value() == null) {
                isPayState.update(false);
            }
            if (timerState.value() == null) {
                timerState.update(0L);
            }
            Boolean isPay = isPayState.value();
            Long timerTs = timerState.value();

            // 根据事件类型进行分类判断，做不同的处理逻辑
            if ("create".equals(value.getEventType())) {
                // 判断是否有pay来过
                if (isPay) {
                    // pay过，匹配成功，输出主流，清空状态。
                    out.collect(new OrderResult(value.getOrderId(), "payed success"));
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                    timerState.clear();
                    isPayState.clear();
                } else {
                    // 没有pay过，要注册定时器，并且进行等待
                    Long ts = value.getEventTime() * 1000L + 15 * 60 * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerState.update(ts);
                }
            } else if ("pay".equals(value.getEventType())) {
                // create先到，那么就继续判断create过
                if (timerTs > 0) {
                    // 如果定时器有数字，说明已经有create来过
                    // 继续判断是否超过了timeout时间
                    if (timerTs > value.getEventTime() * 1000L) {
                        // 定时器还没有触发就来pay了,那么输出成功匹配信息。
                        out.collect(new OrderResult(value.getOrderId(), "payed success"));
                    } else {
                        // 超时输出到侧输出流
                        ctx.output(orderTimeoutOutputTag, new OrderResult(value.getOrderId(), "payed timeout"));
                    }
                    // 输出结束清空状态。
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                    timerState.clear();
                    isPayState.clear();
                }
                // pay先到，更新状态，注册定时器进行等待
                else {
                    isPayState.update(true);
                    ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000L);
                    timerState.update(value.getEventTime() * 1000L);
                }
            }
        }

        /* create先到，等pay。
        *  pay先到，等create */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 根据状态判断那个数据没来
            if (isPayState.value()) {
                // pay先到，create没等到
                ctx.output(orderTimeoutOutputTag, new OrderResult(ctx.getCurrentKey(), "payed but not get create"));
            } else {
                // create到了，没等到pay
                ctx.output(orderTimeoutOutputTag, new OrderResult(ctx.getCurrentKey(), "order timeout"));
            }
            isPayState.clear();
            timerState.clear();
        }
    }

    /***
     * 自定义处理函数
     */
    /*
    private static class OrderTimeoutWarning extends KeyedProcessFunction<String, OrderEvent, OrderResult> {

        // pay标志是否来过的状态
        private ValueState<Boolean> isPayedState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed-state", Boolean.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
            // 先取出标志的状态位
            Boolean isPayed = isPayedState.value();
            if (isPayed == null) {
                isPayed = false;
            }

            if ("create".equals(value.getEventType()) && !isPayed) {
                // 遇到了create事件，并且pay没有来过，就注册定时器并且开始等待
                ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000L + 15 * 60 * 1000L);
            } else if ("pay".equals(value.getEventType())) {
                isPayedState.update(true);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            Boolean isPayed = isPayedState.value();
            if (isPayed) {
                out.collect(new OrderResult(ctx.getCurrentKey(), "order payed successfully"));
            } else {
                out.collect(new OrderResult(ctx.getCurrentKey(), "order timeout"));
            }
            isPayedState.clear();
        }
    }
     */
}
