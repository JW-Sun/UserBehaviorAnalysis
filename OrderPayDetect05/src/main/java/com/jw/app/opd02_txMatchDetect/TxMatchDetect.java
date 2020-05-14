package com.jw.app.opd02_txMatchDetect;

import com.jw.app.opd01_timeout.OrderTimeOut;
import com.jw.app.opd01_timeout.OrderTimeoutWithoutCep;
import com.jw.entity.OrderEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import sun.awt.TimedWindowEvent;

import java.net.URL;

public class TxMatchDetect {

    // 订单有，支付信息没有
    private static final OutputTag<OrderEvent> unMatchPay = new OutputTag<OrderEvent>("unMatchedPays") {};
    // 订单没有，但是支付信息有
    private static final OutputTag<ReceiptEvent> unMatchReceipt = new OutputTag<ReceiptEvent>("unMatchOrder") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 第一条数据流
        URL orderLogUrl = TxMatchDetect.class.getResource("/OrderLog.csv");
        DataStreamSource<String> source = env.readTextFile(orderLogUrl.getPath());

        // orderLog 按照交易事务ID进行keyBy
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
                .filter(new FilterFunction<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.getTxId().length() > 0;
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
                    @Override
                    public String getKey(OrderEvent value) throws Exception {
                        return value.getTxId();
                    }
                });

        // 支付到账的事件流 txid, payChannel,  time
        URL receiptLogUrl = TxMatchDetect.class.getResource("/ReceiptLog.csv");
        DataStreamSource<String> source1 = env.readTextFile(receiptLogUrl.getPath());
        KeyedStream<ReceiptEvent, String> txIdStream = source1
                .map(new MapFunction<String, ReceiptEvent>() {
                    @Override
                    public ReceiptEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new ReceiptEvent(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                })
                .keyBy(new KeySelector<ReceiptEvent, String>() {
                    @Override
                    public String getKey(ReceiptEvent value) throws Exception {
                        return value.getTxId();
                    }
                });

        // 将两条流连接起来共同处理
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> process = orderEventStream
                .connect(txIdStream)
                .process(new txPayMatch());

        process.print("匹配成功");
        process.getSideOutput(unMatchPay).print("没有获得匹配的Order");
        process.getSideOutput(unMatchReceipt).print("没有获得匹配的Receipt");

        env.execute();
    }

    private static class txPayMatch extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        // 定义状态来保存已经到达的订单支付时间和到账事件
        private ValueState<OrderEvent> payState;
        private ValueState<ReceiptEvent> receiptState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payState", OrderEvent.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receiptState", ReceiptEvent.class));
        }

        // 订单支付事件数据的处理
        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 先判断有没有对应的到账事件
            // 没有对应的到账
            if (receiptState.value() == null) {
                // 就要将pay存入状态，注册定时器等待
                payState.update(value);
                ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000L + 5000L);
            } else {
                // 如果有到账事件，在主流进行输出匹配信息。
                ReceiptEvent receipt = receiptState.value();
                out.collect(new Tuple2<>(value, receipt));
                // 清空状态。
                receiptState.clear();
            }
        }

        // 到账时间的处理
        @Override
        public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 有没有对应的pay
            // 没有
            if (payState.value() == null) {
                receiptState.update(value);
                ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000L + 5000L);
            } else {
                OrderEvent order = payState.value();
                out.collect(new Tuple2<>(order, value));
                payState.clear();
            }
        }

        // 时间到对应的数据还没到就会发生报警
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            if (payState.value() != null) {
                // pay不为空，证明receipt对账是没有来的
                // 侧输出没有匹配的pay记录
                ctx.output(unMatchPay, payState.value());

            }

            payState.clear();

            if (receiptState.value() != null) {
                // 说明对应的pay没有来
                ctx.output(unMatchReceipt, receiptState.value());

            }
            receiptState.clear();
        }
    }
}
