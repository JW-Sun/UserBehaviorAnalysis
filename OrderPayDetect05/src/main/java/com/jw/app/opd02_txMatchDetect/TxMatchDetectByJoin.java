package com.jw.app.opd02_txMatchDetect;

import com.jw.entity.OrderEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;

public class TxMatchDetectByJoin {
    public static void main(String[] args) {
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

        /* 直接做Join操作 */
        orderEventStream
                .intervalJoin(txIdStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new TxPayMatchByJoin());

    }

    private static class TxPayMatchByJoin extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {



        @Override
        public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            out.collect(new Tuple2<>(left, right));
        }
    }
}
