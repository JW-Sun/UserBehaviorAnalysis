package com.jw.app;

import com.jw.bean.ApacheLogEvent;
import com.jw.bean.UrlViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.events.MappingEndEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.metrics.stats.Count;

import javax.management.monitor.StringMonitor;
import javax.management.remote.rmi._RMIConnection_Stub;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


/***
 * 统计实时流量
 */
public class NetworkFlowApp {

    private static final ThreadLocal<DateFormat> sdf = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        }
    };

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> dataStream = env.readTextFile("E:\\JavaIDEAProject\\ProjectGroup\\UserBehaviorAnalysis\\NetworkFlowAnalysis02\\src\\main\\java\\com\\jw\\data\\apachetest.log");

        DataStream<ApacheLogEvent> apacheLogEventDataStream = dataStream.map(new MapFunction<String, ApacheLogEvent>() {
            public ApacheLogEvent map(String value) throws Exception {
                String[] splitStr = value.split(" ");
                String id = splitStr[0].trim();
                String userId = splitStr[1].trim();
                //定义时间转换
                //得到时间戳
                Long timeStamp = sdf.get().parse(splitStr[3].trim()).getTime();
                String method = splitStr[5].trim();
                String url = splitStr[6].trim();
                return new ApacheLogEvent(id, userId, timeStamp, method, url);
            }
        });

        /*日志出现了乱序的数据，所以说在处理数据的时候使用watermakers*/
        apacheLogEventDataStream = apacheLogEventDataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                //定义提取时间戳的字段
                //因为上一步传进来已经是毫秒级了，因为Date的getTime方法返回的是毫秒，所以说不需要进行乘1000的操作
                return element.getTimeStamp();
            }
        });

        //统计热门url的数量，所以用这个进行做聚合
        DataStream<String> process = apacheLogEventDataStream.keyBy(new KeySelector<ApacheLogEvent, String>() {
            public String getKey(ApacheLogEvent value) throws Exception {
                return value.getUrl();
            }
        })
                .timeWindow(Time.minutes(10), Time.seconds(5)) //10分钟的窗口，每过5s统计一次。
                .allowedLateness(Time.seconds(60)) //允许60s的迟到数据。
                .aggregate(new CountAgg(), new WindowResult()) //先聚合-输出的是url的点击次数，然后是执行windowFunction进行窗口聚合（输入的是url的次数，输出的是封装了windowEnd的UrlViewCount）
                .keyBy(new KeySelector<UrlViewCount, Long>() {
                    public Long getKey(UrlViewCount value) throws Exception {
                        return value.getWindowEnd();
                    }
                })
                .process(new TopHotUrl(5));

        process.print();

        env.execute("top hot url");
    }

    /***
     * 输入是同一个以URL（key）的ApacheLogEvent，输出次数
     */
    private static class CountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        public Long createAccumulator() {
            return 0L;
        }

        public Long add(ApacheLogEvent value, Long accumulator) {
            return accumulator + 1;
        }

        public Long getResult(Long accumulator) {
            return accumulator;
        }

        public Long merge(Long a, Long b) {
            return a + b;
        }
    }


    private static class WindowResult implements WindowFunction<Long, UrlViewCount, String, TimeWindow> {

        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
            out.collect(new UrlViewCount(s, window.getEnd(), input.iterator().next()));
        }
    }

    /*根据不同的窗口排序输出*/
    private static class TopHotUrl extends KeyedProcessFunction<Long, UrlViewCount, String> {

        private int topSize;

        public TopHotUrl(int topsize) {
            this.topSize = topsize;
        }

        //定义一个状态
        private ListState<UrlViewCount> urlState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            urlState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("urlState", UrlViewCount.class));
        }



        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            //来一个加一个
            urlState.add(value);
            //定义一个计时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        /***
         * 时间到出发定时器的操作
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //触发定时器的时候需要进行的操作。
            //状态中拿到所有数据
            List<UrlViewCount> list = new ArrayList<>();

            Iterator<UrlViewCount> iterator = urlState.get().iterator();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }



            Collections.sort(list, new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return (int) (o2.getCount() - o1.getCount());
                }
            });

            List<UrlViewCount> urlViewCounts = list.subList(0, topSize);

            urlState.clear();

            StringBuilder bd = new StringBuilder();
            bd.append("时间： ").append(new Timestamp(timestamp - 1)).append("\n");
            int idx = 1;
            for (UrlViewCount urlViewCount : urlViewCounts) {
                bd.append("第 ").append(idx++).append(" : ")
                .append(" URL: ").append(urlViewCount.getUrl())
                .append(" 访问量: ").append(urlViewCount.getCount()).append("\n");
            }
            bd.append("========================");
            Thread.sleep(1000);

            out.collect(bd.toString());
        }
    }
}
