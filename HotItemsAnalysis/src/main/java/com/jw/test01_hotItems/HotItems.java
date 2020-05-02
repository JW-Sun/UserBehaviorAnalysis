package com.jw.test01_hotItems;

import com.jw.test01_hotItems.bean.ItemViewCount;
import com.jw.test01_hotItems.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;

public class HotItems {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*拼接kafka*/
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.159.102:9092");
        properties.put("group.id", "consumer-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");

        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));

        //2.source读取数据
//        DataStream<String> dataStream = env.readTextFile("E:\\JavaIDEAProject\\ProjectGroup\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        //String map 转换成UserBehavior
        DataStream<UserBehavior> userBehaviorDataStream = dataStream.map(new MapFunction<String, UserBehavior>() {
            public UserBehavior map(String s) throws Exception {
                String[] splitStr = s.split(",");
                return new UserBehavior(Long.parseLong(splitStr[0].trim()),
                        Long.parseLong(splitStr[1].trim()),
                        Integer.parseInt(splitStr[2].trim()),
                        splitStr[3].trim(),
                        Long.parseLong(splitStr[4].trim()));
            }
        });


        //时间戳已经是升序的，所以直接调用相关方法即可
        DataStream<UserBehavior> wm = userBehaviorDataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimeStamp() * 1000L;
            }
        });


        /*3. transform处理数据*/
        //首先进行过滤操作。只有pv才是
        DataStream<String> processedStream = wm.filter(new FilterFunction<UserBehavior>() {
            public boolean filter(UserBehavior value) throws Exception {
                return value.getBehavior().equals("pv");
            }
        })
        .keyBy(new KeySelector<UserBehavior, Long>() {
            public Long getKey(UserBehavior value) throws Exception {
                return value.getItemId();
            }
        })
        .timeWindow(Time.hours(1), Time.minutes(5))
        .aggregate(new CountAgg(), new WindowResult())  //窗口聚合操作
        .keyBy(new KeySelector<ItemViewCount, Long>() {
            public Long getKey(ItemViewCount value) throws Exception {
                return value.getWindowEnd();
            }
        })    //按照窗口分组;
        .process(new TopNHotItems(3));

        /*上面的几步先按照窗口做聚合操作，然后再按窗口进行分组操作，
        然后拿到所有数据进行排序（把每个数据放在状态列表中，设置计时器延迟触发保证所有数据到达）*/

        //控制台输出
        processedStream.print();

        env.execute("hot item job");
    }


    //自定义预聚合函数--三个参数分别代表的就是 输入类型、累加器（状态的类型）、输出的类型
    private static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        //初始状态的初值
        public Long createAccumulator() {
            return 0L;
        }

        //来一个计数器就+1
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        //得到输出结果
        public Long getResult(Long accumulator) {
            return accumulator;
        }


        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    //自定义预聚合函数计算平均数(计算时间戳的平均值)
    private static class AvgAgg implements AggregateFunction<UserBehavior, Tuple2<Long, Integer>, Double> {

        public Tuple2<Long, Integer> createAccumulator() {
            return new Tuple2<Long, Integer>(0L, 0);
        }

        public Tuple2<Long, Integer> add(UserBehavior value, Tuple2<Long, Integer> accumulator) {
            return new Tuple2<Long, Integer>(accumulator.f0, accumulator.f1 + 1);
        }

        public Double getResult(Tuple2<Long, Integer> accumulator) {
            return (double)accumulator.f0 / accumulator.f1;
        }

        public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
            return new Tuple2<Long, Integer>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    //自定义窗口函数，输出ItemViewCount
    //预聚合的输出就是这里的输入
    //第三个泛型就是keyBy的类型
    private static class WindowResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(key, window.getEnd(), input.iterator().next()));
        }
    }

    //自定义的处理函数
    private static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {

        private int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        private ListState<ItemViewCount> itemState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("itemState", ItemViewCount.class));
        }


        //每一条数据来了怎么办法

        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            //把每条数据存入状态列表
            itemState.add(value);

            //注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        //定时器触发的时候，对所有的数据排序，并输出结果
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //将state中的数据取出，放到ArrayList
            List<ItemViewCount> allItems = new ArrayList<>();

            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }

            //按照count大小排序 从大到小进行排序
            Collections.sort(allItems, new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.getCount() - o1.getCount());
                }
            });

            List<ItemViewCount> sortedTopItems = allItems.subList(0, topSize);

            //清空状态
            itemState.clear();

            //将排名结果格式化输出
            StringBuilder res = new StringBuilder();
            res.append("时间： ").append(new Timestamp(timestamp - 1)).append("\n");

            //输出每一个商品的信息
            int idx = 1;
            for (ItemViewCount sortedTopItem : sortedTopItems) {
                res.append("No. ").append(idx++).append(" : ")
                .append(" 商品ID = ").append(sortedTopItem.getItemId())
                .append(" 浏览量= ").append(sortedTopItem.getCount())
                .append("\n");
            }
            res.append("======================================");

            out.collect(res.toString());

            Thread.sleep(1000);
        }
    }
}
