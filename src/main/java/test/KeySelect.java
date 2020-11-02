package yux.test;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import yux.broadcast.mysqlConfBst.MysqlSink;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KeySelect extends AbstractTestBase {

    StreamExecutionEnvironment env;
    ExecutionEnvironment env1;

    @Before
    public void createEnv(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env1 = ExecutionEnvironment.getExecutionEnvironment();
    }

    @Test
    public void tupleKeySelect() throws Exception {
        DataStreamSource<Tuple3<String, Integer, Integer>> source = env.fromElements(
                new Tuple3<String, Integer, Integer>("a", 1, 2),
                new Tuple3<String, Integer, Integer>("b", 2, 3)
        );

        source.keyBy(0);
        source.keyBy(0, 1);

        DataStreamSource<Person> personInfoDS = env.fromElements(
                new Person("zhangsan", 16,
                        new Address("BJ", "BQJ", new Tuple2<>("MJHY", 361)))
        );

        personInfoDS.keyBy("name");
        personInfoDS.keyBy("address.city");
        personInfoDS.keyBy("address.home._0");
        personInfoDS.keyBy(new KeySelector<Person, String>() {
            @Override
            public String getKey(Person person) throws Exception {
                return person.address.city + "_" + person.age;
            }
        });
    }

    @Test
    public void testCounter() throws Exception {
        DataStreamSource<String> source = env.fromElements("a", "b", "a", "c");

        DataStream<Tuple2<String, Integer>> wordCount =
                source.map(new RichMapFunction<String, Tuple2<String, Integer>>() {

                    IntCounter lineCounter = new IntCounter();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        getRuntimeContext().addAccumulator("line_counter", lineCounter);
                    }

                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        lineCounter.add(1);
                        return new Tuple2<>(value, 1);
                    }
                });

        JobExecutionResult executionResult = env.execute("counter");
        Integer counter = executionResult.getAccumulatorResult("line_counter");
        System.out.println(counter);
    }

    @Test
    public void streamIterator() throws Exception {
        DataStream<Long> someIntegers = env.generateSequence(0, 1000);

        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1 ;
            }
        });

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        iteration.closeWith(stillGreaterThanZero);

        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });

        lessThanZero.print();

        Iterator<Long> results = DataStreamUtils.collect(lessThanZero);
        while(results.hasNext()){
            System.out.println(results.next());
        }

        env.execute("test iterator");


    }

    @Test
    public void test1() throws Exception {
        DataStreamSource<Integer> source = env.fromElements(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        SplitStream<Integer> splited = source.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> output = new ArrayList<String>();
                if (value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        });

        splited.select("even").print();
        env.execute("aaa");
    }

    @Test
    public void mysqlSink() throws Exception {
        DataStreamSource<Tuple3<Integer, String, Integer>> source = env.fromElements(
                new Tuple3<Integer, String, Integer>(0, "zs", 19),
                new Tuple3<Integer, String, Integer>(1, "ls", 20),
                new Tuple3<Integer, String, Integer>(2, "ww", 12),
                new Tuple3<Integer, String, Integer>(3, "zl", 21),
                new Tuple3<Integer, String, Integer>(4, "wb", 22)
        );

        source.addSink(new MysqlSink());
        env.execute("test sink");
    }

    @Test
    public void testLatency(){
        env.setBufferTimeout(100);
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value;
            }
        }).setBufferTimeout(100);
    }

    @Test
    public void windowAggregateFunction() throws Exception {

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        System.out.println(System.currentTimeMillis());
        DataStreamSource<Tuple4<Long, String, String, Double>> source =
            env.fromElements(
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "张三", "class1", 100.0),
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "李四", "class2", 96.0),
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "王五", "class1", 90.0),
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "赵六", "class1", 60.0),
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis()+10000, "王八", "class1", 71.0)
            );

        DataStream<Double> classAvgScore =
                source
                .assignTimestampsAndWatermarks(new wmarkAss())
                .keyBy(2)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .aggregate(new AggregateFunction<Tuple4<Long, String, String, Double>, Tuple2<Double, Double>, Double>() {
                    @Override
                    public Tuple2<Double, Double> createAccumulator() {
                        return new Tuple2<Double, Double>(0.0, 0.0);
                    }

                    @Override
                    public Tuple2<Double, Double> add(Tuple4<Long, String, String, Double> value, Tuple2<Double, Double> accumulator) {
                        return new Tuple2<Double, Double>(accumulator.f0 + value.f3, accumulator.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Double> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Double> merge(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
                        return new Tuple2<Double, Double>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });

        classAvgScore.print();

        env.execute("class avg");
    }

    @Test
    public void testProcessWindowFunction() throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Long, String, String, Double>> source =
                env.fromElements(
                        new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "张三", "class1", 100.0),
                        new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "李四", "class2", 96.0),
                        new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "王五", "class1", 90.0),
                        new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "赵六", "class1", 60.0),
                        new Tuple4<Long, String, String, Double>(System.currentTimeMillis()+10000, "王八", "class1", 71.0)
                );

        DataStream<Tuple3<String, Double, String>> classAvg = source.assignTimestampsAndWatermarks(new wmarkAss())
            .keyBy(new KeySelector<Tuple4<Long, String, String, Double>, String>() {
                @Override
                public String getKey(Tuple4<Long, String, String, Double> value) throws Exception {
                    return value.f2;
                }
            })
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<Tuple4<Long, String, String, Double>, Tuple3<String, Double, String>, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, Iterable<Tuple4<Long, String, String, Double>> elements, Collector<Tuple3<String, Double, String>> out) throws Exception {
                    Double totalScore = 0.0;
                    Integer studentNum = 0;
                    Iterator<Tuple4<Long, String, String, Double>> stuInfos = elements.iterator();
                    while (stuInfos.hasNext()) {
                        Tuple4<Long, String, String, Double> stuInfo = stuInfos.next();
                        totalScore += stuInfo.f3;
                        studentNum += 1;
                    }

                    String windowInfo = context.window().getStart() + "_" + context.window().getEnd();
                    out.collect(new Tuple3<String, Double, String>(key, totalScore / studentNum, windowInfo));
                }
            });

        classAvg.print();

        env.execute("class avg");
    }

    @Test
    public void testReduceFunction() throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Long, String, String, Double>> source =
            env.fromElements(
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "张三", "class1", 100.0),
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "王五", "class1", 90.0),
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "赵六", "class1", 60.0),
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "李四", "class2", 96.0),
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis()+10000, "王八", "class1", 71.0)
            );

        DataStream<Tuple2<String, Double>> classScore = source.assignTimestampsAndWatermarks(new wmarkAss())
            .keyBy(new KeySelector<Tuple4<Long, String, String, Double>, String>() {
                @Override
                public String getKey(Tuple4<Long, String, String, Double> value) throws Exception {
                    return value.f2;
                }
            }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(new ReduceFunction<Tuple4<Long, String, String, Double>>() {
                @Override
                public Tuple4<Long, String, String, Double> reduce(Tuple4<Long, String, String, Double> first, Tuple4<Long, String, String, Double> second) throws Exception {
                    return new Tuple4<Long, String, String, Double>(0L, "", second.f2, first.f3 + second.f3);
                }
            })
            .map(new MapFunction<Tuple4<Long, String, String, Double>, Tuple2<String, Double>>() {

                @Override
                public Tuple2<String, Double> map(Tuple4<Long, String, String, Double> value) throws Exception {
                    return new Tuple2<String, Double>(value.f2, value.f3) ;
                }
            });

        classScore.print();

        env.execute("class avg");
    }

    @Test
    public void testProcessFuncCombieAggFunc() throws Exception {

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Long, String, String, Double>> source =
            env.fromElements(
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "张三", "class1", 100.0),
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "王五", "class1", 90.0),
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "赵六", "class1", 60.0),
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "李四", "class2", 96.0),
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "李四", "class2", 99.0),
                    new Tuple4<Long, String, String, Double>(System.currentTimeMillis()+10000, "王八", "class1", 71.0)
            );

        DataStream<Tuple2<String, Double>> avgScore = source.assignTimestampsAndWatermarks(new wmarkAss())
            .keyBy(new KeySelector<Tuple4<Long, String, String, Double>, String>() {
                @Override
                public String getKey(Tuple4<Long, String, String, Double> value) throws Exception {
                    return value.f2;
                }
            })
            .window(TumblingEventTimeWindows.of(Time.seconds(6)))
            .aggregate(new MyAggFunction(), new MyProcessWindowFunction());

        avgScore.print();

        env.execute("test combie");

    }

    @Test
    public void testWindowJoin() throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple4<Long, String, String, Double>> source = env.fromElements(
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "张三", "class1", 100.0),
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "王五", "class1", 90.0),
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "赵六", "class1", 60.0),
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "李四", "class2", 96.0),
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis(), "李四", "class2", 99.0),
                new Tuple4<Long, String, String, Double>(System.currentTimeMillis() + 10000, "王八", "class1", 71.0)
        ).assignTimestampsAndWatermarks(new wmarkAss());

        DataStream<Tuple3<Long, String, String>> classInfo = env.fromElements(
                new Tuple3<Long, String, String>(System.currentTimeMillis(), "class1", "尖子班"),
                new Tuple3<Long, String, String>(System.currentTimeMillis(), "class2", "平行班"),
                new Tuple3<Long, String, String>(System.currentTimeMillis() + 10000, "class2", "平行班")
        ).assignTimestampsAndWatermarks(new wmarkAssClass());


        DataStream<Tuple4<String, String, String, Double>> stuClazzInfo = source
            .join(classInfo)
            .where(new KeySelector<Tuple4<Long, String, String, Double>, String>() {
                @Override
                public String getKey(Tuple4<Long, String, String, Double> value) throws Exception {
                    return value.f2;
                }
            }).equalTo(new KeySelector<Tuple3<Long, String, String>, String>() {
                @Override
                public String getKey(Tuple3<Long, String, String> value) throws Exception {
                    return value.f1;
                }
            }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .apply(new JoinFunction<Tuple4<Long, String, String, Double>, Tuple3<Long, String, String>, Tuple4<String, String, String, Double>>() {
                @Override
                public Tuple4<String, String, String, Double> join(Tuple4<Long, String, String, Double> first,
                                                                   Tuple3<Long, String, String> second)
                        throws Exception {
                    return new Tuple4<>(first.f2, second.f2, first.f1, first.f3);
                }
            });

        stuClazzInfo.print();

        env.execute("window join");
    }

    @Test
    public void testCogroup() throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Long, String, Double>> chineseScore = env.fromElements(
            new Tuple3<Long, String, Double>(System.currentTimeMillis(), "class1", 90.0),
            new Tuple3<Long, String, Double>(System.currentTimeMillis(), "class1", 80.0),
            new Tuple3<Long, String, Double>(System.currentTimeMillis(), "class1", 65.0),
            new Tuple3<Long, String, Double>(System.currentTimeMillis(), "class2", 74.0),
            new Tuple3<Long, String, Double>(System.currentTimeMillis() + 10000, "class2", 74.0)
        ).assignTimestampsAndWatermarks(new wmarkCoGroup());

        DataStream<Tuple3<Long, String, Double>> mathScore = env.fromElements(
                new Tuple3<Long, String, Double>(System.currentTimeMillis(), "class1", 45.0),
                new Tuple3<Long, String, Double>(System.currentTimeMillis(), "class1", 70.0),
                new Tuple3<Long, String, Double>(System.currentTimeMillis(), "class2", 100.0)
        ).assignTimestampsAndWatermarks(new wmarkCoGroup());

        DataStream<Tuple3<String, Double, Double>> scoreCogroup = chineseScore.coGroup(mathScore)
                .where(new KeySelector<Tuple3<Long, String, Double>, String>() {
                    @Override
                    public String getKey(Tuple3<Long, String, Double> value) throws Exception {
                        return value.f1;
                    }
                })
                .equalTo(new KeySelector<Tuple3<Long, String, Double>, String>() {
                    @Override
                    public String getKey(Tuple3<Long, String, Double> value) throws Exception {
                        return value.f1;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple3<Long, String, Double>, Tuple3<Long, String, Double>, Tuple3<String, Double, Double>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<Long, String, Double>> first, Iterable<Tuple3<Long, String, Double>> second, Collector<Tuple3<String, Double, Double>> out) throws Exception {
                        Iterator<Tuple3<Long, String, Double>> chineseItr = first.iterator();
                        Double chineseTotalScore = 0.0;
                        Integer chineseValidScoreNum = 0;
                        String whichClass = "";
                        boolean classGet = false;
                        while (chineseItr.hasNext()) {
                            Tuple3<Long, String, Double> chineseScoreInfo = chineseItr.next();
                            chineseTotalScore += chineseScoreInfo.f2;
                            chineseValidScoreNum += 1;
                            if (!classGet) {
                                whichClass = chineseScoreInfo.f1;
                                classGet = true;
                            }
                        }
                        Double chineseAvg = (chineseTotalScore / chineseValidScoreNum);

                        Iterator<Tuple3<Long, String, Double>> mathItr = second.iterator();
                        Double mathTotalScore = 0.0;
                        Integer mathValidScoreNum = 0;
                        while (mathItr.hasNext()) {
                            mathTotalScore += mathItr.next().f2;
                            mathValidScoreNum += 1;
                        }
                        Double mathAvg = (mathTotalScore / mathValidScoreNum);
                        out.collect(new Tuple3<String, Double, Double>(whichClass, chineseAvg, mathAvg));
                    }
                });

        scoreCogroup.print();
        env.execute("execute cogroup");

    }
}

class MyAggFunction implements AggregateFunction<Tuple4<Long,String,String,Double>, Tuple2<Double, Double>, Double>{

    @Override
    public Tuple2<Double, Double> createAccumulator() {
        return new Tuple2<>(0.0, 0.0);
    }

    @Override
    public Tuple2<Double, Double> add(Tuple4<Long, String, String, Double> value, Tuple2<Double, Double> accumulator) {
        return new Tuple2<>(accumulator.f0 + value.f3, accumulator.f1 + 1);
    }

    @Override
    public Double getResult(Tuple2<Double, Double> accumulator) {
        return accumulator.f0 / accumulator.f1;
    }

    @Override
    public Tuple2<Double, Double> merge(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
        return new Tuple2<Double, Double>(a.f0 + b.f0, a.f1 + b.f1);
    }
}

class MyProcessWindowFunction extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Double> elements, Collector<Tuple2<String, Double>> out) throws Exception {
        Iterator<Double> classAvg = elements.iterator();
        Double maxScore = 0.0;
        while(classAvg.hasNext()){
            maxScore = Math.max(classAvg.next(), maxScore);
        }
        out.collect(new Tuple2<>(key, maxScore));
    }
}

class wmarkCoGroup implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, Double>> {

    private final long maxOutOfOrderness = 3000; // 10 seconds

    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, Double> element, long previousElementTimestamp) {
        long eventTime = element.f0;
        currentMaxTimestamp = Math.max(element.f0, currentMaxTimestamp);
        return eventTime;
    }
}

class wmarkAssClass implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>> {

    private final long maxOutOfOrderness = 3000; // 10 seconds

    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
        long eventTime = element.f0;
        currentMaxTimestamp = Math.max(element.f0, currentMaxTimestamp);
        return eventTime;
    }
}

class wmarkAss implements AssignerWithPeriodicWatermarks<Tuple4<Long, String, String, Double>> {


    private final long maxOutOfOrderness = 3000; // 10 seconds

    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple4<Long, String, String, Double> element, long previousElementTimestamp) {
        long eventTime = element.f0;
        currentMaxTimestamp = Math.max(element.f0, currentMaxTimestamp);
        return eventTime;
    }
}

class Person {

    public Person(String name, int age, Address address){
        this.name = name;
        this.address = address;
        this.age = age;
    }

    String name;
    int age;
    Address address;

}

class Address {

    public Address(String city, String street, Tuple2<String, Integer> home) {
        this.city = city;
        this.street = street;
        this.home = home;
    }

    String city;
    String street;
    Tuple2<String, Integer> home;
}
