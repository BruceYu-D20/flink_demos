package yux.stream;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink_WordCount_J {

//    recursive.file.enumeration=true

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

        DataStream<WordWithCount> wordCounts = text.flatMap(new RichFlatMapFunction<String, WordWithCount>() {

            private IntCounter ic = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ic = new IntCounter();
                getRuntimeContext().addAccumulator("my_accumulator", ic);
                super.open(parameters);
                super.open(parameters);
            }

            @Override
            public void flatMap(String lines, Collector<WordWithCount> out) throws Exception {
                for (String word : lines.split("\\s")) {
                    this.ic.add(1);
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        })
        .keyBy("word")
        .timeWindow(Time.seconds(5),Time.seconds(1))
        .reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                return new WordWithCount(a.word, a.count + b.count);
            }
        });

        wordCounts.print().setParallelism(1);

        JobExecutionResult jobExecutionResult = env.execute("Socket Window WordCount");
        jobExecutionResult.getAccumulatorResult("my_accumulator");

    }

    // Data type for words with count
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
