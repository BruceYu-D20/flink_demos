package yux.sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Time;

public class Flink_Stream_Window_SQL_J {

    public static void main(String[] args) {

        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        DataStream<String> datas = senv.socketTextStream("localhost", 9999);

        datas.flatMap(new FlatMapFunction<String, Tuple2<String, Time>>() {
            @Override
            public void flatMap(String words, Collector<Tuple2<String, Time>> out) throws Exception {
                for(String word: words.split("\\s")){
                    out.collect(new Tuple2<>(word, new Time(System.currentTimeMillis())));
                }
            }
        });
    }
}
