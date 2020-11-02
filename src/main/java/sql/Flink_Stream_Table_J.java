package yux.sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class Flink_Stream_Table_J {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        DataStream<String> datas = senv.socketTextStream("localhost", 9999);

        DataStream<String> words = datas.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(word);
                }
            }
        });

//        Table wordsTable = stenv.fromDataStream(words, "name");
        stenv.registerDataStream("words", words, "word");

        Table wordsCounts = stenv.sqlQuery("SELECT word,count(word) as cnt from words group by word");

        stenv.toRetractStream(wordsCounts, Row.class).print();

        senv.execute("WORD COUNT SQL");
    }

    public static class WordCounts{
        String word;
        Integer cnt;

        public WordCounts(String word, Integer cnt){
            this.word = word;
            this.cnt = cnt;
        }
    }
}
