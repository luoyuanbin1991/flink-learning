package org.lyb;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author luoyuanbin
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流式运行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(8);

        //读取数据
        DataStreamSource<String> dataStream = senv.readTextFile("target/classes/words.txt");

        //处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> result =
            dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                @Override public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    String[] wordArr = s.split(" ");
                    for (String word : wordArr) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                }
            }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                @Override public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                    return tuple.f0;
                }
            }).sum(1);

        result.print();

        //执行任务，不执行此代码，不会输出结果，原因就是上面都是在定义任务而非执行
        senv.execute();
    }
}
