package org.lyb;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author luoyuanbin
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        //创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        DataSet<String> inputDataSet =  env.readTextFile("target/classes/words.txt");

        DataSet<Tuple2<String,Integer>> result = inputDataSet.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {

            @Override public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] wordArr = s.split(" ");
                for(String word:wordArr){
                    collector.collect(new Tuple2<>(word,1));
                }
            }
        }).groupBy(0).sum(1);
        result.print();
    }
}
