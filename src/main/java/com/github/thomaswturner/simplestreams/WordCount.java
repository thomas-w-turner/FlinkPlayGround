package com.github.thomaswturner.simplestreams;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCount {

  public static void main(String[] args) throws Exception {
    System.out.println("----- Word Count Stream Application -----");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> text =
        env.fromElements("A dodgy bloke takes a dodgy smoke from another dodgy bloke");

    DataStream<Tuple2<String, Integer>> counts =
        text.map(String::toLowerCase)
            .flatMap(new StringTokenizer())
            .keyBy((tuple) -> tuple.f0)
            .sum(1);

    counts.print();
    env.execute();
  }

  private static final class StringTokenizer
      implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
      String[] tokens = value.split("\\W+");
      Arrays.stream(tokens)
          .forEach(
              token -> {
                if (token.length() > 0) {
                  out.collect(new Tuple2<>(token, 1));
                }
              });
    }
  }
}
