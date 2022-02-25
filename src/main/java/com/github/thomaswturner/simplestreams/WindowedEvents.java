package com.github.thomaswturner.simplestreams;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.ZonedDateTime;

public class WindowedEvents {

  public static void main(String[] args) throws Exception {
    System.out.println("----- Windowed Events Stream Application -----");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    SingleOutputStreamOperator<Tuple2<Integer, Long>> windowed =
        env.fromElements(
                new Tuple2<>(16, ZonedDateTime.now().plusMinutes(25).toInstant().getEpochSecond()),
                new Tuple2<>(15, ZonedDateTime.now().plusMinutes(2).toInstant().getEpochSecond()))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<Integer, Long>>forBoundedOutOfOrderness(
                        Duration.ofSeconds(20))
                    .withTimestampAssigner((event, timestamp) -> event.f1));

    SingleOutputStreamOperator<Tuple2<Integer, Long>> reduced =
        windowed.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).maxBy(0, true);

    reduced.print();
    env.execute();
  }
}
