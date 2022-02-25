package com.github.thomaswturner.simplestreams;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StringTransform {

  public static void main(String[] args) throws Exception {
    System.out.println("----- String Transform Stream Application -----");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> dataStream = env.fromElements("A", "Stream", "Of", "Strings");

    SingleOutputStreamOperator<String> upperCaseTransform = dataStream.map(String::toUpperCase);

    upperCaseTransform.print();

    env.execute();
  }
}
