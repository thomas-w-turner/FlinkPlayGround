package com.github.thomaswturner;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Application {

  public static void main(String[] args) throws Exception {
    System.out.println("----- Flink Play Ground Application -----");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> dataStream = env.fromElements("A", "Stream", "Of", "Strings");

    SingleOutputStreamOperator<String> upperCaseTransform = dataStream.map(String::toUpperCase);

    upperCaseTransform.print();

    env.execute();
  }
}
