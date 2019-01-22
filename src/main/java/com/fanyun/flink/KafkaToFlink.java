package com.fanyun.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaToFlink {
    public static void main(String[] args)throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);      //启动检查点
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(
                java.util.regex.Pattern.compile("ReportedData"),
                new SimpleStringSchema(),
                properties);
        DataStream<String> stream = env.addSource(myConsumer);

        stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "Stream Value ========== " + value;
            }
        }).print();

        env.execute("flink kafka test");
    }
}
