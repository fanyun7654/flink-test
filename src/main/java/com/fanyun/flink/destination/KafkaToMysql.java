package com.fanyun.flink.destination;

import com.fanyun.flink.KafkaToFlink;
import com.fanyun.flink.destination.postgreSql.PostGreSQLSink;
import com.fanyun.flink.destination.sql.MysqlSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaToMysql {

    public static final String broker = "10.201.82.55:9092";
    public static final String producerTopic = "test";
    public static void main(String[] args) throws Exception {
        //kafka配置信息
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.201.82.55:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();  //设置此可以屏蔽掉日记打印情况
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(500);

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>(
                "ReportedData",
                new SimpleStringSchema(),
                properties);
        DataStream<String> sourceStream = env.addSource(myConsumer);

        DataStream<Tuple3<String, String, String>> sourceStreamTra = sourceStream.
                filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return StringUtils.isNotBlank(value);
            }
        }).map(new KafkaToFlink.JobMapFun());

//        sourceStreamTra.addSink(new PostGreSQLSink());
//        sourceStreamTra.addSink(new MysqlSink());

        TypeInformation typeInformation = TypeExtractor.createTypeInfo(new Tuple3<String, String, String>().getClass());
        TypeInformationSerializationSchema typeInformationSerializationSchema = new TypeInformationSerializationSchema(typeInformation, new ExecutionConfig());
        FlinkKafkaProducer010<Tuple3<String, String, String>> myProducer = new FlinkKafkaProducer010<>(broker, producerTopic, typeInformationSerializationSchema);
        myProducer.setWriteTimestampToKafka(true);//设定是否将timestamp写入kafka
        sourceStreamTra.addSink(myProducer);
        env.execute("data to mysql start");
    }
}
