package org.bytecamp;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class SomeUtils {
    // first get special environment
    public static StreamExecutionEnvironment getEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://127.0.0.1:9000/gmall-flink/checkpoint");
        env.getCheckpointConfig().setCheckpointTimeout(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        return env;
    }

    // get old topic sources
    public static DataStreamSource<JSONObject> getOldTopicSource(StreamExecutionEnvironment env,String appName) {
        KafkaSource<JSONObject> source = getKafkaSource(Constant.Kafka.OLD_KAFKA_TOPIC);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), appName);
    }

    // get new topic sources
    public static DataStreamSource<JSONObject> getNewTopicSource(StreamExecutionEnvironment env,String appName) {
        KafkaSource<JSONObject> source = getKafkaSource(Constant.Kafka.NEW_KAFKA_TOPIC);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), appName);
    }

    // get two topic sources
    public static DataStreamSource<JSONObject> getTwoTopicSource(StreamExecutionEnvironment env,String appName) {
        KafkaSource<JSONObject> source = getKafkaSource(Constant.Kafka.NEW_KAFKA_TOPIC, Constant.Kafka.OLD_KAFKA_TOPIC);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), appName);
    }


    // get kafka Source with the topicName
    public static KafkaSource<JSONObject> getKafkaSource(String... topicName) {
        return KafkaSource.<JSONObject>builder()
                .setBootstrapServers(Constant.Kafka.KAFKA_BROKER_ADDRESS)
                .setTopics(topicName)
                .setGroupId(Constant.Kafka.TEST_KAFKA_CONSUMER_PREFIX+System.currentTimeMillis()) // consumer group id
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaRecordDeserializationSchema<JSONObject>() {

                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<JSONObject> collector) throws IOException {
                        JSONObject entry = JSON.parseObject(new String(consumerRecord.value()));
                        // add kafka meta
                        entry.put(Constant.Data.TOPIC_NAME,consumerRecord.topic());
                        entry.put(Constant.Data.TIMESTAMP_FIELD,consumerRecord.timestamp()); // ???
                        collector.collect(entry);
                    }

                    @Override
                    public TypeInformation<JSONObject> getProducedType() {
                        return TypeInformation.of(JSONObject.class);
                    }
                })
                .build();
    }

    public static KafkaSink<String> getKafkaSink(String topicName) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.Kafka.KAFKA_BROKER_ADDRESS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
    }
}
