package org.bytecamp;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.bytecamp.common.Constant;

import java.util.ArrayList;
import java.util.HashMap;


public class Worker {

    private final String APP_NAME = "temp_name";

    // first get special environment
    private StreamExecutionEnvironment getEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ... set environment
        env.setParallelism(1);

        return env;
    }

    // get old topic sources
    private DataStreamSource<String> getOldTopicSource(StreamExecutionEnvironment env) {
        KafkaSource<String> source = getKafkaSource(Constant.Kafka.OLD_KAFKA_TOPIC);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), APP_NAME);
    }

    // get new topic sources
    private DataStreamSource<String> getNewTopicSource(StreamExecutionEnvironment env) {
        KafkaSource<String> source = getKafkaSource(Constant.Kafka.NEW_KAFKA_TOPIC);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), APP_NAME);
    }

    // get two topic sources
    private DataStreamSource<String> getTwoTopicSource(StreamExecutionEnvironment env) {
        KafkaSource<String> source = getKafkaSource(Constant.Kafka.NEW_KAFKA_TOPIC, Constant.Kafka.OLD_KAFKA_TOPIC);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), APP_NAME);
    }


    // get kafka Source with the topicName
    private KafkaSource<String> getKafkaSource(String... topicName) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.Kafka.KAFKA_BROKER_ADDRESS)
                .setTopics(topicName)
                .setGroupId(Constant.Kafka.TEST_KAFKA_CONSUMER_PREFIX + this.getClass().getName())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private KafkaSink<String> getKafkaSink(String topicName) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.Kafka.KAFKA_BROKER_ADDRESS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
    }

    /**
     * verify function
     */
    private static class JsonWindowFunction extends ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow> {

        private final long GAP = 1000 * 60; // 1 minute

        @Override
        public void process(String s, ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
            // 1. get time window information.
            TimeWindow window = context.window();
            long thresholdTimestamp = window.getStart() + GAP;

            // 2. build the state table.
            ArrayList<JSONObject> windowDataSet = new ArrayList<>();
            elements.forEach(windowDataSet::add);

            elements.forEach(v -> {
                // 3. judge deprecated data.
                if (v.getLong(Constant.Data.TIMESTAMP_FIELD) < thresholdTimestamp) {

                    // 4. find other data.
                    int count = 0;
                    for (JSONObject e : windowDataSet) {
                        if (equal(v, e)) {
                            count++;
                        }
                        if (count >= 2) {
                            break;
                        }
                    }
                    if (count < 2) {
                        out.collect(v);
                    }
                }
            });

        }

        private boolean equal(JSONObject a, JSONObject b) {
            // todo: ???
            return true;
        }
    }

    // main
    public void start() {
        StreamExecutionEnvironment env = getEnvironment();
        DataStreamSource<String> DataSource = getTwoTopicSource(env);
        if (Constant.Data.DATA_TYPE == Constant.Data.Types.JSON) {
            DataSource.map(value -> {
                        JSONObject jsonObject = JSON.parseObject(value);
                        jsonObject.put(Constant.Data.TIMESTAMP_FIELD, System.currentTimeMillis());
                        return jsonObject;
                    })
                    .keyBy(e -> e.getString(Constant.Data.PRIMARY_KEY))
                    .window(SlidingEventTimeWindows.of(Time.minutes(6), Time.seconds(1)))
                    .process(new JsonWindowFunction())
                    .map(JSON::toString)
                    .sinkTo(getKafkaSink(Constant.Kafka.OUT_PUT_TOPIC));
        }
    }
}
