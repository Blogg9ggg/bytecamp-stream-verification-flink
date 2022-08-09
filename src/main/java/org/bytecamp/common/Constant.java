package org.bytecamp.common;

public class Constant {
    public static class Kafka {
        public static final String KAFKA_BROKER_ADDRESS = "172.20.108.155:9092";

        //
        public static final int PARTITION_NUMBER = 10;

        public static final String OLD_KAFKA_TOPIC = "test-old-topic";
        public static final String NEW_KAFKA_TOPIC = "test-new-topic";
        public static final String TEST_KAFKA_TOPIC = "test-new-topic";

        public static final String OUT_PUT_TOPIC = "test-output-topic";

        public static final String TEST_KAFKA_CONSUMER_PREFIX = "test-";
    }

    public static class Data{
        public static class Types {
            public static final String JSON = "JSON";
            public static final String KV = "K:V";
        }

        public static final String DATA_TYPE = Types.JSON;
        public static final String TIMESTAMP_FIELD = "verify_timestamp";
        public static final String PRIMARY_KEY = "id";
    }

}
