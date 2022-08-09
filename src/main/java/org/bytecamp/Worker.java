package org.bytecamp;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;

import static org.bytecamp.SomeUtils.*;


public class Worker implements Serializable {

    private final String APP_NAME = "temp_name";


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
    public void start() throws Exception {
        StreamExecutionEnvironment env = getEnvironment();
        DataStreamSource<JSONObject> DataSource = getTwoTopicSource(env,APP_NAME);
        if (Constant.Data.DATA_TYPE.equals(Constant.Data.Types.JSON)) {
            DataSource
                    .keyBy(e -> e.getString(Constant.Data.PRIMARY_KEY))
                    .window(SlidingEventTimeWindows.of(Time.minutes(6), Time.seconds(1)))
                    .process(new JsonWindowFunction())
                    .map(JSON::toString)
                    .sinkTo(getKafkaSink(Constant.Kafka.OUT_PUT_TOPIC));
        }
        env.execute(APP_NAME);
    }
}
