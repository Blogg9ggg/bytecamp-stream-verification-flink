package org.bytecamp;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.Objects;

import static org.bytecamp.SomeUtils.*;

public class DetailVerification {
    private final String APP_NAME = DetailVerification.class.getName();

    /**
     * 明细流校验返回不匹配的JSONObject.
     */
    private static class DetailJsonProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject>{

        private transient MapState<String, LinkedList<JSONObject>> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, LinkedList<JSONObject>> descriptor = new MapStateDescriptor("MapDescriptor",String.class,LinkedList.class);
            mapState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(JSONObject entry, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
            // 1. get deque of the key.
            LinkedList<JSONObject> deque = mapState.get(ctx.getCurrentKey());
            if (Objects.isNull(deque)){
                deque = new LinkedList<>();
            }

            // 2. handle
            // ...
            deque.offer(entry);
            out.collect(entry);


            // 3.1 update state
            // 应该每次都需要更新
            mapState.put(ctx.getCurrentKey(),deque);


            // 定时器注册接口
            // ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + gap);
        }

        /**  定时器触发函数 */
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }
    }

    public void start() throws Exception {
        StreamExecutionEnvironment env = getEnvironment();
        DataStreamSource<JSONObject> DataSource = getNewTopicSource(env,APP_NAME);
        if (Constant.Data.DATA_TYPE.equals(Constant.Data.Types.JSON)) {
            DataSource.keyBy(e -> e.getString(Constant.Data.PRIMARY_KEY))
                    .process(new DetailJsonProcessFunction())
                    .map(JSON::toString)
                    .sinkTo(getKafkaSink(Constant.Kafka.OUT_PUT_TOPIC));
        }
        env.execute(APP_NAME);
    }

}
