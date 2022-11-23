package chapter06;

import chapter05.ClickSource;
import chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> dataStream = env.addSource(new ClickSource());
        dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(

                new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event o, long l) {
                        return o.timestamp;
                    }
                }

        ));
        dataStream.keyBy(data -> data.user)
                .window(SlidingEventTimeWindows.of(Time.seconds(3),Time.seconds(2)));
//                .window(TumblingEventTimeWindows.of(Time.seconds(3)));
        env.execute();

    }
}
