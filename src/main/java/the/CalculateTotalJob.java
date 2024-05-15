package the;

import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import the.model.Order;
import the.model.OrderSerialization;

import java.time.Duration;

public class CalculateTotalJob {
    static MongoSource<Order> source = MongoSource.<Order>builder()
            .setUri("mongodb://root:example@mongo:27017/?authSource=admin")
            .setDatabase("test")
            .setCollection("orders")
            .setFetchSize(2048)
            .setLimit(10000)
            .setNoCursorTimeout(true)
            .setPartitionStrategy(PartitionStrategy.SAMPLE)
            .setPartitionSize(MemorySize.ofMebiBytes(64))
            .setSamplesPerPartition(10)
            .setDeserializationSchema(new OrderSerialization())
            .build();

    static MongoSink<BsonDocument> sink = MongoSink.<BsonDocument>builder()
            .setUri("mongodb://root:example@mongo:27017/?authSource=admin")
            .setDatabase("analytics")
            .setCollection("metrics")
            .setBatchSize(1000)
            .setBatchIntervalMs(1000)
            .setMaxRetries(3)
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setSerializationSchema((input, context) -> new InsertOneModel<>(input))
            .build();

    static WatermarkStrategy<Order> strategy = WatermarkStrategy
            .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((event, timestamp) -> event.createdTime.toEpochMilli());

    public static StreamExecutionEnvironment get() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setRuntimeMode(RuntimeExecutionMode.BATCH);

        env.fromSource(source, strategy, "MongoDB-Source")
                .setParallelism(2)
                .map((MapFunction<Order, Integer>) order -> order.totalMoney)
                .setParallelism(2)
                .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                .reduce(Integer::sum)
                .map((MapFunction<Integer, BsonDocument>) total -> new BsonDocument("total_amount", new BsonInt32(total)))
                .sinkTo(sink)
                .setParallelism(1);

        return env;
    }
}
