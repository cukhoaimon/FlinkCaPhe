package the;

import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.bson.*;
import org.bson.types.ObjectId;
import the.model.OrderItem;
import the.model.OrderItemSerialization;

public class ReduceProduct {
    static MongoSource<OrderItem> orderItem = MongoSource.<OrderItem>builder()
            .setUri("mongodb://root:example@mongo:27017/?authSource=admin")
            .setDatabase("test")
            .setCollection("orderitems")
            .setFetchSize(2048)
            .setLimit(10000)
            .setNoCursorTimeout(true)
            .setPartitionStrategy(PartitionStrategy.SAMPLE)
            .setPartitionSize(MemorySize.ofMebiBytes(64))
            .setSamplesPerPartition(10)
            .setDeserializationSchema(OrderItemSerialization.getInstance())
            .build();

    static MongoSink<BsonDocument> sink = MongoSink.<BsonDocument>builder()
            .setUri("mongodb://root:example@mongo:27017/?authSource=admin")
            .setDatabase("analytics")
            .setCollection("products")
            .setBatchSize(1000)
            .setBatchIntervalMs(1000)
            .setMaxRetries(3)
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setSerializationSchema((input, context) -> new InsertOneModel<>(input))
            .build();

    static StreamExecutionEnvironment get() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setRuntimeMode(RuntimeExecutionMode.BATCH);

        env.fromSource(orderItem, WatermarkStrategy.noWatermarks(), "MongoDB-Source")
                .setParallelism(2)
                .map((MapFunction<OrderItem, Tuple2<String, Integer>>) item -> new Tuple2<>(item.productId.getValue().toHexString(), item.quantity))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(item -> item.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .setParallelism(1)
                .map((MapFunction<Tuple2<String, Integer>, BsonDocument>) product -> {
                    BsonDocument bson = new BsonDocument();
                    bson.append("productId", new BsonObjectId(new ObjectId(product.f0)));
                    bson.append("quantity", new BsonInt32(product.f1));
                    return bson;
                })
                .sinkTo(sink)
                .setParallelism(1);

        return env;
    }
}
