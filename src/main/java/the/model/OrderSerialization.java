package the.model;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.bson.BsonDocument;

import java.time.Instant;

public class OrderSerialization implements MongoDeserializationSchema<Order> {
    @Override
    public Order deserialize(BsonDocument document) {
        Order order = new Order();
        order._id = document.getObjectId("_id");
        order.userID = document.getObjectId("userID");
        order.status = document.get("status").asString().getValue();
        order.totalMoney = document.get("totalMoney").asInt32().getValue();
        order.createdTime = Instant.ofEpochSecond(document.get("createdTime").asDateTime().getValue());

        return order;
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
}

