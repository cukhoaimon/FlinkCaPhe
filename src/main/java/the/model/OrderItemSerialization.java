package the.model;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.bson.BsonDocument;

public class OrderItemSerialization implements MongoDeserializationSchema<OrderItem> {
    private static OrderItemSerialization singleton;

    private OrderItemSerialization() {}

    public static OrderItemSerialization getInstance() {
        if (singleton == null) {
            singleton = new OrderItemSerialization();
        }

        return singleton;
    }

    @Override
    public OrderItem deserialize(BsonDocument document) {
        OrderItem item = new OrderItem();
        item.productId = document.getObjectId("product");
        item.quantity = document.get("quantity").asInt32().getValue();
        return item;
    }

    @Override
    public TypeInformation<OrderItem> getProducedType() {
        return TypeInformation.of(OrderItem.class);
    }
}
