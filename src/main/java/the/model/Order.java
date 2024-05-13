package the.model;

import org.bson.BsonObjectId;

import java.time.Instant;

public class Order {
    public BsonObjectId _id;
    public BsonObjectId userID;
    public Integer totalMoney;
    public String status;
    public Instant createdTime;

    @Override
    public String toString() {
        return super.toString();
    }
}
