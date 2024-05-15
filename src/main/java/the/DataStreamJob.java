/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package the;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.MongoSourceBuilder;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import the.model.Order;
import the.model.OrderItem;
import the.model.OrderItemSerialization;
import the.model.OrderSerialization;
import org.apache.flink.connector.base.DeliveryGuarantee;
import java.time.Duration;
import java.time.Instant;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {
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

	public static void main(String[] args) throws Exception {
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

		// Execute program, beginning computation.
		env.execute("Calculate total money job");
	}
}


