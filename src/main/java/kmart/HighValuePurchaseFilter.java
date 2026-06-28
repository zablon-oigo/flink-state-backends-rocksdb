package kmart;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class HighValuePurchaseFilter {

    private static final String BROKER_URL = "kafka:9092";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int LOYALTY_THRESHOLD = 3;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(30000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BROKER_URL)
                .setTopics("kmart-raw")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<JsonNode> purchases = env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source")
                .map(MAPPER::readTree)
                .filter(node -> node.has("price") && node.get("price").asDouble() > 150);

        purchases.sinkTo(createPurchaseSink());

        DataStream<CustomerStats> customerStats = purchases
                .keyBy(node -> node.get("customer_id").asText())
                .process(new CustomerAggregator());

        customerStats.sinkTo(createStatsSink());

        env.execute("High Value Purchase Filter");
    }

    private static KafkaSink<JsonNode> createPurchaseSink() {
        return KafkaSink.<JsonNode>builder()
                .setBootstrapServers(BROKER_URL)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JsonNode>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(
                            JsonNode element, KafkaSinkContext context, Long timestamp) {
                        try {
                            return new ProducerRecord<>(
                                    "high-value-purchases",
                                    element.get("customer_id").asText().getBytes(StandardCharsets.UTF_8),
                                    MAPPER.writeValueAsBytes(element));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .setTransactionalIdPrefix("purchase-sink-")
                .build();
    }

    private static KafkaSink<CustomerStats> createStatsSink() {
        return KafkaSink.<CustomerStats>builder()
                .setBootstrapServers(BROKER_URL)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new KafkaRecordSerializationSchema<CustomerStats>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(
                            CustomerStats stats, KafkaSinkContext context, Long timestamp) {
                        try {
                            return new ProducerRecord<>(
                                    "customer-stats",
                                    stats.customerId.getBytes(StandardCharsets.UTF_8),
                                    MAPPER.writeValueAsBytes(stats));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .setTransactionalIdPrefix("stats-sink-")
                .build();
    }

    public static class CustomerAggregator
            extends KeyedProcessFunction<String, JsonNode, CustomerStats> {

        private transient ValueState<CustomerStats> statsState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<CustomerStats> descriptor =
                    new ValueStateDescriptor<>("customer-stats-state", CustomerStats.class);
            statsState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                JsonNode value,
                Context ctx,
                Collector<CustomerStats> out) throws Exception {

            CustomerStats stats = statsState.value();
            if (stats == null) {
                stats = new CustomerStats(ctx.getCurrentKey(), 0, 0.0);
            }

            stats.purchaseCount++;
            stats.totalSpent += value.get("price").asDouble();

            statsState.update(stats);

            if (stats.purchaseCount > LOYALTY_THRESHOLD) {
                System.out.printf("LOYAL Customer=%s Count=%d TotalSpent=%.2f%n",
                        stats.customerId, stats.purchaseCount, stats.totalSpent);
                out.collect(stats);
            } else {
                System.out.printf("Customer=%s Count=%d TotalSpent=%.2f%n",
                        stats.customerId, stats.purchaseCount, stats.totalSpent);
            }
        }
    }
    
    public static class CustomerStats {
        public String customerId;
        public long purchaseCount;
        public double totalSpent;

        public CustomerStats() {}

        @JsonCreator
        public CustomerStats(
                @JsonProperty("customerId") String customerId,
                @JsonProperty("purchaseCount") long purchaseCount,
                @JsonProperty("totalSpent") double totalSpent) {
            this.customerId = customerId;
            this.purchaseCount = purchaseCount;
            this.totalSpent = totalSpent;
        }
    }
}