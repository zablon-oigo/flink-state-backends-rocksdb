package kmart;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import tools.jackson.databind.ObjectMapper;
import net.datafaker.Faker;

public class App {
        private static final String BOOTSTRAP = "localhost:29092";
    private static final String TOPIC = "kmart-raw";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Faker faker = new Faker();
        ObjectMapper mapper = new ObjectMapper();

        try {

            while (true) {

                String orderId = UUID.randomUUID().toString();
                String customerId = UUID.randomUUID().toString();
                String productId = UUID.randomUUID().toString();
                long timestamp = System.currentTimeMillis();

                String productName = faker.commerce().productName();
                String category = faker.commerce().department();

                int quantity = ThreadLocalRandom.current().nextInt(1, 5);
                double price = Double.parseDouble(faker.commerce().price());

                String city = faker.address().city();
                String country = faker.address().country();
                double latitude = Double.parseDouble(faker.address().latitude());
                double longitude = Double.parseDouble(faker.address().longitude());

                String firstName = faker.name().firstName();
                String lastName = faker.name().lastName();
                String email = faker.internet().emailAddress();

                // Build JSON object
                var order = new java.util.HashMap<String, Object>();
                order.put("order_id", orderId);
                order.put("customer_id", customerId);
                order.put("product_id", productId);
                order.put("timestamp", timestamp);
                order.put("product_name", productName);
                order.put("category", category);
                order.put("quantity", quantity);
                order.put("price", price);
                order.put("city", city);
                order.put("country", country);
                order.put("latitude", latitude);
                order.put("longitude", longitude);
                order.put("first_name", firstName);
                order.put("last_name", lastName);
                order.put("email", email);

                String json = mapper.writeValueAsString(order);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, customerId, json);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println(" partition=" + metadata.partition()
                                + " offset=" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });

                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}