package exemplo.poc.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ENABLE_AUTO_COMMIT = "false";
    private static final String ENABLE_IDEMPOTENCE = "true";
    private static final String GROUP_ID = "id_grupo";
    private static final String ISOLATION_LEVEL = "read_committed";
    private static final String KEY_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String TRANSACTIONAL_ID = "id_transacao";
    private static final String VALUE_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String VALUE_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";

    @Bean
    public KafkaConsumer<String, String> kafkaConsumer() {
        var configProps = generateConnectionProperties();
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ISOLATION_LEVEL);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS);
        return new KafkaConsumer<String, String>(configProps);
    }

    @Bean
    public KafkaProducer<String, String> kafkaProducer() {
        var configProps = generateConnectionProperties();
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_IDEMPOTENCE);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS);
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS);
        return new KafkaProducer<String, String>(configProps);
    }

    private Map<String, Object> generateConnectionProperties() {
        var configProps = new HashMap<String, Object>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return configProps;
    }
}
