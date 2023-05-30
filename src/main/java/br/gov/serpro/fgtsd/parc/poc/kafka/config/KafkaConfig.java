package br.gov.serpro.fgtsd.parc.poc.kafka.config;

import jakarta.persistence.EntityManagerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String ISOLATION_LEVEL_CONFIG = "read_committed";
    private static final String TRANSACTIONAL_ID_CONFIG = "id_transacao";

    @Autowired
    private Environment env;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        final Map<String, Object> configProps = generateConnectionProperties();

        //Necessário para que o consumidor apenas processe mensagens comitadas ou que não estejam em uma transação.
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ISOLATION_LEVEL_CONFIG);

        return new DefaultKafkaConsumerFactory<>(configProps, new StringDeserializer(), new StringDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        //Necessário ser configurado explicitamente no kafkaListenerContainerFactory. Caso contrário, o commit dos offsets será pelo método "commitSync", e não junto com a transação com o método "sendOffsetsToTransaction".
        factory.getContainerProperties().setTransactionManager(kafkaTransactionManager());

        return factory;
    }

    @Bean
    public KafkaTransactionManager kafkaTransactionManager() {
        return new KafkaTransactionManager(producerFactory());
    }

    @Bean
    @Primary
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = generateConnectionProperties();

        //Garante que a partição terá exatamente uma mensagem, sem duplicações.
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //Habilita o uso de transações kafka no produtor. O Spring Kafka também sincroniza o commit Kafka com o commit do DB, tudo sendo feita na anotação @Transaction.
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID_CONFIG);

        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    private Map<String, Object> generateConnectionProperties() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        return configProps;
    }
}