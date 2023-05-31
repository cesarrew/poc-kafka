package br.gov.serpro.fgtsd.parc.poc.kafka.config;

import br.gov.serpro.fgtsd.parc.poc.kafka.recover.CustomDeadLetterPublishingRecoverer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    private static final Long DELAY_BETWEEN_ATTEMPTS = 1_500L;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEAD_LETTER_REASON_HEADER = "dead-letter-reason";
    private static final String CONSUMER_ISOLATION_LEVEL = "read_committed";
    private static final Integer NUMBER_OF_ATTEMPTS = 3;
    private static final String TOPIC_A_DLQ = "topico_a_dlq";
    private static final String PRODUCER_TRANSACTIONAL_ID = "id_transacao";

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        final Map<String, Object> configProps = generateConnectionProperties();

        //Necessário para que o consumidor apenas processe mensagens comitadas ou que não estejam em uma transação.
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, CONSUMER_ISOLATION_LEVEL);

        return new DefaultKafkaConsumerFactory<>(configProps, new StringDeserializer(), new StringDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();

        //Necessário ser configurado explicitamente no kafkaListenerContainerFactory. Caso contrário, o commit dos offsets será pelo método "commitSync", e não junto com a transação com o método "sendOffsetsToTransaction". O problema é usar juntamente com o envio para DLQ. Quando isso acontece, mensagens produzidas que deveriam ter sido abortadas são comitadas juntamente com o offset consumido.
        //factory.getContainerProperties().setTransactionManager(kafkaTransactionManager());

        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(errorHandler(kafkaTemplate()));
        return factory;
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        CustomDeadLetterPublishingRecoverer customDeadLetterPublishingRecoverer = new CustomDeadLetterPublishingRecoverer(kafkaTemplate, (consumerRecord, exception) -> {
            return new TopicPartition(TOPIC_A_DLQ, consumerRecord.partition());
        });

        customDeadLetterPublishingRecoverer.setHeadersFunction((record, exception) -> this.addErrorHeader(record, exception));
        return new DefaultErrorHandler(customDeadLetterPublishingRecoverer, new FixedBackOff(DELAY_BETWEEN_ATTEMPTS, NUMBER_OF_ATTEMPTS));
    }

    //Não utilizado juntamente com o tratamento de erro que produz na DLQ.
    /*
    @Bean
    public KafkaTransactionManager kafkaTransactionManager() {
        return new KafkaTransactionManager(producerFactory());
    }
    */

    //Necessário ser declarado ao usar o kafkaTransactionManager.
    /*
    @Bean
    @Primary
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
    */

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = generateConnectionProperties();

        //Garante que a partição terá exatamente uma mensagem, sem duplicações.
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //Habilita o uso de transações kafka no produtor. O Spring Kafka também sincroniza o commit Kafka com o commit do DB, tudo sendo feita na anotação @Transaction.
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, PRODUCER_TRANSACTIONAL_ID);

        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    private Headers addErrorHeader(ConsumerRecord<?, ?> consumerRecord, Exception exception) {
        Headers headers = consumerRecord.headers();
        headers.add(DEAD_LETTER_REASON_HEADER, exception.getMessage().getBytes());
        return headers;
    }

    private Map<String, Object> generateConnectionProperties() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return configProps;
    }
}