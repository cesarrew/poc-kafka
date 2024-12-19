package exemplo.poc.kafka.config;

import exemplo.poc.kafka.exception.ConsumerProblemException;
import exemplo.poc.kafka.exception.ProducerProblemException;
import exemplo.poc.kafka.recover.CustomDeadLetterPublishingRecoverer;
import jakarta.persistence.EntityManagerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonDelegatingErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CONSUMER_ISOLATION_LEVEL = "read_committed";
    private static final String DEAD_LETTER_REASON_HEADER = "dead-letter-reason";
    private static final Long DELAY_BETWEEN_ATTEMPTS = 1_500L;
    private static final Integer NUMBER_OF_ATTEMPTS = Integer.MAX_VALUE;
    private static final String PRODUCER_TRANSACTIONAL_ID = "id_transacao";
    private static final String TOPIC_A_CONSUMER_DLQ = "topico_a_consumer_dlq";
    private static final String TOPIC_A_PRODUCER_DLQ = "topico_a_producer_dlq";

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        var configProps = generateConnectionProperties();
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, CONSUMER_ISOLATION_LEVEL);
        return new DefaultKafkaConsumerFactory<>(configProps, new StringDeserializer(), new StringDeserializer());
    }

    //Cria um error handler que delega para outros error handlers a tarefa de tratamento de erro dependendo da exceção.
    @Bean
    public CommonDelegatingErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var commonDelegatingErrorHandler = new CommonDelegatingErrorHandler(generateGeneralErrorHandler());
        commonDelegatingErrorHandler.addDelegate(ConsumerProblemException.class, generateConsumerErrorHandler(kafkaTemplate));
        commonDelegatingErrorHandler.addDelegate(ProducerProblemException.class, generateProducerErrorHandler(kafkaTemplate));
        return commonDelegatingErrorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();

        //Necessário ser configurado explicitamente no kafkaListenerContainerFactory. Caso contrário, o commit dos offsets será pelo método "commitSync", e não junto com a transação com o método "sendOffsetsToTransaction". O problema é usar juntamente com o envio para DLQ. Quando isso acontece, mensagens produzidas que deveriam ter sido abortadas são comitadas juntamente com o offset consumido.
        factory.getContainerProperties().setTransactionManager(kafkaTransactionManager());

        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(errorHandler(kafkaTemplate()));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaTransactionManager kafkaTransactionManager() {
        return new KafkaTransactionManager(producerFactory());
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        var configProps = generateConnectionProperties();

        //Garante que a partição terá exatamente uma mensagem, sem duplicações.
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //Habilita o uso de transações kafka no produtor. O Spring Kafka também sincroniza o commit Kafka com o commit do DB, tudo sendo feita na anotação @Transaction.
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, PRODUCER_TRANSACTIONAL_ID);

        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    @Primary
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    private Headers addErrorHeader(ConsumerRecord<?, ?> consumerRecord, Exception exception) {
        var headers = consumerRecord.headers();
        headers.add(DEAD_LETTER_REASON_HEADER, exception.getMessage().getBytes());
        return headers;
    }

    private Map<String, Object> generateConnectionProperties() {
        var configProps = new HashMap<String, Object>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return configProps;
    }

    //Cria um handler que envia a mensagem Kafka para a DLQ criada para posterior tratamento pelo consumidor.
    private DefaultErrorHandler generateConsumerErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var consumerDLQRecoverer = new CustomDeadLetterPublishingRecoverer(kafkaTemplate, (consumerRecord, exception) -> {
            return new TopicPartition(TOPIC_A_CONSUMER_DLQ, consumerRecord.partition());
        });

        consumerDLQRecoverer.setHeadersFunction(this::addErrorHeader);
        return new DefaultErrorHandler(consumerDLQRecoverer);
    }

    //Cria um handler para tratar exceções não mapeadas. O comportamente é fazer novas tentativas de processamento de tempos em tempos.
    private DefaultErrorHandler generateGeneralErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(DELAY_BETWEEN_ATTEMPTS, NUMBER_OF_ATTEMPTS));
    }

    //Cria um handler que envia a mensagem Kafka para a DLQ criada para posterior tratamento pelo produtor.
    private DefaultErrorHandler generateProducerErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var producerDLQRecoverer = new CustomDeadLetterPublishingRecoverer(kafkaTemplate, (consumerRecord, exception) -> {
            return new TopicPartition(TOPIC_A_PRODUCER_DLQ, consumerRecord.partition());
        });

        producerDLQRecoverer.setHeadersFunction(this::addErrorHeader);
        return new DefaultErrorHandler(producerDLQRecoverer);
    }
}
