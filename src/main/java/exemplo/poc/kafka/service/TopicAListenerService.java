package exemplo.poc.kafka.service;

import exemplo.poc.kafka.exception.ConsumerProblemException;
import exemplo.poc.kafka.exception.ProducerProblemException;
import exemplo.poc.kafka.model.Message;
import exemplo.poc.kafka.repository.MessageRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.Collections;

@Service
public class TopicAListenerService {

    private static final String CONSUMER_PROBLEM_IDENTIFICATION = "problema_consumidor";
    private static final String GENERAL_PROBLEM_IDENTIFICATION = "problema_geral";
    private static final String GROUP_ID = "id_grupo";
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicAListenerService.class);
    private static final String PRODUCER_PROBLEM_IDENTIFICATION = "problema_produtor";
    private static final String TOPIC_A = "topico_a";
    private static final String TOPIC_B = "topico_b";
    private static final String TOPIC_C = "topico_c";

    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private MessageRepository messageRepository;

    /*
    Comandos de terminal para uso do servidor Kafka e execução do exemplo:

    Inicializar Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
    Inicializar Kafka: bin/kafka-server-start.sh config/server.properties
    Produzir mensagens no topico_a: bin/kafka-console-producer.sh --topic topico_a --bootstrap-server localhost:9092
    Consumir mensagens do topico_a: bin/kafka-console-consumer.sh --topic topico_a --from-beginning --bootstrap-server localhost:9092 --isolation-level=read_committed
    Consumir mensagens do topico_b: bin/kafka-console-consumer.sh --topic topico_b --from-beginning --bootstrap-server localhost:9092 --isolation-level=read_committed
    Consumir mensagens do topico_c: bin/kafka-console-consumer.sh --topic topico_c --from-beginning --bootstrap-server localhost:9092 --isolation-level=read_committed
    Consumir mensagens do topico_a_consumer_dlq: bin/kafka-console-consumer.sh --topic topico_a_consumer_dlq --from-beginning --bootstrap-server localhost:9092 --isolation-level=read_committed
    Consumir mensagens do topico_a_producer_dlq: bin/kafka-console-consumer.sh --topic topico_a_producer_dlq --from-beginning --bootstrap-server localhost:9092 --isolation-level=read_committed
    */

    /*
    Caso 3 - Sem o KafkaTransactionManager e usando o sendOffsetsToTransaction
    --------------------------------------------------------------------------

    Objetivo: Consumir mensagem do tópico A, gravar no banco e depois produzir mensagens para os tópicos B e C. Enviar para DLQ (consumidor ou produtor) em caso de ConsumerProblemException ou ProducerProblemException após 3 tentativas. Nesse caso, a mensagem deve ser dada como consumida e as mensagens para os tópicos B e C não devem ser comitadas, assim como as operações de banco. Fazer retry infinito caso seja outro erro.

    Teste 1: Mensagem sem problemas de consumo.
    Resultado: Mensagem foi consumida e as mensagens para os tópicos B e C foram comitadas. Foi usado o sendOffsetsToTransaction para comitar o offset junto com a transação. O Spring Kafka fez também o commitSync após o commit da transação, mas isso não é um problema real. Foi feito também o commit em banco. Resultado final desejado.

    Teste 2: Mensagem com problema de consumo, erro conhecido ConsumerProblemException.
    Resultado: Mensagem foi consumida e as mensagens para os tópicos B e C não foram comitadas. Foi usado o commitSync pelo Spring Kafka para o consumo da mensagem após o commit da transação na DLQ, então, em caso de erro na transação, a mensagem não seria consumida. Existe a possibilidade ainda assim da transação ser comitada e o offset não. Isso implicaria no reprocessamento da mensagem, o que é um risco mitigável. Foi enviado para a DQL do consumidor e também foi feito rollback no banco. Resultado final desejado.

    Teste 3: Mensagem com problema de consumo, erro desconhecido.
    Resultado: Mensagem não consumida e demais mensagens produzidas não comitadas. Retry infinito. Resultado final desejado.

    Conclusão: O uso do sendOffsetsToTransaction sem o kafkaTransactionManager garante o consumo transacional. Em caso de problema que necessite enviar para as DQLs o Spring Kafka usa o commitSync, mas as mensagens enviadas para os tópicos B e C não serão comitadas junto. O uso do commitSync pelo Spring Kafka em alguns momentos não é desejável, mas não aparenta ser impactante.
    */
    @Transactional
    @KafkaListener(groupId = GROUP_ID, topics = TOPIC_A)
    public void processMessage(Consumer<String, String> consumer, ConsumerRecord<String, String> consumerRecord) {
        LOGGER.info("Iniciando do processamento da mensagem do tópico A...");
        saveMessageDataBase(consumerRecord.value());
        sendKafkaMessage("Mensagem para o tópico B: " + consumerRecord.value(), TOPIC_B);
        sendKafkaMessage("Mensagem para o tópico C: " + consumerRecord.value(), TOPIC_C);

        if (consumerRecord.value().contains(CONSUMER_PROBLEM_IDENTIFICATION)) {
            throw new ConsumerProblemException("Problema no consumidor. Enviando para a DLQ.");
        }

        if (consumerRecord.value().contains(PRODUCER_PROBLEM_IDENTIFICATION)) {
            throw new ProducerProblemException("Problema no produtor. Enviando para a DLQ.");
        }

        if (consumerRecord.value().contains(GENERAL_PROBLEM_IDENTIFICATION)) {
            throw new RuntimeException("Problema não mapeado. Novas tentativas de processamento serão feitas.");
        }

        LOGGER.info("Enviando do offset da mensagem do tópico A para ser incluída na transação e encerrando o processamento do consumo da mensagem...");

        kafkaTemplate.sendOffsetsToTransaction(
                Collections.singletonMap(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), new OffsetAndMetadata(consumerRecord.offset() + 1)),
                consumer.groupMetadata()
        );

        applicationEventPublisher.publishEvent(consumerRecord.value());
        LOGGER.info("Fim do processamento da mensagem do tópico A...");
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    protected void handleAfterDataBaseCommit(String event) {
        var messageList = messageRepository.findAll();
        LOGGER.info("Mensagens no banco de dados: {}", String.join(", ", messageList.stream().map(Message::getMessage).toList()));
    }

    private void saveMessageDataBase(String messageTopicA) {
        LOGGER.info("Salvando mensagem no banco de dados...");
        var message = new Message(messageTopicA);
        messageRepository.save(message);
    }

    private void sendKafkaMessage(String message, String topic) {
        LOGGER.info("Enviando mensagem para o tópico \"{}\"...", topic);
        var producerRecord = new ProducerRecord<String, String>(topic, message);
        kafkaTemplate.send(topic, message);
    }
}
