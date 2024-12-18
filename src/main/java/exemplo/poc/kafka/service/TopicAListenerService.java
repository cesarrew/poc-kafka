package exemplo.poc.kafka.service;

import exemplo.poc.kafka.exception.ConsumerProblemException;
import exemplo.poc.kafka.exception.ProducerProblemException;
import exemplo.poc.kafka.model.Message;
import exemplo.poc.kafka.repository.MessageRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.Collections;

@Service
public class TopicAListenerService {

    private static final String CONSUMER_PROBLEM_IDENTIFICATION = "problema_consumidor";
    private static final Long DELAY_BETWEEN_ATTEMPTS = 5_000L;
    private static final String GENERAL_PROBLEM_IDENTIFICATION = "problema_geral";
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicAListenerService.class);
    private static final String PRODUCER_PROBLEM_IDENTIFICATION = "problema_produtor";
    private static final String TOPIC_A = "topico_a";
    private static final String TOPIC_A_CONSUMER_DLQ = "topico_a_consumer_dlq";
    private static final String TOPIC_A_PRODUCER_DLQ = "topico_a_producer_dlq";
    private static final String TOPIC_B = "topico_b";
    private static final String TOPIC_C = "topico_c";

    @Autowired
    private KafkaConsumer<String, String> kafkaConsumer;

    @Autowired
    private KafkaProducer<String, String> kafkaProducer;

    @Autowired
    private MessageRepository messageRepository;

    @Autowired
    private PlatformTransactionManager platformTransactionManager;

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
    Caso 4 - Uso da API nativa do Kafka
    -----------------------------------

    Consumindo mensagem do tópico A, gravando em banco e depois produzindo mensagens para os tópicos B e C. Enviando para a DLQ do consumidor em caso de erro no consumidor, DLQ do produtor em caso de erro no produtor e tentativas infinitas em caso de erro não mapeado.

    Teste 1: Usando loops e recursos da API nativa do Kafka.
    Resultado: O tratamento de erros foi feito adequadamente de acordo com o tipo de exceção. Além disso, o rollback foi feito no banco ao enviar para a DLQ.

    Conclusão caso 4: Pode-se mapear problemas conhecidos lançando determinadas exceções para determinar se será feito retry ou se será enviado para a DLQ do consumidor/produtor. É feito também rollback no banco em caso de necessidade de enviar para a DLQ, o que é necessário.
    */
    public void processMessage() throws InterruptedException {
        kafkaProducer.initTransactions();

        try {
            kafkaConsumer.subscribe(Collections.singleton(TOPIC_A));

            while (true) {
                var consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                if (consumerRecords.isEmpty()) {
                    continue;
                }

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    var consumerRecordProcessed = false;

                    while (!consumerRecordProcessed) {
                        try {
                            LOGGER.info("Iniciando do processamento da mensagem do tópico A...");
                            kafkaProducer.beginTransaction();

                            consumerRecordProcessed = new TransactionTemplate(platformTransactionManager).execute(transactionStatus -> {
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

                                LOGGER.info("Enviando do offset da mensagem do tópico A para ser incluída na transação, comitando e encerrando o processamento do consumo da mensagem...");
                                sendOffsetsAndCommitTransaction(consumerRecord);
                                return true;
                            });
                        } catch (ConsumerProblemException cpe) {
                            LOGGER.error("O erro ConsumerProblemException ocorreu. Criando nova transação para envio para a DLQ do consumidor...");
                            kafkaProducer.abortTransaction();
                            kafkaProducer.beginTransaction();

                            sendKafkaMessage("Mensagem para a DLQ do consumidor do tópico A: " + consumerRecord.value(), TOPIC_A_CONSUMER_DLQ);

                            LOGGER.info("Enviando do offset da mensagem do tópico A para ser incluída na transação kafka, fazendo rollback da transação de banco e finalizando processamento do consumo da mensagem com envio para a DLQ do consumidor...");
                            sendOffsetsAndCommitTransaction(consumerRecord);
                            consumerRecordProcessed = true;
                        } catch (ProducerProblemException ppe) {
                            LOGGER.error("O erro ProducerProblemException ocorreu. Criando nova transação para envio para a DLQ do produtor...");
                            kafkaProducer.abortTransaction();
                            kafkaProducer.beginTransaction();

                            sendKafkaMessage("Mensagem para a DLQ do produtor do tópico A: " + consumerRecord.value(), TOPIC_A_PRODUCER_DLQ);

                            LOGGER.info("Enviando do offset da mensagem do tópico A para ser incluída na transação kafka, fazendo rollback da transação de banco e finalizando processamento do consumo da mensagem com envio para a DLQ do produtor...");
                            sendOffsetsAndCommitTransaction(consumerRecord);
                            consumerRecordProcessed = true;
                        } catch (RuntimeException re) {
                            LOGGER.error("O erro \"{}\" ocorreu ao processar a mensagem do tópico A. Reprocessando em 5 segundos...", re.getMessage());
                            kafkaProducer.abortTransaction();
                            Thread.sleep(DELAY_BETWEEN_ATTEMPTS);
                        }
                    }
                }

                var messageList = messageRepository.findAll();
                LOGGER.info("Mensagens no banco de dados: {}", String.join(", ", messageList.stream().map(Message::getMessage).toList()));
            }
        } finally {
            kafkaConsumer.close();
            kafkaProducer.close();
        }
    }

    private void saveMessageDataBase(String messageTopicA) {
        LOGGER.info("Salvando mensagem no banco de dados...");
        var message = new Message(messageTopicA);
        messageRepository.save(message);
    }

    private void sendKafkaMessage(String message, String topic) {
        LOGGER.info("Enviando mensagem para o tópico \"{}\"...", topic);
        var producerRecord = new ProducerRecord<String, String>(topic, message);
        kafkaProducer.send(producerRecord);
    }

    private void sendOffsetsAndCommitTransaction(ConsumerRecord<String, String> consumerRecord) {
        var offsetMap = Collections.singletonMap(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), new OffsetAndMetadata(consumerRecord.offset() + 1));
        kafkaProducer.sendOffsetsToTransaction(offsetMap, kafkaConsumer.groupMetadata());
        kafkaProducer.commitTransaction();
    }
}
