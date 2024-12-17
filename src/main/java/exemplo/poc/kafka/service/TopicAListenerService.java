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
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Collections;

@Service
public class TopicAListenerService {

    private static final String CONSUMER_PROBLEM_IDENTIFICATION = "problema_consumidor";
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
    Caso 1
    ------

    Consumindo mensagem do tópico A, gravando em banco e depois produzindo mensagens para os tópicos B e C. Tentativas infinitas em caso de erro.

    Teste 1: Sem configurar o bean kafkaTransactionManager no consumidor. Usando um único produtor com o mesmo transaction id.
    Resultado: A transação funciona entre produtores. O consumidor fica fora da transação, pois o método "commitSync" é usando para envio dos offsets.

    Teste 2: Configurando o bean kafkaTransactionManager no consumidor. Usando um único produtor com o mesmo transaction id.
    Resultado: A transação funciona de modo geral. O commit dos offsets fica ligado ao commit da transação. Uso do método "sendOffsetsToTransaction" para isso.

    Teste 3: Lançando exceção após produzir mensagem do tópico B. Usando um único produtor com o mesmo transaction id.
    Resultado: É feito rollback na transação de forma geral, incluindo consumidor, banco e produtor. A mensagem do tópico B é produzida, mas não é comitada.

    Conclusão caso 1: Quando não se usa DLQ, deve-se usar um único produtor com um único transaction id (quando uma única transação é desejada). O kafkaTransactionManager deve ser configurado no consumidor para este participar da transação sem precisar usar o método kafkaTemplate.sendOffsetsToTransaction.

    Caso 2
    ------

    Consumindo mensagem do tópico A, gravando em banco e depois produzindo mensagens para os tópicos B e C. Enviando para DLQ após 3 tentativas.

    Teste 1: Usando o mesmo kafkaTemplate ao produzir na DLQ e lançando exceção após produzir mensagem do tópico B.
    Resultado: A mensagem foi comitada na DLQ e offset do consumidor comitado com o "sendOffsetsToTransaction". No entanto, a mensagem do tópico B também foi comitada.

    Teste 2: Usando um kafkaTemplate diferente com outro transaction id ao produzir na DLQ e lançando exceção após produzir mensagem do tópico B.
    Resultado: A mensagem foi comitada na DLQ e offset do consumidor comitado com o "sendOffsetsToTransaction". No entanto, a mensagem do tópico B também foi comitada.

    Teste 3: Usando um kafkaTemplate diferente com outro transaction id ao produzir na DLQ e lançando exceção após produzir mensagem do tópico B. Sem configurar o bean kafkaTransactionManager no consumidor.
    Resultado: A mensagem foi comitada na DLQ e offset do consumidor comitado com o "commitSync". A mensagem do tópico B não foi comitada.

    Teste 4: Usando o mesmo kafkaTemplate ao produzir na DLQ e lançando exceção após produzir mensagem do tópico B. Sem configurar o bean kafkaTransactionManager no consumidor.
    Resultado: A mensagem foi comitada na DLQ e offset do consumidor comitado com o "commitSync". A mensagem do tópico B não foi comitada.

    Teste 5: Usando o mesmo kafkaTemplate ao produzir na DLQ e lançando exceção após produzir mensagem do tópico B. Sem configurar o bean kafkaTransactionManager no consumidor mas usando o método kafkaTemplate.sendOffsetsToTransaction no final da execução do listener.
    Resultado: A mensagem foi comitada na DLQ e offset do consumidor comitado com o "commitSync". A mensagem do tópico B não foi comitada.

    Teste 6: Usando o mesmo kafkaTemplate ao produzir na DLQ e lançando exceção após chamar kafkaTemplate.sendOffsetsToTransaction no final do método. Sem configurar o bean kafkaTransactionManager no consumidor.
    Resultado: A mensagem foi comitada na DLQ e offset do consumidor comitado com o "commitSync". A mensagens dos tópicos B e C não foram comitadas.

    Teste 6: Usando o mesmo kafkaTemplate ao produzir na DLQ e chamando o kafkaTemplate.sendOffsetsToTransaction no final do método. Sem configurar o bean kafkaTransactionManager no consumidor.
    Resultado: A mensagem não foi enviada para a DLQ e o offset do consumidor foi comitado com o "sendOffsetsToTransaction". A mensagens dos tópicos B e C foram comitadas normalmente.

    Conclusão caso 2: Nesse caso, o kafkaTransactionManager não pode ser configurado no consumidor porque em caso de exceção e envio da mensagem para a DLQ, o consumidor usa o "sendOffsetsToTransaction" e comita também a mensagem do, no caso, tópico B, o que não é desejado. É necessário então usar o kafkaTemplate.sendOffsetsToTransaction no final do método listener para que, no caminho feliz, o consumidor use o método Kafka "sendOffsetsToTransaction" e participe da transação.

    Caso 3
    ------

    Consumindo mensagem do tópico A, gravando em banco e depois produzindo mensagens para os tópicos B e C. Enviando para a DLQ do consumidor em caso de erro no consumidor, DLQ do produtor em caso de erro no produtor e tentativas infinitas em caso de erro não mapeado.

    Teste 1: Usando o CommonDelegatingErrorHandler para delegar o problema para os handlers responsáveis.
    Resultado: O tratamento de erros foi feito adequadamente de acordo com o tipo de exceção. Além disso, o rollback foi feito no banco ao enviar para a DLQ.

    Conclusão caso 2: Pode-se mapear problemas conhecidos lançando determinadas exceções e usar o CommonDelegatingErrorHandler para gerenciar qual handler irá tratar. É feito também rollback no banco em caso de necessidade de enviar para a DLQ, o que é desejável.
    */
    @Transactional
    public void processMessage() throws InterruptedException {
        kafkaProducer.initTransactions();

        try {
            kafkaConsumer.subscribe(Collections.singleton(TOPIC_A));

            while (true) {
                var consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                if (consumerRecords.isEmpty()) {
                    continue;
                }

                kafkaProducer.beginTransaction();

                try {
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        LOGGER.info("Início do processamento da mensagem do tópico A.");

                        if (consumerRecord.value().contains(CONSUMER_PROBLEM_IDENTIFICATION)) {
                            throw new ConsumerProblemException("Problema no consumidor. Enviando para a DLQ.");
                        }

                        if (consumerRecord.value().contains(PRODUCER_PROBLEM_IDENTIFICATION)) {
                            throw new ProducerProblemException("Problema no produtor. Enviando para a DLQ.");
                        }

                        if (consumerRecord.value().contains(GENERAL_PROBLEM_IDENTIFICATION)) {
                            throw new RuntimeException("Problema não mapeado. Novas tentativas de processamento serão feitas.");
                        }

                        saveMessageDataBase(consumerRecord.value());
                        sendKafkaMessages(consumerRecord.value());

                        var offsetMap = Collections.singletonMap(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), new OffsetAndMetadata(consumerRecord.offset() + 1));
                        kafkaProducer.sendOffsetsToTransaction(offsetMap, kafkaConsumer.groupMetadata());
                        LOGGER.info("Envio do offset da mensagem do tópico A para ser incluída na transação e fim do processamento.");
                    }

                    kafkaProducer.commitTransaction();
                    LOGGER.info("Transação comitada com sucesso.");
                } catch (ConsumerProblemException cpe) {
                    LOGGER.error("O erro ConsumerProblemException ocorreu ao processar a mensagem do tópico A. Enviando para a DLQ do consumidor.");
                    kafkaProducer.abortTransaction();

                    LOGGER.info("Inciando uma nova transação para enviar a mensagem para a DLQ do consumidor.");
                    kafkaProducer.beginTransaction();

                    var messageTopicAConsumerDLQ = "Mensagem para a DLQ do consumidor do tópico A: " + consumerRecords.get;
                    var producerRecordTopicB = new ProducerRecord<String, String>(TOPIC_B, messageTopicB);
                    kafkaProducer.send(producerRecordTopicB);
                    LOGGER.info("Mensagem para o tópico B enviada.");

                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        kaf.send(new ProducerRecord<>(DLQ_TOPIC, record.key(), "xyz"));
                    }

                    // Commit offsets to mark the message as consumed
                    producer.sendOffsetsToTransaction(getConsumerOffsets(records), GROUP_ID);
                    producer.commitTransaction();
                    System.out.println("Message sent to DLQ and offsets committed.");
                } catch (RuntimeException re) {
                    LOGGER.error("O erro \"{}\" ocorreu ao processar a mensagem do tópico A. Reprocessando em 10 segundos.", re.getMessage());
                    kafkaProducer.abortTransaction();
                    Thread.sleep(10000);
                }
            }
        } finally {
            kafkaConsumer.close();
            kafkaProducer.close();
        }
    }

    private void saveMessageDataBase(String messageTopicA) {
        var message = new Message(messageTopicA);
        messageRepository.save(message);
        LOGGER.info("Mensagem salva no banco de dados");
    }

    private void sendKafkaMessages(String messageTopicA) {
        var messageTopicB = "Mensagem para o tópico B: " + messageTopicA;
        var producerRecordTopicB = new ProducerRecord<String, String>(TOPIC_B, messageTopicB);
        kafkaProducer.send(producerRecordTopicB);
        LOGGER.info("Mensagem para o tópico B enviada.");

        var messageTopicC = "Mensagem para o tópico C: " + messageTopicA;
        var producerRecordTopicC = new ProducerRecord<String, String>(TOPIC_C, messageTopicC);
        kafkaProducer.send(producerRecordTopicC);
        LOGGER.info("Mensagem para o tópico C enviada.");
    }
}
