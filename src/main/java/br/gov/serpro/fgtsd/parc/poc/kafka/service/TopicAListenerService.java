package br.gov.serpro.fgtsd.parc.poc.kafka.service;

import br.gov.serpro.fgtsd.parc.poc.kafka.model.Message;
import br.gov.serpro.fgtsd.parc.poc.kafka.repository.MessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TopicAListenerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicAListenerService.class);
    private static final String GROUP_ID = "id_grupo";
    private static final String TOPIC_A = "topico_a";
    private static final String TOPIC_B = "topico_b";
    private static final String TOPIC_C = "topico_c";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private MessageRepository messageRepository;

    /*
    Caso 1
    ------

    Consumindo mensagem do tópico A, gravando em banco e depois produzindo mensagens para os tópicos B e C, com o mesmo produtor, mesmo transaction id.

    Teste 1: Sem configurar o bean kafkaTransactionManager no consumidor.
    Resultado: A transação funciona entre produtores. O consumidor fica fora da transação, pois o método "commitSync" é usando para envio dos offsets.

    Teste 2: Configurando o bean kafkaTransactionManager no consumidor.
    Resultado: A transação funciona de modo geral. O commit dos offsets fica ligado ao commit da transação. Uso do método "sendOffsetsToTransaction" para isso.

    Teste 3: Lançando exceção após produzir mensagem do tópico B.
    Resultado: É feito rollback na transação de forma geral, incluindo consumidor, banco e produtor. A mensagem do tópico B é produzida, mas não é comitada.

    Caso 2
    ------

    Cenário parecido com o caso 1. No entanto, ao invés de retries infinitos, uso de DLQ em caso de erro após 3 tentativas.

    Teste 1: Usando o mesmo kafkaTemplate ao produzir na DLQ e lançando exceção após produzir mensagem do tópico B.
    Resultado: A mensagem foi comitada na DLQ. No entanto, a mensagem do tópico B também foi comitada.

    Teste 2: Usando um kafkaTemplate diferente com outro transaction id ao produzir na DLQ e lançando exceção após produzir mensagem do tópico B.
    Resultado: A mensagem foi comitada na DLQ. No entanto, a mensagem do tópico B também foi comitada.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @KafkaListener(groupId = GROUP_ID, topics = TOPIC_A)
    public void processMessage(String messageTopicA) {
        LOGGER.info("Início do processamento da mensagem do tópico A...");
        saveMessageDataBase(messageTopicA);
        sendKafkaMessages(messageTopicA);
        LOGGER.info("Fim do processamento da mensagem do tópico A...");
    }

    private void saveMessageDataBase(String messageTopicA) {
        var message = new Message(messageTopicA);
        messageRepository.save(message);
    }

    private void sendKafkaMessages(String messageTopicA) {
        var messageTopicB = messageTopicA + " - Destinada ao tópico B";
        var messageTopicC = messageTopicA + " - Destinada ao tópico C";
        kafkaTemplate.send(TOPIC_B, messageTopicB);

        if (1 == 1) {
            throw new RuntimeException();
        }

        kafkaTemplate.send(TOPIC_C, messageTopicC);
    }
}
