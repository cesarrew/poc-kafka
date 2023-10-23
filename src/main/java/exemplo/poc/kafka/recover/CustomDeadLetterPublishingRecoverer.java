package exemplo.poc.kafka.recover;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import java.util.function.BiFunction;

//Classe criada para customizar a mensagem a ser enviada para a DLQ.
public class CustomDeadLetterPublishingRecoverer extends DeadLetterPublishingRecoverer {

    public CustomDeadLetterPublishingRecoverer(KafkaOperations<?, ?> template, BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {
        super(template, destinationResolver);
    }

    @Override
    protected ProducerRecord<Object, Object> createProducerRecord(ConsumerRecord<?, ?> record, TopicPartition topicPartition, Headers headers, byte[] key, byte[] value) {
        var customValue = "Mensagem para o tópico DLQ: " + record.value();
        return new ProducerRecord<>(topicPartition.topic(), topicPartition.partition(), record.key(), customValue, headers);
    }
}