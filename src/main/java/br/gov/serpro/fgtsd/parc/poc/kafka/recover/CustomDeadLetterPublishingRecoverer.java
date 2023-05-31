package br.gov.serpro.fgtsd.parc.poc.kafka.recover;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import java.util.function.BiFunction;

public class CustomDeadLetterPublishingRecoverer extends DeadLetterPublishingRecoverer {

    public CustomDeadLetterPublishingRecoverer(KafkaOperations<?, ?> template, BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {
        super(template, destinationResolver);
    }

    @Override
    protected ProducerRecord<Object, Object> createProducerRecord(ConsumerRecord<?, ?> record, TopicPartition topicPartition, Headers headers, byte[] key, byte[] value) {
        String customValue = "Mensagem para o tópico DLQ: " + record.value();
        return new ProducerRecord<>(topicPartition.topic(), topicPartition.partition(), record.key(), customValue, headers);
    }
}
