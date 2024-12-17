package exemplo.poc.kafka;

import exemplo.poc.kafka.service.TopicAListenerService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PocKafkaApplication {

    @Autowired
    private TopicAListenerService topicAListenerService;

    public static void main(String[] args) {
        SpringApplication.run(PocKafkaApplication.class, args);
    }

    @PostConstruct
    private void init() throws InterruptedException {
        topicAListenerService.processMessage();
    }
}
