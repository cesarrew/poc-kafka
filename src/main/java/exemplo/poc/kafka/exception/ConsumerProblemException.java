package exemplo.poc.kafka.exception;

public class ConsumerProblemException extends RuntimeException {

    public ConsumerProblemException(String message) {
        super(message);
    }
}