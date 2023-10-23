package exemplo.poc.kafka.exception;

public class ProducerProblemException extends RuntimeException {

    public ProducerProblemException(String message) {
        super(message);
    }
}