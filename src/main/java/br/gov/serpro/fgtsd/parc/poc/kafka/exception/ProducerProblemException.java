package br.gov.serpro.fgtsd.parc.poc.kafka.exception;

public class ProducerProblemException extends RuntimeException {

    public ProducerProblemException(String message) {
        super(message);
    }
}