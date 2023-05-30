package br.gov.serpro.fgtsd.parc.poc.kafka.repository;

import br.gov.serpro.fgtsd.parc.poc.kafka.model.Message;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MessageRepository extends JpaRepository<Message, Long> {
}
