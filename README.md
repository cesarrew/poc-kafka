# POC Kafka Transactions with Database Transactions

## Objetivo

Esta prova de conceito (POC) tem como objetivo validar o uso de transações Kafka juntamente com transações de banco de dados. Cada branch deste repositório demonstra uma forma diferente de implementar essa integração, destacando os prós e contras de cada abordagem.

## Cenário Testado

O cenário testado é o seguinte:

1. Consumir mensagem do tópico A.
2. Gravar a mensagem no banco de dados.
3. Produzir mensagens para os tópicos B e C.

### Tratamento de Exceções

- **ConsumerProblemException ou ProducerProblemException**: Após 3 tentativas, a mensagem deve ser enviada para a DLQ (Dead Letter Queue). Nesse caso, a mensagem deve ser marcada como consumida e as mensagens para os tópicos B e C não devem ser comitadas, assim como as operações no banco de dados.
- **Outros Erros**: Realizar tentativas infinitas de reprocessamento.

## Branches

Cada branch deste repositório apresenta uma forma diferente de implementar o cenário proposto.

## Licença

Este projeto está licenciado sob a [MIT License](LICENSE).
