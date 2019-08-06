# Kfk

Sarama wrapper to produce and consume messages.

Messages are enriched with message type, which is used in the consumer to offer this information to the message handlers

Message handlers implement `messageHandler` interface

Check `kafka_test.go` for more information and examples of use
