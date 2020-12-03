# Kfk

Sarama wrapper to produce and consume messages.

Messages are enriched with message type, which is used in the consumer to offer this information to the message handlers

Message handlers implement `messageHandler` interface

Check `kafka_test.go` for more information and examples of use

## Installing

We're using vendor versioning on the modules, for that reason if you want to install the last version you will to do:

```sh
$ go get github.com/chiguirez/kfk/v2
```

# Authors

* [Pau Galindo](https://github.com/paugalindo)
* [Jose I. Ortiz de Galisteo](https://github.com/hosseio)
* [Eduardo R. Golding](https://github.com/K4L1Ma)
