package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/alifpay/cache"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//PubSub kafka publisher subscriber
var (
	prod *kafka.Producer
	cons *kafka.Consumer
)

//Connect set kafka configs and connect to kafka server
func Connect(adr, username, pass string, topics []string) (err error) {

	cons, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        adr,
		"security.protocol":        "SASL_SSL",
		"sasl.mechanisms":          "SCRAM-SHA-256",
		"sasl.username":            username,
		"sasl.password":            pass,
		"go.events.channel.enable": true,
		"group.id":                 "gateapi",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		//"debug":                    "generic,broker,security",
	})
	if err != nil {
		log.Println("Failed to create consumer", err)
		return
	}
	err = cons.SubscribeTopics(topics, nil)
	if err != nil {
		log.Println("Failed to Subscribe", err)
		return
	}

	prod, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": adr,
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "SCRAM-SHA-256",
		"sasl.username":     username,
		"sasl.password":     pass,
		//"debug":             "generic,broker,security",
	})
	if err != nil {
		log.Println("producer ", err)
		return
	}
	return
}

//Close consumer
func Close() {
	prod.Close()
	cons.Close()
}

//Produce send message to kafka
func Produce(topic string, prm interface{}) error {

	valByte, err := json.Marshal(prm)
	if err != nil {
		log.Println("BalanceResp json.Marshal", err)
		return err
	}

	dChan := make(chan kafka.Event)

	err = prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny},
		Value: valByte},
		dChan)
	e := <-dChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		err = m.TopicPartition.Error
	}

	close(dChan)
	return err
}

//Consume get message from kafka
func Consume(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	run := true
	for run {
		select {
		case <-ctx.Done():
			log.Println("Caught signal to stop consumer:")
			wg.Done()
			run = false
		case ev := <-cons.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				log.Println("AssignedPartitions", e)
				cons.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				log.Println("RevokedPartitions", e)
				cons.Unassign()
			case *kafka.Message:
				//send message to handler ??
				obj := StrObj{}
				if err := json.Unmarshal(e.Value, &obj); err != nil {
					log.Println("kafka.Message json.Unmarshal", err, string(e.Value))
				}
				cache.Set(obj.ID, []byte("info from Kafka consumer"), 40)
				fmt.Println(string(e.Value))
				//cache.Set(e.Value)
			case kafka.PartitionEOF:
				log.Println("Reached: ", e)
			case kafka.Error:
				log.Println("Error: ", e)
			}
		}
	}
}

type StrObj struct {
	ID  string
	Val string
}
