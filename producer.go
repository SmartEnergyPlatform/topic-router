/*
 * Copyright 2018 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

//var producer = map[string]*kafka.Writer{}
var producer = sync.Map{}


func ClearProducer(){
	log.Println("clear producers")
	keys := []string{}
	producer.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	for _, key := range keys {
		value, ok := producer.Load(key)
		if ok {
			value.(*kafka.Writer).Close()
			producer.Delete(key)
		}
	}
}

func getProducer(topic string)(writer *kafka.Writer){
	if w, ok := producer.Load(topic); ok {
		return w.(*kafka.Writer)
	}
	log.Println("creade producer for", topic)
	broker, err := GetBroker()
	if err != nil {
		log.Fatal("error while getting broker", err)
	}
	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers: broker,
		Topic:   topic,
		Balancer:  &kafka.LeastBytes{},
		MaxAttempts:25,
		Logger: log.New(ioutil.Discard, "", 0),
	})
	producer.Store(topic, writer)
	return writer
}

func Produce(topic string, message string) (err error){
	return getProducer(topic).WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte(message),
			Time: time.Now(),
		},
	)
}


var initConn *kafka.Conn
func InitTopic(topic string) (err error){
	if initConn == nil {
		broker, err := GetBroker()
		if err != nil {
			return err
		}
		if len(broker) == 0 {
			return errors.New("unable to find broker")
		}
		initConn, err = kafka.Dial("tcp", broker[0])
		if err != nil {
			log.Println("ERROR: while init topic connection ", err)
			return err
		}
	}
	return initConn.CreateTopics(kafka.TopicConfig{
		Topic: topic,
		NumPartitions: 1,
		ReplicationFactor:1,
	})
	/*/
	return Produce(topic, "topic_init")
	/**/
}
