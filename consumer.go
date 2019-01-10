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
	"encoding/json"
	"log"

	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

func InitConsumer(topics []string) (chan bool, error) {
	for _, topic := range topics {
		log.Println("init topic: \""+topic+"\"")
		Produce(topic, "topic_init")
	}

	stop := make(chan bool)
	zk, chroot := kazoo.ParseConnectionString(Config.ZookeeperUrl)
	kafkaconf := consumergroup.NewConfig()
	kafkaconf.Version = sarama.V0_10_1_0
	kafkaconf.Consumer.Return.Errors = true
	kafkaconf.Zookeeper.Chroot = chroot
	consumerGroupName := Config.ConsumerGroup
	consumer, err := consumergroup.JoinConsumerGroup(
		consumerGroupName,
		topics,
		zk,
		kafkaconf)

	if err != nil {
		return stop, err
	}

	go func(stop chan bool, consumer *consumergroup.ConsumerGroup) {
		defer consumer.Close()
		startTime := time.Now().Add(time.Duration(-10) * time.Second)
		kafkaTimeout := Config.KafkaTimeout
		useTimeout := true
		if kafkaTimeout <= 0 {
			useTimeout = false
			kafkaTimeout = 3600
		}
		kafkaping := time.NewTicker(time.Second * time.Duration(kafkaTimeout/10))
		kafkatimout := time.NewTicker(time.Second * time.Duration(kafkaTimeout))
		timeouts := map[string]bool{}
		for _, topic := range topics {
			timeouts[topic] = false
		}
		for {
			select {
			case <-kafkaping.C:
				if useTimeout {
					for topic, timeout := range timeouts {
						if timeout {
							log.Println("send ping to ", topic)
							Produce(topic, "topic_init")
						}
					}
				}
			case <-kafkatimout.C:
				if useTimeout {
					for topic, timeout := range timeouts {
						if timeout {
							log.Fatal("ERROR: kafka missing ping timeout in ", topic)
						}
						timeouts[topic] = true
					}
				}
			case <-stop:
				return
			case errMsg := <-consumer.Errors():
				log.Fatal("ERROR: kafka consumer error: ", errMsg)
			case msg, ok := <-consumer.Messages():
				if !ok {
					log.Fatal("empty kafka consumer")
				} else {
					timeouts[msg.Topic] = false
					if msg.Timestamp.Before(startTime) {
						log.Println("WARNING: ignore old message: ", msg.Topic, msg.Timestamp)
						consumer.CommitUpto(msg)
					} else if string(msg.Value) != "topic_init" {
						targets, envelope, err := GetRoutes(msg)
						if err != nil {
							log.Println(err, string(msg.Value))
						} else {
							log.Println(msg.Topic, "(", envelope["device_id"].(string), envelope["service_id"].(string), ") --> ", targets)
							envelope["source_topic"] = msg.Topic
							if len(targets) > 0 {
								resultMsg, err := json.Marshal(envelope)
								if err != nil {
									log.Println("ERROR while marshaling envelope: ", err)
								} else {
									for _, targetTopic := range targets {
										Produce(targetTopic, string(resultMsg))
									}
								}
							}
							consumer.CommitUpto(msg)
						}
					} else {
						consumer.CommitUpto(msg)
					}
				}
			}
		}
	}(stop, consumer)

	return stop, nil
}
