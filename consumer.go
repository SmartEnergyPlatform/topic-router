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
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"github.com/wvanbergen/kazoo-go"
	"io"
	"io/ioutil"
	"log"

	"time"
)


func InitKafkaReader(parentContext context.Context, errOut chan<-error, brokers []string, topics []string, startTime time.Time){
	errIn := make(chan error)
	ctx, cancel := context.WithCancel(parentContext)
	for _, topic := range topics{
		initKafkaReader(ctx, errIn, brokers, Config.ConsumerGroup, topic, getKafkaHandler(startTime))
	}
	go func(){
		select {
		case <-parentContext.Done():
			log.Println("close kafka readers ", topics)
		case err := <-errIn:
			log.Println("ERROR: ", err)
			cancel()
			errOut <- err
		}
	}()
}

func initKafkaReader(ctx context.Context, errOut chan<-error, brokers []string, groupId string, topic string, handler func(msg kafka.Message)error){
	log.Println("DEBUG: consume topic: \""+topic+"\"")
	err := InitTopic(topic)
	if err != nil {
		log.Println("ERROR: unable to create topic", err)
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		GroupID:  groupId,
		Topic:     topic,
		MaxWait:  1*time.Second,
		Logger: log.New(ioutil.Discard, "", 0),
		ErrorLogger:log.New(ioutil.Discard, "", 0),
	})
	go func(){
		for {
			select {
			case <-ctx.Done():
				log.Println("close kafka reader ", topic)
				return
			default:
				m, err := r.FetchMessage(ctx)
				if err == io.EOF || err == context.Canceled {
					log.Println("close consumer for topic ", topic)
					return
				}
				if err != nil{
					log.Println("ERROR: while consuming topic ", topic, err)
					errOut<-err
					return
				}
				err = handler(m)
				if err != nil {
					log.Println("ERROR: unable to handle message (no commit)", err)
				}else{
					//log.Println("DEBUG: commit for ", m.Topic)
					err = r.CommitMessages(ctx, m)
				}
			}
		}
	}()
}


func getKafkaHandler(startTime time.Time) func(kafka.Message)error {
	return func(msg kafka.Message) error {
		log.Println("DEBUG: handle msg for", msg.Topic)
		if msg.Time.Before(startTime) {
			log.Println("WARNING: ignore old message: ", msg.Topic, msg.Time)
			return nil
		} else if string(msg.Value) != "topic_init" {
			targets, envelope, err := GetRoutes(msg)
			if err != nil {
				log.Println("ERROR: ", err, string(msg.Value))
			} else {
				log.Println(msg.Topic, "(", envelope["device_id"].(string), envelope["service_id"].(string), ") --> ", targets)
				envelope["source_topic"] = msg.Topic
				if len(targets) > 0 {
					resultMsg, err := json.Marshal(envelope)
					if err != nil {
						log.Println("ERROR while marshaling envelope: ", err)
					} else {
						for _, targetTopic := range targets {
							err = Produce(targetTopic, string(resultMsg))
							if err != nil {
								log.Fatal("ERROR while producing msg", err, targetTopic, resultMsg)
							}
						}
					}
				}
				return nil
			}
		}
		return nil
	}
}


func GetBroker() (brokers []string, err error) {
	return getBroker(Config.ZookeeperUrl)
}

func getBroker(zkUrl string) (brokers []string, err error) {
	zookeeper := kazoo.NewConfig()
	zk, chroot := kazoo.ParseConnectionString(zkUrl)
	zookeeper.Chroot = chroot
	if kz, err := kazoo.NewKazoo(zk, zookeeper); err != nil {
		return brokers, err
	}else{
		return kz.BrokerList()
	}
}
