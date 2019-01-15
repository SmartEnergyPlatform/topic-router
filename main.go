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
	"flag"
	"log"
	"reflect"
	"time"

	"sort"
)

func main() {
	configLocation := flag.String("config", "config.json", "configuration file")
	flag.Parse()

	err := LoadConfig(*configLocation)
	if err != nil {
		log.Fatal(err)
	}

	Init(context.Background())
}

func Init(done context.Context){
	ticker := time.NewTicker(time.Duration(Config.TopicUpdateInterval) * time.Second)
	topics := []string{}
	startTime := time.Now().Add(time.Duration(-10) * time.Second)
	var ctx context.Context
	var stopOldConsumers = func() {}
	errIn := make(chan error)
	for {
		select {
		case <-done.Done():
			log.Println("stop program")
			ClearProducer()
			return
		case err := <-errIn:
			log.Println("ERROR: ", err)
			log.Println("reconnect kafka")
			topics = []string{}
		case <-ticker.C:
		}
		newTopics, err := GetTopics()
		sort.Strings(newTopics)
		//log.Println("check for new topics", newTopics)
		if err != nil {
			log.Fatal("ERROR: while getting new topics ", err)
		}
		if err == nil && !reflect.DeepEqual(topics, newTopics) {
			ClearProducer()
			log.Println("update consumer: ", topics, newTopics)
			broker, err := GetBroker()
			if err != nil {
				log.Fatal("ERROR: while getting broker", err)
			}
			topics = newTopics
			stopOldConsumers()
			ctx, stopOldConsumers = context.WithCancel(context.Background())
			InitKafkaReader(ctx, errIn, broker, topics,startTime)
		}
	}
}
