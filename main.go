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
	"flag"
	"log"
	"os"

	"reflect"
	"time"

	"sort"

	"github.com/Shopify/sarama"
)

func main() {
	configLocation := flag.String("config", "config.json", "configuration file")
	flag.Parse()

	err := LoadConfig(*configLocation)
	if err != nil {
		log.Fatal(err)
	}

	if Config.SaramaLog == "true" {
		sarama.Logger = log.New(os.Stderr, "[Sarama] ", log.LstdFlags)
	}

	Init()
	defer CloseProducer()
}

func Init() {
	ticker := time.NewTicker(time.Duration(Config.TopicUpdateInterval) * time.Second)
	stop := make(chan bool)
	topics := []string{}
	for {
		newTopics, err := GetTopics()
		sort.Strings(newTopics)
		if err != nil {
			log.Println("ERROR: ", err)
		}
		if err == nil && !reflect.DeepEqual(topics, newTopics) {
			log.Println("update consumer: ", topics, newTopics)
			newStop, err := InitConsumer(newTopics)
			if err != nil {
				log.Println("ERROR: ", err)
			} else {
				topics = newTopics
				close(stop)
				stop = newStop
				log.Println("successfull consumer update: ", topics)
			}
		}
		<-ticker.C
	}
}
