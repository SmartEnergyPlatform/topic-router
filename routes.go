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
	"errors"
	"net/url"

	"encoding/json"
	"net/http"

	"log"

	"github.com/Shopify/sarama"
)

type PrefixMessage struct {
	DeviceId    string      `json:"device_id,omitempty"`
	ServiceId   string      `json:"service_id,omitempty"`
	RoutingInfo string      `json:"routing_info,omitempty"`
	Value       interface{} `json:"value"`
	AsString    bool        `json:"as_string"`
}

type Envelope struct {
	DeviceId    string      `json:"device_id,omitempty"`
	ServiceId   string      `json:"service_id,omitempty"`
	Value       interface{} `json:"value"`
	SourceTopic string      `json:"source_topic"`
}

func GetRoutes(msg *sarama.ConsumerMessage) (result []string, envelope map[string]interface{}, err error) {
	topic := url.QueryEscape(msg.Topic)
	err = json.Unmarshal(msg.Value, &envelope)
	if err != nil {
		log.Println("ERROR while unmarshaling envelope")
		return
	}
	deviceid, ok := envelope["device_id"].(string)
	if !ok {
		err = errors.New("invalid envelope")
		return
	}
	serviceId, ok := envelope["service_id"].(string)
	if !ok {
		err = errors.New("invalid envelope")
		return
	}
	uri := topic + "/" + url.QueryEscape(deviceid) + "/" + url.QueryEscape(serviceId)
	resp, err := http.Get(Config.TopicPrefixRepoUrl + "/get/routes/" + uri)
	if err != nil {
		return result, envelope, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&result)
	return
}

func GetTopics() (result []string, err error) {
	temp := []string{}
	resp, err := http.Get(Config.TopicPrefixRepoUrl + "/topics")
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return
	}
	for _, topic := range temp {
		if topic != "" && topic != " " {
			result = append(result, topic)
		}
	}
	return
}
