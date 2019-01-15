package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var onceTarget1 sync.Once
var onceTarget2 sync.Once

func Test(t *testing.T) {
	zkcloser, zkPort, zkIp, err := testHelper_getZkDependency()
	defer zkcloser()
	if err != nil {
		t.Error(err)
		return
	}
	kafkacloser, err := testHelper_getKafkaDependency(zkIp)
	defer kafkacloser()
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(3*time.Second)
	configLocation := flag.String("config", "config.json", "configuration file")
	flag.Parse()

	err = LoadConfig(*configLocation)
	if err != nil {
		t.Error(err)
		return
	}
	Config.ZookeeperUrl = "localhost:" + zkPort
	Config.TopicUpdateInterval = 1

	targets := []string{
		"target_1",
	}
	sources := []string{
		"source_1",
	}
	ptsserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/get/routes/") {
			//t.Log("request routes", targets)
			json.NewEncoder(w).Encode(targets)
		}else if r.URL.Path=="/topics"  {
			//t.Log("request topics", sources)
			json.NewEncoder(w).Encode(sources)
		}
	}))
	defer ptsserver.Close()

	Config.TopicPrefixRepoUrl = ptsserver.URL

	ctx1, target1Cancel := context.WithCancel(context.Background())
	defer onceTarget1.Do(target1Cancel)
	target1Err := make(chan error)
	broker, err := GetBroker()
	if err != nil {
		t.Error(err)
		return
	}
	target_1 := []kafka.Message{}
	initKafkaReader(ctx1, target1Err, broker, "test_consumer", "target_1", func(msg kafka.Message) error {
		target_1 = append(target_1, msg)
		return nil
	})

	ctx2, target2Cancel := context.WithCancel(context.Background())
	defer onceTarget2.Do(target2Cancel)
	target2Err := make(chan error)
	broker, err = GetBroker()
	if err != nil {
		t.Error(err)
		return
	}
	target_2 := []kafka.Message{}
	initKafkaReader(ctx2, target2Err, broker, "test_consumer", "target_2", func(msg kafka.Message) error {
		target_2 = append(target_2, msg)
		return nil
	})

	stopCtx, stop := context.WithCancel(context.Background())
	go Init(stopCtx)
	defer stop()

	time.Sleep(10*time.Second)
	err = Produce("source_1", `{"device_id":"td1", "service_id":"ts1", "value":"1"}`)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(10*time.Second)
	targets = []string{
		"target_1",
		"target_2",
	}
	sources = []string{
		"source_1",
		"source_2",
	}
	time.Sleep(10*time.Second)
	err = Produce("source_2", `{"device_id":"td2", "service_id":"ts2", "value":"2"}`)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(10*time.Second)
	onceTarget1.Do(target1Cancel)
	onceTarget2.Do(target2Cancel)

	if len(target_1) != 2 || len(target_2) != 1 {
		for _, target := range target_1 {
			t.Log(string(target.Value))
		}
		for _, target := range target_2 {
			t.Log(string(target.Value))
		}
		t.Error("unexpected result", len(target_1), len(target_2))
		return
	}

	if target_1[0].Topic != "target_1" || target_1[1].Topic != "target_1" || target_2[0].Topic != "target_2" {
		t.Error("unexpected result", target_1, target_2)
		return
	}

	if !testCheckResult(target_1[0], "source_1", "td1", "ts1", "1") {
		t.Error("unexpected result", string(target_1[0].Value))
		return
	}

	if !testCheckResult(target_1[1], "source_2", "td2", "ts2", "2") {
		t.Error("unexpected result", target_1)
		return
	}

	if !testCheckResult(target_2[0], "source_2", "td2", "ts2", "2") {
		t.Error("unexpected result", target_1)
		return
	}
}

func Test2(t *testing.T) {
	zkcloser, zkPort, zkIp, err := testHelper_getZkDependency()
	defer zkcloser()
	if err != nil {
		t.Error(err)
		return
	}
	kafkacloser, err := testHelper_getKafkaDependency(zkIp)
	defer kafkacloser()
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(3*time.Second)
	configLocation := flag.String("config", "config.json", "configuration file")
	flag.Parse()

	err = LoadConfig(*configLocation)
	if err != nil {
		t.Error(err)
		return
	}
	Config.ZookeeperUrl = "localhost:" + zkPort
	Config.TopicUpdateInterval = 1

	targets := []string{
		"target_1",
	}
	sources := []string{
		"source_1",
	}
	ptsserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/get/routes/") {
			//t.Log("request routes", targets)
			json.NewEncoder(w).Encode(targets)
		}else if r.URL.Path=="/topics"  {
			//t.Log("request topics", sources)
			json.NewEncoder(w).Encode(sources)
		}
	}))
	defer ptsserver.Close()

	Config.TopicPrefixRepoUrl = ptsserver.URL

	ctx1, target1Cancel := context.WithCancel(context.Background())
	defer onceTarget1.Do(target1Cancel)
	target1Err := make(chan error)
	broker, err := GetBroker()
	if err != nil {
		t.Error(err)
		return
	}
	target_1 := []kafka.Message{}
	initKafkaReader(ctx1, target1Err, broker, "test_consumer", "target_1", func(msg kafka.Message) error {
		target_1 = append(target_1, msg)
		return nil
	})

	stopCtx, stop := context.WithCancel(context.Background())
	go Init(stopCtx)
	defer stop()

	time.Sleep(1*time.Second)
	err = Produce("source_1", `{"device_id":"td1", "service_id":"ts1", "value":"1"}`)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(1*time.Second)
	err = Produce("source_1", `{"device_id":"td2", "service_id":"ts2", "value":"2"}`)
	if err != nil {
		t.Error(err)
		return
	}
	err = Produce("source_1", `{"device_id":"td3", "service_id":"ts3", "value":"3"}`)
	if err != nil {
		t.Error(err)
		return
	}
	err = Produce("source_1", `{"device_id":"td4", "service_id":"ts4", "value":"4"}`)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(10*time.Second)
	onceTarget1.Do(target1Cancel)

	if len(target_1) != 4  {
		for _, target := range target_1 {
			t.Log(string(target.Value))
		}
		t.Error("unexpected result", len(target_1))
		return
	}

	if target_1[0].Topic != "target_1" || target_1[1].Topic != "target_1" {
		t.Error("unexpected result", target_1)
		return
	}

	if !testCheckResult(target_1[0], "source_1", "td1", "ts1", "1") {
		t.Error("unexpected result", string(target_1[0].Value))
		return
	}

	if !testCheckResult(target_1[1], "source_1", "td2", "ts2", "2") {
		t.Error("unexpected result", target_1)
		return
	}
}

func testCheckResult(message kafka.Message, source string, device string, service string, value string) bool {
	temp := map[string]string{}
	err := json.Unmarshal(message.Value, &temp)
	if err != nil {
		return false
	}
	if temp["device_id"] != device || temp["service_id"] != service || temp["value"] != value || temp["source_topic"] != source  {
		return false
	}
	return true
}


func testHelper_getKafkaDependency(zookeeperUrl string) (closer func(), err error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return func() {},err
	}
	kafkaport, err := GetFreePort()
	if err != nil {
		log.Fatalf("Could not find new port: %s", err)
	}
	networks, _ := pool.Client.ListNetworks()
	hostIp := ""
	for _, network := range networks {
		if network.Name == "bridge" {
			hostIp = network.IPAM.Config[0].Gateway
		}
	}
	log.Println("host ip: ", hostIp)
	env := []string{
		"ALLOW_PLAINTEXT_LISTENER=yes",
		"KAFKA_LISTENERS=OUTSIDE://:9092",
		"KAFKA_ADVERTISED_LISTENERS=OUTSIDE://"+hostIp+":"+strconv.Itoa(kafkaport),
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=OUTSIDE:PLAINTEXT",
		"KAFKA_INTER_BROKER_LISTENER_NAME=OUTSIDE",
		"KAFKA_ZOOKEEPER_CONNECT="+zookeeperUrl+":2181",
	}
	log.Println("start kafka with env ", env)
	kafkaContainer, err := pool.RunWithOptions( &dockertest.RunOptions{Repository:"bitnami/kafka", Tag:"latest", Env: env, PortBindings: map[docker.Port][]docker.PortBinding{
		"9092/tcp": {{HostIP: "", HostPort: strconv.Itoa(kafkaport)}},
	}})
	if err != nil {
		return func() {}, err
	}
	err = pool.Retry(func() error {
		log.Println("try kafka connection...")
		conn, err := kafka.Dial("tcp",hostIp+":"+strconv.Itoa(kafkaport))
		if err != nil {
			log.Println(err)
			return err
		}
		defer conn.Close()
		return nil
	})
	return func() { kafkaContainer.Close() }, err
}

func testHelper_getZkDependency() (closer func(), hostPort string, ipAddress string, err error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return func() {}, "", "", err
	}
	zkport, err := GetFreePort()
	if err != nil {
		log.Fatalf("Could not find new port: %s", err)
	}
	env := []string{}
	log.Println("start zookeeper on ", zkport)
	zkContainer, err := pool.RunWithOptions(&dockertest.RunOptions{Repository: "wurstmeister/zookeeper", Tag: "latest", Env: env, PortBindings: map[docker.Port][]docker.PortBinding{
		"2181/tcp": {{HostIP: "", HostPort: strconv.Itoa(zkport)}},
	}})
	if err != nil {
		return func() {}, "", "", err
	}
	hostPort = strconv.Itoa(zkport)
	err = pool.Retry(func() error {
		time.Sleep(5*time.Second)
		return nil
	})
	return func() { zkContainer.Close() }, hostPort, zkContainer.Container.NetworkSettings.IPAddress, err
}


func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}