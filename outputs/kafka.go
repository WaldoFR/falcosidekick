package outputs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go/compress"
	"github.com/segmentio/kafka-go/sasl/plain"
	"io/ioutil"
	"log"
	"strings"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/segmentio/kafka-go"

	"github.com/falcosecurity/falcosidekick/types"
)

// NewKafkaClient returns a new output.Client for accessing the Apache Kafka.
func NewKafkaClient(config *types.Configuration, stats *types.Statistics, promStats *types.PromStatistics, statsdClient, dogstatsdClient *statsd.Client) (*Client, error) {
	var compression compress.Compression
	if config.Kafka.Compression != "" {
		var err error
		compression, err = getKafkaCompressionType(config.Kafka.Compression)
		if err != nil {
			return nil, err
		}
	}

	kafkaWriter := &kafka.Writer{
		Addr:        kafka.TCP(strings.Split(config.Kafka.HostPort, ",")...),
		Topic:       config.Kafka.Topic,
		Compression: compression,
	}

	var sasl *plain.Mechanism
	if config.Kafka.Login != "" {
		sasl = &plain.Mechanism{
			Username: config.Kafka.Login,
			Password: config.Kafka.Password,
		}
	}

	var tlsConfig *tls.Config
	certs, _ := x509.SystemCertPool()
	if certs == nil {
		certs = x509.NewCertPool()
	}
	if config.Kafka.CACertFile != "" {
		data, err := ioutil.ReadFile(config.Kafka.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("error reading CACertFile %v, err: %v", config.Kafka.CACertFile, err)
		}

		ok := certs.AppendCertsFromPEM(data)
		if !ok {
			return nil, errors.New("error adding CACertFile in cert pool")
		}
	}
	tlsConfig = &tls.Config{
		ClientCAs:          certs,
		InsecureSkipVerify: false,
	}

	kafkaWriter.Transport = &kafka.Transport{
		TLS:         tlsConfig,
		SASL: sasl,
	}
	return &Client{
		OutputType:      "Kafka",
		Config:          config,
		Stats:           stats,
		PromStats:       promStats,
		StatsdClient:    statsdClient,
		DogstatsdClient: dogstatsdClient,
		KafkaProducer:   kafkaWriter,
	}, nil
}

// KafkaProduce sends a message to a Apach Kafka Topic
func (c *Client) KafkaProduce(falcopayload types.FalcoPayload) {
	c.Stats.Kafka.Add(Total, 1)

	falcoMsg, err := json.Marshal(falcopayload)
	if err != nil {
		c.setKafkaErrorMetrics()
		log.Printf("[ERROR] : Kafka - %v - %v\n", "failed to marshalling message", err.Error())
		return
	}

	kafkaMsg := kafka.Message{
		Value: falcoMsg,
	}

	err = c.KafkaProducer.WriteMessages(context.Background(), kafkaMsg)
	if err != nil {
		if err.Error() == kafka.UnknownTopicOrPartition.Error() && c.Config.Kafka.AutoCreateTopic {
			if errCreatingTopic := c.createKafkaTopic(); errCreatingTopic != nil {
				c.setKafkaErrorMetrics()
				log.Printf("[ERROR] : Kafka - %v\n", errCreatingTopic)
				return
			} else {
				log.Printf("[INFO] : Kafka - Created topic %v\n", c.Config.Kafka.Topic)
			}
		} else {
			c.setKafkaErrorMetrics()
			log.Printf("[ERROR] : Kafka - %v\n", err)
			return
		}
	}

	go c.CountMetric("outputs", 1, []string{"output:kafka", "status:ok"})
	c.Stats.Kafka.Add(OK, 1)
	c.PromStats.Outputs.With(map[string]string{"destination": "kafka", "status": OK}).Inc()
	log.Printf("[INFO] : Kafka - Publish OK\n")
}

// setKafkaErrorMetrics set the error stats
func (c *Client) setKafkaErrorMetrics() {
	go c.CountMetric(Outputs, 1, []string{"output:kafka", "status:error"})
	c.Stats.Kafka.Add(Error, 1)
	c.PromStats.Outputs.With(map[string]string{"destination": "kafka", "status": Error}).Inc()
}

// createKafkaTopic create kafka topic
func (c *Client) createKafkaTopic() error {
	client := kafka.Client{
		Addr:      c.KafkaProducer.Addr,
		Timeout:   0,
		Transport: c.KafkaProducer.Transport,
	}

	_, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Addr: c.KafkaProducer.Addr,
		Topics: []kafka.TopicConfig{
			{
				Topic:             c.KafkaProducer.Topic,
				NumPartitions:     2,
				ReplicationFactor: 4,
			},
		},
		ValidateOnly: false,
	})
	if err != nil {
		return err
	}
	return nil
}

//getKafkaCompressionType return valid compression enum
func getKafkaCompressionType(compression string) (compress.Compression, error) {
	switch compression {
	case compress.Gzip.String():
		return compress.Gzip, nil
	case compress.Snappy.String():
		return compress.Snappy, nil
	case compress.Lz4.String():
		return compress.Lz4, nil
	case compress.Zstd.String():
		return compress.Zstd, nil
	default:
		return 0, errors.New("unknown compression types")
	}
}
