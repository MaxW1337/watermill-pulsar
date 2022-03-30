package pulsar

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/pulsar-client-go/pulsar"
	jsoniter "github.com/json-iterator/go"
	"sync"
)

type Publisher struct {
	client    pulsar.Client
	logger    watermill.LoggerAdapter
	producers map[string]pulsar.Producer
	lock      sync.Mutex
}

func NewPublisher(config pulsar.ClientOptions, logger watermill.LoggerAdapter) (*Publisher, error) {
	client, err := pulsar.NewClient(config)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		client:    client,
		logger:    logger,
		producers: make(map[string]pulsar.Producer),
	}, nil
}

func (p *Publisher) getProducer(topic string) (pulsar.Producer, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if producer, exists := p.producers[topic]; exists {
		return producer, nil
	}

	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		return nil, err
	}

	p.producers[topic] = producer

	return producer, nil
}

// Publish publishes message to Pulsar.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		messageFields := watermill.LogFields{
			"message_uuid": msg.UUID,
			"topic_name":   topic,
		}

		p.logger.Trace("Publishing message", messageFields)

		b, err := jsoniter.Marshal(msg)
		if err != nil {
			return err
		}

		producer, err := p.getProducer(topic)
		if err != nil {
			return err
		}

		_, err = producer.Send(msg.Context(), &pulsar.ProducerMessage{Payload: b})
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Publisher) Close() error {
	p.logger.Trace("Closing publisher", nil)
	defer p.logger.Trace("PulsarPublisher closed", nil)

	p.client.Close()

	return nil
}
