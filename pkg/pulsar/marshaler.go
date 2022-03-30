package pulsar

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
)

const UUIDHeaderKey = "_watermill_message_uuid"

// Marshaler marshals Watermill's message to Pulsar message.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*pulsar.ProducerMessage, error)
}

// Unmarshaler unmarshals Kafka's message to Watermill's message.
type Unmarshaler interface {
	Unmarshal(consumerMessage *pulsar.ConsumerMessage) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

type DefaultMarshaler struct{}

func (DefaultMarshaler) Marshal(topic string, msg *message.Message) (*pulsar.ProducerMessage, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}

	properties := make(map[string]string)
	properties[UUIDHeaderKey] = msg.UUID

	for key, value := range msg.Metadata {
		properties[key] = value
	}

	return &pulsar.ProducerMessage{
		Payload:    msg.Payload,
		Properties: properties,
	}, nil
}

func (DefaultMarshaler) Unmarshal(pulsarMsg *pulsar.ConsumerMessage) (*message.Message, error) {
	var messageID string
	metadata := make(message.Metadata, len(pulsarMsg.Properties()))

	for key, value := range pulsarMsg.Properties() {
		if string(key) == UUIDHeaderKey {
			messageID = value
		} else {
			metadata.Set(key, value)
		}
	}

	msg := message.NewMessage(messageID, pulsarMsg.Payload())
	msg.Metadata = metadata

	return msg, nil
}
