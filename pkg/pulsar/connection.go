package pulsar

import "github.com/apache/pulsar-client-go/pulsar"

func NewPulsarConnection(config pulsar.ClientOptions) (pulsar.Client, error) {
	return pulsar.NewClient(config)
}
