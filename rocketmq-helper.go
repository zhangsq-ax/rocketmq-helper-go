package rocketmq_helper_go

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"time"
)

type RocketMQHelperOptions struct {
	NameServers     []string         // name server list
	InstanceId      string           // instance id
	Namespace       string           // namespace
	AccessKey       string           // access key
	SecretKey       string           // secret key
	ConsumerOptions *ConsumerOptions // the default options of consumer
	ProducerOptions *ProducerOptions // the default options of producer
}

type ConsumeFromOptions struct {
	Where consumer.ConsumeFromWhere // message consume model
	Time  *time.Time                // the time to start consume message that if ConsumeFromOptions.Where is consumer.ConsumeFromTimestamp
}

type ConsumerOptions struct {
	Group        string                // consumer group name
	ConsumeModel consumer.MessageModel // consume message model
	ConsumeFrom  *ConsumeFromOptions   // where to start consume messages
}

type ProducerOptions struct {
	Group       string // producer group name
	TraceConfig *primitive.TraceConfig
}

type MessageEntity struct {
	Body       []byte            // message body
	Tag        string            // message tag
	Keys       []string          // message keys
	Properties map[string]string // message properties
}

type ConsumeOptions struct {
	Topic    string                   // the topic name of to be consuming
	Selector consumer.MessageSelector // the selector to be consuming messages
}

// RocketMQHelper RocketMQ helper
type RocketMQHelper struct {
	opts     *RocketMQHelperOptions
	consumer rocketmq.PushConsumer
	producer rocketmq.Producer
}

// NewRocketHelper to create new RocketMQHelper instance
func NewRocketHelper(opts *RocketMQHelperOptions) *RocketMQHelper {
	return &RocketMQHelper{
		opts: opts,
	}
}

// CreateConsumer to create a new push consumer
func (h *RocketMQHelper) CreateConsumer(opts *ConsumerOptions) (rocketmq.PushConsumer, error) {
	globalOpts := h.opts
	if opts == nil {
		if globalOpts.ConsumerOptions == nil {
			return nil, fmt.Errorf("missing default options for consumer")
		}
		opts = globalOpts.ConsumerOptions
	}
	consumerOpts := []consumer.Option{
		consumer.WithNameServer(globalOpts.NameServers),
		consumer.WithGroupName(opts.Group),
		consumer.WithConsumerModel(opts.ConsumeModel),
	}
	if globalOpts.Namespace != "" {
		consumerOpts = append(consumerOpts, consumer.WithNamespace(globalOpts.Namespace))
	}
	if globalOpts.InstanceId != "" {
		consumerOpts = append(consumerOpts, consumer.WithInstance(globalOpts.InstanceId))
	}
	if globalOpts.AccessKey != "" && globalOpts.SecretKey != "" {
		consumerOpts = append(consumerOpts, consumer.WithCredentials(primitive.Credentials{
			AccessKey: globalOpts.AccessKey,
			SecretKey: globalOpts.SecretKey,
		}))
	}
	if opts.ConsumeFrom == nil {
		consumerOpts = append(consumerOpts, consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset))
	} else {
		consumerOpts = append(consumerOpts, consumer.WithConsumeFromWhere(opts.ConsumeFrom.Where))
		if opts.ConsumeFrom.Where == consumer.ConsumeFromTimestamp {
			t := time.Now()
			if opts.ConsumeFrom.Time != nil {
				t = *opts.ConsumeFrom.Time
			}
			consumerOpts = append(consumerOpts, consumer.WithConsumeTimestamp(t.Format("20060102150405")))
		}
	}

	return rocketmq.NewPushConsumer(consumerOpts...)
}

func (h *RocketMQHelper) getConsumer() (rocketmq.PushConsumer, error) {
	if h.consumer == nil {
		c, err := h.CreateConsumer(nil)
		if err != nil {
			return nil, err
		}
		h.consumer = c
	}
	return h.consumer, nil
}

// Consume to start consume messages by internal consumer
func (h *RocketMQHelper) Consume(opts *ConsumeOptions, onMessage func(*primitive.MessageExt) consumer.ConsumeResult) error {
	c, err := h.getConsumer()
	if err != nil {
		return err
	}

	return ConsumeByConsumer(c, opts, onMessage)
}

// CreateProducer create a producer
func (h *RocketMQHelper) CreateProducer(opts *ProducerOptions) (rocketmq.Producer, error) {
	globalOpts := h.opts
	if opts == nil {
		if globalOpts.ProducerOptions == nil {
			return nil, fmt.Errorf("missing default options for producer")
		}
		opts = globalOpts.ProducerOptions
	}
	producerOpts := []producer.Option{
		producer.WithNameServer(globalOpts.NameServers),
		producer.WithGroupName(opts.Group),
	}
	if globalOpts.Namespace != "" {
		producerOpts = append(producerOpts, producer.WithNamespace(globalOpts.Namespace))
	}
	if globalOpts.InstanceId != "" {
		producerOpts = append(producerOpts, producer.WithInstanceName(globalOpts.InstanceId))
	}
	if globalOpts.AccessKey != "" && globalOpts.SecretKey != "" {
		producerOpts = append(producerOpts, producer.WithCredentials(primitive.Credentials{
			AccessKey: globalOpts.AccessKey,
			SecretKey: globalOpts.SecretKey,
		}))
	}
	if opts.TraceConfig != nil {
		producerOpts = append(producerOpts, producer.WithTrace(opts.TraceConfig))
	}

	p, err := rocketmq.NewProducer(producerOpts...)
	if err != nil {
		return nil, err
	}

	return p, p.Start()
}

func (h *RocketMQHelper) getProducer() (rocketmq.Producer, error) {
	if h.producer == nil {
		p, err := h.CreateProducer(nil)
		if err != nil {
			return nil, err
		}
		h.producer = p
	}
	return h.producer, nil
}

// SendMessage send a message by internal consumer
func (h *RocketMQHelper) SendMessage(ctx context.Context, topic string, msg *MessageEntity) (*primitive.SendResult, error) {
	p, err := h.getProducer()
	if err != nil {
		return nil, err
	}

	return SendMessageByProducer(ctx, p, topic, msg)
}

// ConsumeByConsumer consume messages by exists consumer
func ConsumeByConsumer(csm rocketmq.PushConsumer, opts *ConsumeOptions, onMessage func(*primitive.MessageExt) consumer.ConsumeResult) error {
	err := csm.Subscribe(opts.Topic, opts.Selector, func(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range messages {
			result := onMessage(msg)
			if result != consumer.ConsumeSuccess {
				return result, nil
			}
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return err
	}

	return csm.Start()
}

// SendMessageByProducer send a message by exists producer
func SendMessageByProducer(ctx context.Context, prd rocketmq.Producer, topic string, msg *MessageEntity) (*primitive.SendResult, error) {
	message := &primitive.Message{
		Topic: topic,
		Body:  msg.Body,
	}
	if msg.Keys != nil {
		message.WithKeys(msg.Keys)
	}
	if msg.Tag != "" {
		message.WithTag(msg.Tag)
	}
	if msg.Properties != nil {
		message.WithProperties(msg.Properties)
	}

	return prd.SendSync(ctx, message)
}
