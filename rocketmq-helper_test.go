package rocketmq_helper_go

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	_ "github.com/stretchr/testify"
	"os"
	"strconv"
	"testing"
	"time"
)

var (
	helper     *RocketMQHelper
	RMQ_SERVER = os.Getenv("RMQ_SERVER")
	ACCESS_KEY = os.Getenv("RMQ_ACCESS_KEY")
	SECRET_KEY = os.Getenv("RMQ_SECRET_KEY")
	GROUP      = os.Getenv("RMQ_GROUP")
	TOPIC      = os.Getenv("RMQ_TOPIC")
	timer      *time.Timer
)

func TestNewRocketHelper(t *testing.T) {
	rlog.SetLogLevel("error")
	helper = NewRocketHelper(&RocketMQHelperOptions{
		NameServers: []string{RMQ_SERVER},
		AccessKey:   ACCESS_KEY,
		SecretKey:   SECRET_KEY,
		ConsumerOptions: &ConsumerOptions{
			Group:        GROUP,
			ConsumeModel: consumer.Clustering,
			ConsumeFrom: &ConsumeFromOptions{
				Where: consumer.ConsumeFromLastOffset,
			},
		},
		ProducerOptions: &ProducerOptions{
			Group: GROUP,
		},
	})
	timer = time.NewTimer(30 * time.Second)
}

func TestRocketMQHelper_Consume(t *testing.T) {
	go func() {
		err := helper.Consume(&ConsumeOptions{
			Topic: TOPIC,
			Selector: consumer.MessageSelector{
				Type:       consumer.TAG,
				Expression: "helper_test",
			},
		}, func(msg *primitive.MessageExt) consumer.ConsumeResult {
			//t.Log(msg)
			fmt.Println(msg)
			return consumer.ConsumeSuccess
		})
		if err != nil {
			t.Error(err)
		}
	}()
}

func TestRocketMQHelper_SendMessage(t *testing.T) {
	i := 1
	for {
		select {
		case <-timer.C:
			return
		default:
			tag := "helper_test"
			if i%2 == 0 {
				tag = ""
			}
			t.Logf("send message: %s\n", tag)
			_, err := helper.SendMessage(context.Background(), TOPIC, &MessageEntity{
				Body: []byte(strconv.Itoa(int(time.Now().UnixMilli()))),
				Tag:  tag,
			})
			if err != nil {
				t.Error(err)
			}
			i++
			time.Sleep(2 * time.Second)
		}
	}
}
