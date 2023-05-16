package gormq

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/streadway/amqp"
)

func TestPublish(t *testing.T) {
	client, err := NewClient(`amqp://guest:guest@127.0.0.1:5672/`, 3, 3)

	if err != nil {
		panic(err)
	}

	defer client.Close()

	for i := 1; i <= 20; i++ {
		go func(i int) {
			UUID, _ := uuid.NewV4()
			msgId := UUID.String()
			err := client.PublishToExchange("amq.direct", "direct", "r.amq.direct", msgId, []byte(fmt.Sprintf("hello world,%d", i)), map[string]interface{}{})

			if err != nil {
				log.Printf("PublishToExchange error %s", err)
			}
			if i%5 == 0 {
				client.Close()
			}
		}(i)
	}

	time.Sleep(10 * time.Second)
}

func TestContinueAndBreak(t *testing.T) {
	for i := 1; i <= 10; i++ {
		log.Println("i", i)
		for k := 10; k <= 20; k++ {
			if k == 12 {
				break // 结束当前循环
				// continue  跳过此次循环
				// return //结束当前方法
			}
			log.Println("k", k)
		}
	}
}

// 并发测试
func TestConsume(t *testing.T) {

	client, err := NewClient(`amqp://admin:admin@127.0.0.1:5672/`, 3, 3)

	if err != nil {
		panic(err)
	}

	defer client.Close()

	go func(c *Client) {
		for {
			time.Sleep(5 * time.Second)
			client.Close()
		}
	}(client)

	client.Consume("111111", `q.amq.direct`, func(msg amqp.Delivery) error {
		body := string(msg.Body)
		log.Println("receive msg:", msg.MessageId, body)
		time.Sleep(100 * time.Microsecond)
		return nil
	})

}

func TestPublishAndConsume(t *testing.T) {
	client, err := NewClient(`amqp://admin:admin@127.0.0.1:5672/`, 3, 3)

	if err != nil {
		panic(err)
	}

	defer client.Close()

	// go func(c *Client) {
	// 	for {
	// 		time.Sleep(5 * time.Second)
	// 		log.Println("手动关闭链接")
	// 		client.Close()
	// 	}
	// }(client)

	go func() {
		for i := 1; i <= 30; i++ {
			UUID, _ := uuid.NewV4()
			msgId := UUID.String()
			msg := fmt.Sprintf("hello world,%d", i)
			err := client.PublishToExchange("amq.direct", "direct", "r.amq.direct", msgId, []byte(msg), map[string]interface{}{})

			if err != nil {
				log.Printf("PublishToExchange error %s", err)
			}
			log.Println("publish msg:", msg)
			// if i%5 == 0 {
			// 	client.Close()
			// }
			// time.Sleep(time.Second)
		}
	}()

	client.Consume("111111", `q.amq.direct`, func(msg amqp.Delivery) error {
		body := string(msg.Body)
		log.Println("receive msg:", msg.MessageId, body)
		time.Sleep(100 * time.Microsecond)
		return nil
	})

}
