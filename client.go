package gormq

import (
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Client struct {
	AmqpUrl        string
	ConnID         int
	Conn           *amqp.Connection
	ConnRetryTimes int
	ChanSize       int
	CChan          chan *Channel
	ReConnChan     chan int //失败重连队列
	m              *sync.Mutex
	ConnStatus     bool               //链接状态
	CloseChan      chan *amqp.Error   //conn关闭通知
	BlockChan      chan amqp.Blocking //conn block通知
}

type Channel struct {
	ConnID      int
	ChaId       int
	Cha         *amqp.Channel
	ConfirmChan chan amqp.Confirmation //确保消费
	PublishChan chan amqp.Confirmation //确保接收
	CloseChan   chan *amqp.Error       //channel关闭通知

}

func NewClient(amqpURL string, chanSize int, connRetryTimes int) (*Client, error) {
	client := &Client{
		AmqpUrl:        amqpURL,
		CChan:          make(chan *Channel, chanSize*2),
		ConnRetryTimes: connRetryTimes,
		m:              new(sync.Mutex),
		ChanSize:       chanSize,
		ConnID:         0,
		ReConnChan:     make(chan int),
		ConnStatus:     false,
	}

	err := client.Init()

	go func(c *Client) {
		for {
			select {
			case connId := <-c.ReConnChan:
				log.Println("收到重连消息，判断是否需要重连", connId, c.ConnID)
				if c.ConnID == connId {
					if err := c.Init(); err != nil {
						log.Print("重连出现异常", err)
						c.ConnStatus = false
					}
				}
			}
		}
	}(client)

	return client, err
}

func (c *Client) Init() error {
	log.Println("init start")
	t := 0
	for {
		//连接失败，重试三次，每次间隔1s
		if err := c.Connect(); err != nil {
			t = 1
			time.Sleep(time.Second)
			log.Printf("Connect %s error %s, will retry", c.AmqpUrl, err.Error())

			if t < c.ConnRetryTimes {
				continue
			} else {
				return err
			}
		}

		c.ConnID += 1
		c.ConnStatus = true

		c.CloseChan = c.Conn.NotifyClose(make(chan *amqp.Error))
		c.BlockChan = c.Conn.NotifyBlocked(make(chan amqp.Blocking))

		//链接成功后，初始化channel
		if err := c.InitChannel(c.ChanSize); err != nil {
			log.Printf("GetChannel %s error %s", c.AmqpUrl, err.Error())
			return err
		}
		return nil
	}
}

// connect
func (c *Client) Connect() (err error) {
	c.m.Lock()
	defer c.m.Unlock()
	c.Conn, err = amqp.Dial(c.AmqpUrl)
	return err
}

// close
func (c *Client) Close() error {
	return c.Conn.Close()
}

// initChannel
func (c *Client) InitChannel(size int) error {

	for i := 0; i < size; i++ {
		cha, err := c.Conn.Channel()
		if err != nil {
			return err
		}
		if err := cha.Confirm(false); err != nil {
			return err
		}

		channel := &Channel{
			ConnID:      c.ConnID,
			ChaId:       c.ConnID*100 + i,
			Cha:         cha,
			ConfirmChan: make(chan amqp.Confirmation, 1),
			PublishChan: make(chan amqp.Confirmation, 1),
			CloseChan:   make(chan *amqp.Error, 1),
		}

		cha.NotifyClose(channel.CloseChan)

		cha.NotifyPublish(channel.PublishChan)

		c.CChan <- channel
	}
	return nil
}

/**
 * exchangeType  string  "direct", "fanout", "topic" and  "headers"
 */
func (c *Client) PublishToExchange(exchangeName, exchangeType, routingKey string, msgId string, b []byte) error {
	if !c.ConnStatus {
		log.Println("链接状态异常", exchangeName, exchangeType, routingKey, msgId, string(b))
		return nil
	}
	for {
		select {
		case channel := <-c.CChan:
			if !c.IsParentConn(channel) {
				//抛弃非当前链接的channel
				continue
			}
			err := channel.Cha.Publish(
				exchangeName,
				routingKey,
				false, //不保证能将信息分配到queue
				false, //不保证queue有消费者
				amqp.Publishing{
					MessageId:    msgId,
					DeliveryMode: amqp.Persistent,
					ContentType:  "text/plain",
					Body:         b,
				})
			if err != nil {
				log.Println("--- failed to publish to queue, trying to resend ---", channel.ConnID, channel.ChaId, string(b))
				c.ReConnChan <- channel.ConnID
				// 更换重试
				continue
			}
			log.Printf("--- published to exchange '%s',msg:'%s' %d %d ---\r\n", routingKey, string(b), channel.ConnID, channel.ChaId)

			// 交换机确认接受 NotifyPublic
			// 确认信息发送到 NotifyConfirm

			//channel关闭
			select {
			case confirm := <-channel.PublishChan:
				log.Printf("收到确认通知%v %d %d %s", confirm, channel.ConnID, channel.ChaId, string(b))
				if !confirm.Ack {
					c.ReConnChan <- channel.ConnID
					continue
				} else {
					c.CChan <- channel
				}
			case <-channel.CloseChan:
				log.Println("收到信道关闭的通知 channel.CloseChan", channel.ConnID, channel.ChaId, string(b))
				c.ReConnChan <- channel.ConnID
				continue
			case <-time.After(2 * time.Second):
				c.ReConnChan <- channel.ConnID

				log.Println("接受确认信息超时", channel.ConnID, channel.ChaId, string(b))
			}
			return nil
		}
	}
}

/**
 *
 */
func (c *Client) Consume(consumeId string, queueName string, f func([]byte) error) {
	if !c.ConnStatus {
		log.Println("conn状态异常，无法监听队列", queueName)
		return
	}

	for {
		select {
		case channel := <-c.CChan:
			m, err := channel.Cha.Consume(
				queueName,
				consumeId,
				false,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				log.Println("--- failed to consume from queue, trying again ---")
				c.ReConnChan <- channel.ConnID
				continue
			}

			shouldBreak := false
			for {
				if shouldBreak {
					c.ReConnChan <- channel.ConnID
					break
				}
				select {
				case <-c.BlockChan:
					log.Println("--- connection blocked ---")
					shouldBreak = true
				case <-c.CloseChan:
					log.Println("--- connection closed ---")
					shouldBreak = true

				case <-channel.CloseChan:
					log.Println("--- channel closed ---")
					shouldBreak = true
				case d := <-m:
					err := f(d.Body)
					log.Println("消费，返回值", string(d.Body), err)
					if err != nil {
						_ = d.Reject(true)
					} else {
						_ = d.Ack(true)
					}
				}
			}
		}
	}
}

func (c *Client) IsParentConn(cha *Channel) bool {
	return cha.ConnID == c.ConnID
}
