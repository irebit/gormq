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
	ReConnChan     chan *Channel //失败重连队列
	m              *sync.Mutex
	ConnStatus     bool               //链接状态
	CloseChan      chan *amqp.Error   //conn关闭通知
	BlockChan      chan amqp.Blocking //conn block通知
}

type Channel struct {
	ConnID         int
	ChaID          int
	Cha            *amqp.Channel
	ConfirmChan    chan amqp.Confirmation //确保消费
	PublishChan    chan amqp.Confirmation //确保接收
	CloseChan      chan *amqp.Error       //channel关闭通知
	ChannelTimeout int64                  //超时未确认
	TimeoutTimer   *time.Timer            //超时
}

func NewClient(amqpURL string, chanSize int, connRetryTimes int) (*Client, error) {
	client := &Client{
		AmqpUrl:        amqpURL,
		CChan:          make(chan *Channel, chanSize*2),
		ConnRetryTimes: connRetryTimes,
		m:              new(sync.Mutex),
		ChanSize:       chanSize,
		ConnID:         0,
		ReConnChan:     make(chan *Channel),
		ConnStatus:     false,
	}

	err := client.Init()

	go func(c *Client) {
		for {
			select {
			case channel := <-c.ReConnChan:
				log.Println("Receive reconn event", channel.ConnID, c.ConnID)
				if c.ConnID == channel.ConnID {
					c.Close()
					if err := c.Init(); err != nil {
						log.Print("reconn Fail.", err)
						c.ConnStatus = false
					} else {
						log.Print("reconn Success.")
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
			log.Printf("Init Connect %s error %s, will retry", c.AmqpUrl, err.Error())

			if t < c.ConnRetryTimes {
				continue
			} else {
				return err
			}
		}

		//链接成功后，初始化channel
		if err := c.InitChannel(c.ChanSize); err != nil {
			log.Printf("Init GetChannel %s error %s", c.AmqpUrl, err.Error())
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

	if err == nil {
		c.ConnID += 1
		c.ConnStatus = true
		c.CloseChan = c.Conn.NotifyClose(make(chan *amqp.Error))
		c.BlockChan = c.Conn.NotifyBlocked(make(chan amqp.Blocking))
	}

	return err
}

// close
func (c *Client) Close() error {
	log.Println("Client::Close", c.ConnID)
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
			ConnID:         c.ConnID,
			ChaID:          c.ConnID*100 + i,
			Cha:            cha,
			ChannelTimeout: 2,
		}

		channel.CloseChan = cha.NotifyClose(make(chan *amqp.Error, 1))
		channel.PublishChan = cha.NotifyPublish(make(chan amqp.Confirmation, 1))
		channel.TimeoutTimer = time.NewTimer(time.Duration(channel.ChannelTimeout) * time.Second)
		c.CChan <- channel
	}
	return nil
}

/**
 * exchangeType  string  "direct", "fanout", "topic" and  "headers"
 */
func (c *Client) PublishToExchange(exchangeName, exchangeType, routingKey string, msgId string, b []byte, header map[string]interface{}) error {

	for {

		if !c.ConnStatus {
			log.Println("PublishToExchange conn ConnStatus invalid:", exchangeName, exchangeType, routingKey, msgId, string(b))
			return nil
		}

		select {
		case channel := <-c.CChan:
			channel.TimeoutTimer.Reset(time.Duration(channel.ChannelTimeout) * time.Second)
			if !c.IsParentConn(channel) {
				continue
			}
			err := channel.Cha.Publish(
				exchangeName,
				routingKey,
				false, //不保证能将信息分配到queue
				false, //不保证queue有消费者
				amqp.Publishing{
					Headers:      amqp.Table(header),
					MessageId:    msgId,
					DeliveryMode: amqp.Persistent,
					ContentType:  "text/plain",
					Body:         b,
				})
			if err != nil {
				log.Println("PublishToExchange Fail.", channel.ConnID, channel.ChaID, string(b))
				c.ReConnChan <- channel
				continue
			}
			log.Println("PublishToExchange Success.", channel.ConnID, channel.ChaID, string(b))

			select {
			case confirm := <-channel.PublishChan:
				log.Println("PublishToExchange receive confirm.", channel.ConnID, channel.ChaID, string(b), confirm.Ack)
				if !confirm.Ack {
					c.ReConnChan <- channel
					continue
				} else {
					c.CChan <- channel
				}
			case <-channel.CloseChan:
				log.Println("PublishToExchange channel closed.", channel.ConnID, channel.ChaID, string(b))
				c.ReConnChan <- channel
				continue
			case <-channel.TimeoutTimer.C:
				c.ReConnChan <- channel
				log.Println("PublishToExchange confirm timeout.", channel.ConnID, channel.ChaID, string(b))
			}
			return nil
		}
	}
}

/**
 *
 */
func (c *Client) Consume(consumeId string, queueName string, f func(amqp.Delivery) error) {
	for {

		if !c.ConnStatus {
			log.Println("Consume conn ConnStatus invalid:", queueName)
			return
		}

		select {
		case channel := <-c.CChan:
			if !c.IsParentConn(channel) {
				continue
			}
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
				log.Println("Consume Fail", channel.ConnID, channel.ChaID)
				c.ReConnChan <- channel
				continue
			}

			shouldBreak := false
			for {
				if shouldBreak {
					c.ReConnChan <- channel
					break
				}
				select {
				case <-c.BlockChan:
					log.Println("Consume connection blocked", channel.ConnID, channel.ChaID)
					shouldBreak = true
				case <-c.CloseChan:
					log.Println("Consume connection closed", channel.ConnID, channel.ChaID)
					shouldBreak = true
				case <-channel.CloseChan:
					log.Println("Consume channel closed", channel.ConnID, channel.ChaID)
					shouldBreak = true
				case d := <-m:
					err := f(d)
					log.Println("Consume receive message", string(d.MessageId), err)
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
