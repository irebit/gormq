# gormq

一个简单的golang rabbitmq client 封装

## 依赖
```
github.com/streadway/amqp
```

## 特点
- 采用单conn 多channel的模式
- 支持conn断开重连
- 支持publish并发


## 可能的问题：
- 可能存在信息重复提交的情形