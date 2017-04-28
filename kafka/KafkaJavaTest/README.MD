## 部署注意
1 服务器启动consumer监听消息：
./bin/kafka-console-consumer.sh --zookeeper david:2181,david2:2181 --from-beginning --topic test   

2 本地直接运行kafkaProducer生产数据。

3 观察服务器consumer接收到的消息。

4 停掉kafkaProducer，本地运行kafkaProducer，观察效果。