# Zenoh Gateway Design

这是一个通过gateway，连接多个zenoh network，以提高整个系统的吞吐量，适应大量producer 发布消息到大量 consumer的情况。

系统规模估算：
- producer 大约23000个，平均每个producer发布2000个topic
- consumer 大约4600个，平均每个consumer订阅1000个topic

![[attachments/gateway 2026-04-24 08.18.15.excalidraw]]

如上图所示，gateway A连接producer zenoh network和consumer zenoh network1， gateway B1，B2 连接consumer network 2

producer发布的消息都属于某个具体topic，topic的格式类似于 tenant_id/dataset_id，这样按照上面的估算producer zenoh network中的topic总数大约4600万，远超zenoh的处理能力，需要采取分片（shard，partition）技术：

producer发布的topic通过shard分片到10000个topic，类似于shard/p0，shard/p1。。。shard/p9999。

gateway的设计，有两种方案：
1. 每个gateway负责一个分片，负责传输此分片的内容到对应的consumer zenoh network
2. gateway不具体绑定某个分片，而是根据其连接的consumer zenoh network中的consumer具体订阅的topic，决定订阅哪些分片