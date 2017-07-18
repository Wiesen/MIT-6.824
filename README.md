# MIT-6.824

Note：2015 lab1通过所有的test case后，由于2016发布因此搁置2015

Introduction
---

利用 goroutine 和 channel 模拟分布式环境：

1. 实现 raft 协议基本内容，在网络延时/分区和数据包丢失/重复/乱序下能够保证数据一致性
2. 基于 Master/Slave 架构和 hash 分片实现 sharded fault-tolerant key/value storage system
	- master 管理配置顺序并响应 Join/Leave/Move/Query RPC 请求
	- slave负责数据存储并响应对应 shards的Get/PutAppend RPC 请求

Details
---

[lab2 Raft Consensus Algorithm Implementation](http://wiesen.github.io/post/MIT-6.824-lab2-Raft-Consensus-Algorithm-Implementation/)

[lab3 Fault-Tolerant Key/Value Service Implementation](http://wiesen.github.io/post/MIT-6.824-lab3-Fault-Tolerant-KVService-Implementation/)

[Lab 4 Sharded KeyValue Service Implementation](http://wiesen.github.io/post/MIT-6.824-Lab4-Sharded-KeyValue-Service/)


