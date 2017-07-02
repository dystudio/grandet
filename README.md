# GRANDET(mysql-databus client sdk)简介

grandet是mysql-databus项目的客户端SDK
使用者引入该项目即可，若环境中编译时，已有mysql-databus的vendor文件下github.com和golang的context包，可以不必再下载vendor

### MYSQL-DATABUS

[Mysql-Databus的SERVER端](https://github.com/swordstick/mysql-databus)

## 主要功能

1. 客户端SDK自行解决数据重发问题，使用者不受干扰
2. 支持客户端对服务端节点的自动分析，无需关注服务端的实际部署
3. 提供使用简单的客户端SDK实时获取数据，数据消费者按自身需要灵活使用获取的数据
4. 支持初始化配置，即获取快照
5. 支持指定BINLOG POS配置，即不获取快照，非初始化
6. DML的Do()函数生产SQL语句时，自动增加SCHEMA，MYSQL输出可直接使用
7. DDL的Query自带SCHEMA,MYSQL输出可直接使用


## SDK文档

### CLIENT SDK 安装及使用

1. [DATABUS CLIENT SDK使用介绍-初始化][1]
2. [DATABUS CLIENT SDK使用介绍-非初始化][2]

### CLIENT SDK架构与设计

1. [MYSQL-DATABUS SDK设计介绍][8]
2. [MYSQL-CLIENT GETEVNET()返回的数据结构][9]



### 鸣谢：

* 感谢[go-mysql][10]的作者siddontang，Mysql-Databus依赖的datapipe最初实现基于go-mysql工具包
* 感谢编写过程中提供各类思路的延允，盟主等好基友

[1]: https://github.com/swordstick/mysql-databus/blob/master/doc/databus_client_sdk%E4%BD%BF%E7%94%A8%E4%BB%8B%E7%BB%8D-%E5%88%9D%E5%A7%8B%E5%8C%96.md
[2]: https://github.com/swordstick/mysql-databus/blob/master/doc/databus_client_sdk%E4%BD%BF%E7%94%A8%E4%BB%8B%E7%BB%8D-%E9%9D%9E%E5%88%9D%E5%A7%8B%E5%8C%96.md
[3]: https://github.com/swordstick/mysql-databus/blob/master/doc/%E4%BA%A4%E4%BA%92%E5%91%BD%E4%BB%A4%E4%BB%8B%E7%BB%8D.md
[5]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-databus_%E6%9E%B6%E6%9E%84%E4%BB%8B%E7%BB%8D.md
[6]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-databus_%E6%9C%8D%E5%8A%A1%E7%AB%AF%E9%AB%98%E5%8F%AF%E7%94%A8%E5%AE%9E%E7%8E%B0.md
[7]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-databus_%E4%BC%A0%E8%BE%93%E6%95%B0%E6%8D%AE%E5%B0%81%E8%A3%85%E4%BB%8B%E7%BB%8D.md
[8]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-databus_sdk%E8%AE%BE%E8%AE%A1%E4%BB%8B%E7%BB%8D.md
[9]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-client_getevnet%E5%87%BD%E6%95%B0%E8%BF%94%E5%9B%9E%E7%9A%84%E6%95%B0%E6%8D%AE%E7%BB%93%E6%9E%84.md
[10]: https://github.com/siddontang/go-mysql