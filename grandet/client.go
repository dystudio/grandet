package grandet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
	"github.com/siddontang/go/ioutil2"
)

var errCanalClosed = errors.New("canal was closed")
var MysqlTimeout string = "?timeout=10000ms&writeTimeout=10000ms&readTimeout=10000ms"

type KafkaKey struct {
	Pos    mysql.Position
	Action string
}

type EventDiscrip struct {
	Header *KafkaKey
	Event  interface{}
}

type ReplicateInfo struct {
	MasterName string `toml:"mastername"`
	MasterPos  uint32 `toml:"masterpos"`
	Offset     int64  `toml:"offset"`
}

func LoadRepliInfo(name string) (*ReplicateInfo, error) {
	var m ReplicateInfo

	f, err := os.Open(name)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		m.MasterName = "PLEASERELOAD"
		return &m, nil
	}
	defer f.Close()

	_, err = toml.DecodeReader(f, &m)

	return &m, err
}

type Config struct {
	Brokers       []string `toml:"brokers"`
	ZkPath        []string `toml:"zkpath"`
	NodeName      string   `toml:"nodename"`
	Topic         string   `toml:"topic"`
	FirstLoad     bool     `toml:"firstload"`
	Serverip      string   `toml:"serverip"`
	Serverport    string   `toml:"serverport"`
	LogFile       string   `toml:"logfile"`
	LogLevel      string   `toml:"log_level"`
	LogDir        string   `toml:"log_dir"`
	Mirrorenable  bool     `toml:"mirror_enable"`
	KeepAlivepath string   `toml:"keepalivepath"`
	Replinfopath  string   `toml:"replinfopath"`
	MasterName    string   `toml:"mastername"`
	MasterPos     uint32   `toml:"masterpos"`
}

type Client struct {
	m    sync.Mutex
	cfg  *Config
	rpl  *ReplicateInfo
	Dump DumpSyncMetaData
	ch   chan *EventDiscrip

	rsLock     sync.Mutex
	rsHandlers []RowsEventHandler

	tableLock sync.Mutex
	tables    map[string]*schema.Table
}

func NewClient(cfg *Config) (*Client, error) {

	c := new(Client)
	c.cfg = cfg

	c.ch = make(chan *EventDiscrip)

	// 指定log的级别和日志文件
	log.SetLevelByString(cfg.LogLevel)
	log.SetOutputByName(path.Join(cfg.LogDir, cfg.LogFile))

	log.Info("LoadRepliInfo\n")
	// 初始化当前OFFSET位置
	if len(cfg.Replinfopath) == 0 {
		cfg.Replinfopath = "./Repl.info"
	}
	rpl, err := LoadRepliInfo(cfg.Replinfopath)
	if err != nil {
		log.Fatalf("parse replicateInfo file failed(%s): %s", cfg.Replinfopath, err)
	}

	c.rpl = rpl

	if c.rpl.MasterName != "PLEASERELOAD" {
		return c, nil
	}

	// 判断是否不落地，并且没有Repl文件，且有外部参数录入
	if rpl.MasterName == "PLEASERELOAD" && !cfg.FirstLoad {
		if len(cfg.MasterName) == 0 || cfg.MasterPos == 0 {
			log.Error("When FirstLoad false,and Have not Repl.info,Your MasterName and MasterPos is Wrong !")
			return nil, errors.New("Wrong MasterName or MasterPos")
		}
	}

	// 判断是否不落地，并且没有Repl文件，且有外部参数录入
	if rpl.MasterName == "PLEASERELOAD" && !cfg.FirstLoad {
		log.Info("GetOffset while Start by MasterName and Master Pos \n")
		offsetNew, err := GetOffset(cfg)
		if err != nil {
			log.Error(err)
			return nil, errors.New("Can Not Get Offset by Configuration")
		}

		c.rpl = &ReplicateInfo{MasterName: cfg.MasterName, MasterPos: cfg.MasterPos, Offset: offsetNew}
		return c, nil
	}

	log.Info("Get Zk Message\n")
	// 获取ZK的信息
	initPath := "/Databus"
	zkConn, connErr := ZkConnection(cfg.ZkPath, initPath)
	if connErr != nil {
		panic(connErr)
	}
	defer zkConn.Close()

	value, _ := zkConn.Get(cfg.NodeName + "/" + cfg.Topic)
	zkDumpMetadata, err := DumpsyncMetaDataDecode(value)
	if err != nil {
		panic(err)
	}

	c.Dump = zkDumpMetadata
	fmt.Printf("c.Dump is %v", c.Dump)

	log.Infof("ZK MESSAGE: Offset is %d, Dumpfile is %s \n", zkDumpMetadata.Offset, zkDumpMetadata.Dumpfile)

	log.Info("Get KeepAliveNode \n")
	// 获取Server的KeepAliveNode监听信息
	if cfg.Mirrorenable {
		ok, _ := checkMasterAliveClient(cfg)
		if !ok {
			log.Error("In cluster mode,can not get Master ip/port from zk")
		}
		log.Infof("Get Master IP %s, Port %s", cfg.Serverip, cfg.Serverport)
	}

	return c, nil
}

func (c *Client) ClientStart() {
	go c.start()
}

func (c *Client) start() {
	// Dump数据获取启动
	c.getDump()
	// 获取KAFKA中BinlogEvent数据
	c.getKafka()
}

func (c *Client) getDump() {

	// 获取文件数据
	if c.cfg.FirstLoad && c.rpl.MasterName == "PLEASERELOAD" {
		log.Info("Start Get Dump Rows !\n")
		// 获取文件数据
		c.NewFileCapture(c.cfg.Serverip, c.cfg.Serverport, c.Dump.Dumpfile)
		c.rpl.MasterName = c.Dump.Name
		c.rpl.MasterPos = c.Dump.Pos
		c.rpl.Offset = c.Dump.Offset
		SaveRpl(c.cfg.Replinfopath, c.rpl)
	}
}

func (c *Client) getKafka() {

	log.Info("Start Get Kafka Rows and Query !\n")

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create new consumer
	master, err := sarama.NewConsumer(c.cfg.Brokers, config)
	if err != nil {
		panic(err)
	}

	consumer, err := master.ConsumePartition(c.cfg.Topic, 0, c.rpl.Offset)
	if err != nil {
		panic(err)
	}

	var key KafkaKey

	var msgCount int

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				if myKey := parseJsonKey(msg.Key); myKey != nil {
					value, ok := myKey.(KafkaKey)
					if !ok {
						log.Warning("type assertion is wrong")
						continue
					}

					strs := strings.Split(c.rpl.MasterName, ".")
					binlogInt, _ := strconv.ParseUint(strs[1], 10, 32)

					strsNext := strings.Split(value.Pos.Name, ".")
					binlogIntNext, _ := strconv.ParseUint(strsNext[1], 10, 32)

					// 客户端消除Kafka重发问题
					if binlogInt > binlogIntNext {
						log.Debug("Give up Older Message from Kafka !\n")
						continue
					}

					if binlogInt == binlogIntNext && c.rpl.MasterPos >= value.Pos.Pos {
						log.Debug("Give up Older Message from Kafka !\n")
						continue
					}

					// 已消除重发
					c.rpl.MasterName = value.Pos.Name
					c.rpl.MasterPos = value.Pos.Pos
					c.rpl.Offset = msg.Offset
					key = value
				}

				if key.Action == "DML" {
					if mydata := parseJsonValueRows(msg.Value); mydata != nil {
						value, ok := mydata.(RowsEvent)
						if !ok {
							log.Warning("type assertion is wrong")
							continue
						}
						valueDecode := DeBase64RowsEvent(value)
						sp := &EventDiscrip{Header: &key, Event: valueDecode}
						c.ch <- sp
						log.Debug("Offset is", msg.Offset)
						log.Debug("Received messages", value.Action, value.Table.Schema, value.Table.Name)

					}
				}

				if key.Action == "DDL" {
					if mydata := parseJsonValueQuery(msg.Value); mydata != nil {
						value, ok := mydata.(QueryEvent)
						if !ok {
							log.Warning("type assertion is wrong")
							continue
						}

						sp := &EventDiscrip{Header: &key, Event: value}
						c.ch <- sp
						log.Debug("Offset is", msg.Offset)
						log.Debug("Received messages", value.Action, value.Table.Schema, value.Table.Name)
					}
				}

				SaveRpl(c.cfg.Replinfopath, c.rpl)

			}
		}
	}()
}

func (c *Client) GetEvent(ctx context.Context) (*EventDiscrip, error) {

	select {
	case c := <-c.ch:
		return c, nil
	// case s.err = <-s.ech:
	//	return nil, s.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}

}

func checkMasterAliveClient(cfg *Config) (bool, error) {

	initPath := "/Databus"
	zkConn, connErr := ZkConnection(cfg.ZkPath, initPath)
	if connErr != nil {
		return false, connErr
	}
	defer zkConn.Close()

	exist, err := zkConn.Exist(cfg.KeepAlivepath + "/" + cfg.Topic)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}

	value, _ := zkConn.Get(cfg.KeepAlivepath + "/" + cfg.Topic)
	keepnode, err := KeepAliveNodeDecode(value)
	if err != nil {
		return false, err
	}

	cfg.Serverip = keepnode.IP
	cfg.Serverport = keepnode.Port

	return true, nil

}

func parseJsonValueRows(body []byte) interface{} {
	var getdetail RowsEvent
	if json.Unmarshal(body, &getdetail) != nil {
		log.Debug("Wrong Value")
		return nil
	}
	return getdetail
}

func parseJsonValueQuery(body []byte) interface{} {
	var getdetail QueryEvent
	if json.Unmarshal(body, &getdetail) != nil {
		log.Debug("Wrong Value")
		return nil
	}
	return getdetail
}

func parseJsonKey(head []byte) interface{} {
	var pos KafkaKey
	if json.Unmarshal(head, &pos) != nil {
		log.Debug("Wrong Key")
		return nil
	}
	return pos
}

func SaveRpl(name string, rpl *ReplicateInfo) error {

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)

	e.Encode(rpl)

	err := ioutil2.WriteFileAtomic(name, buf.Bytes(), 0644)
	if err != nil {
		log.Errorf("Client save rplication info to file %s err %v", name, err)
	}
	return errors.Trace(err)
}

func GetOffset(cfg *Config) (int64, error) {
	// 从Kafka上面利用MasterName,MasterPos，获取Offset
	p, err := NewPartitionConsumer(cfg.Topic, cfg.Brokers, true)
	if err != nil {
		log.Error(err)
		return 0, errors.Trace(err)
	}

	log.Debug("Get NewPartitionConsumer!\n")

	defer p.master.Close()
	defer p.client.Close()

	p.Name = cfg.MasterName
	p.Pos = uint64(cfg.MasterPos)

	log.Debug("Into SerchAsc!\n")
	offsetpos, err := p.SearchAsc()
	if err != nil {
		return 0, err
	}

	return offsetpos, nil

}
