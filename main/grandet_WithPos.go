package main

import (
	"context"
	"fmt"
	"grandet"
	"os"
	"os/signal"
	"time"

	"github.com/ngaut/log"
)

func main() {

	// 初始化配置信息
	var cfg grandet.Config

	cfg.Brokers = []string{"10.29.204.73:19092", "10.27.185.100:19092", "10.169.117.85:19092"}
	cfg.ZkPath = []string{"10.29.204.73:12181", "10.27.185.100:12181", "10.169.117.85:12181"}
	cfg.FirstLoad = false
	// 非初始化情况下，NodeName可以不配置
	cfg.NodeName = "DumpMeta"
	cfg.Topic = "databustest.test"
	// 非初始化情况下,Serverip,Serverport,Mirrorenable,KeepAlivepath都不需要配置
	cfg.Serverip = "10.169.117.85"
	cfg.Serverport = "888"
	cfg.LogLevel = "debug"
	cfg.LogDir = "/root/log"
	cfg.LogFile = "grandet_error.log"
	cfg.Mirrorenable = true
	cfg.KeepAlivepath = "KeepAliveNode"
	// 若不配置，默认为当前文件夹
	cfg.Replinfopath = "/root/data/Repl.info"
	cfg.MasterName = "binlog.000145"
	cfg.MasterPos = 2165057

	// 新建Client
	log.Info("Get New Client Start!")
	Cl, err := grandet.NewClient(&cfg)
	if err != nil {
		panic(err)
	}

	log.Info("ClientStart !")
	// Client启动同步
	Cl.ClientStart()

	// 设置退出消息监控
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	log.Info("Process Event !")
	// 循环处理返回数据
	go func() {
		var timeout time.Duration
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			ev, err := Cl.GetEvent(ctx)
			cancel()

			// 处理非正常返回
			if err == context.DeadlineExceeded {
				timeout = 2 * timeout
				continue
			}
			if err != nil {
				panic(err)
			}

			timeout = time.Second

			//处理正常返回

			// 获取Binlog信息
			fmt.Printf("Binlog File is %s,Pos is %d,Action is %s \n", ev.Header.Pos.Name, ev.Header.Pos.Pos, ev.Header.Action)

			//获取EVENT数据
			switch e := ev.Event.(type) {
			case grandet.RowsEvent:
				// 显示相关ROWS信息
				fmt.Printf("Table is %s.%s,Action is %s \n", e.Table.Schema, e.Table.Name, e.Action)
				cmd := grandet.Do(&e)
				log.Info(cmd)
			case grandet.QueryEvent:
				// 显示相关DDL信息
				log.Info(string(e.Query))
			default:
				fmt.Print("Can not Match DDL or DML ! \n")
				continue
			}
		}
	}()

	<-signals
	log.Error("Interrupt is detected,Close Grandet Prog ")

}
