package grandet

import (
	"bufio"
	"io"
	"net"

	"github.com/ngaut/log"
)

func (c *Client) NewFileCapture(ip string, port string, filename string) {
	address := ip + ":" + port
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Errorf("GRANDET: Dial Server %s Wrong\n", address)
		panic("GRANDET: Bye .. Bye ..")
	}

	//fmt.Fprintf(conn, "GET / HTTP/1.0\r\n\r\n")

	filenamen := filename + "\n"
	_, err = conn.Write([]byte(filenamen))

	connReader := bufio.NewReader(conn)

	c.getFile(connReader)
}

func (c *Client) getFile(connReader *bufio.Reader) {

	line, err := connReader.ReadBytes('\n')
	if err != nil {
		log.Error("GRANDET: READ Conn ERROR \n")
		panic("GRANDET: Bye .. Bye ..")
	}

	if string(line) != "PONG\n" {
		log.Error("GRANDET: Do NOT GET PONG,Bye .. \n")
		panic("GRANDET: Bye .. Bye ..")
	}

	log.Infof("Start Get DumpFile From Net !!! \n")
	for {
		line, err := connReader.ReadBytes('\n')
		if err == io.EOF {
			log.Info("GRANDET: Read Conn File OVER \n")
			return
		} else if err != nil {
			log.Error("GRANDET: Read Conn Error \n")
			panic("GRANDET: Bye .. Bye ..")
		}

		context := line[0 : len(line)-1]
		if mydata := parseJsonValueRows(context); mydata != nil {
			value, ok := mydata.(RowsEvent)
			if !ok {
				log.Warning("type assertion is wrong")
				continue
			}
			valueDecode := DeBase64RowsEvent(value)

			/*
				fmt.Print("Start Value\n")
				fmt.Print(value, "\n")
				fmt.Print("Start valueDecode \n")
				fmt.Print(valueDecode, "\n")
			*/
			kk := &KafkaKey{Action: "Dump"}
			sp := &EventDiscrip{Header: kk, Event: valueDecode}
			c.ch <- sp
			log.Debugf("Received messages Action %s,Schema %s,Table_Name %s ", value.Action, value.Table.Schema, value.Table.Name)
		}
	}

	log.Infof("End Get DumpFile From Net !!! \n")

}
