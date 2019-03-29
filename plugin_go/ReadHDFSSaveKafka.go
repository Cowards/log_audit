package main

import (
	"encoding/xml"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/colinmarc/hdfs"
	"io/ioutil"
	"strings"
	"sync"
	"time"
)

type Configuration struct {
	Property []Property `xml:"property"`
}

type Property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

var Address = []string{"172.17.0.217:9092", "172.17.0.218:9092", "172.17.0.219:9092"}

func main() {

	hdfsClient, _ := hdfs.New("cdh-m1.sxkj.online:8020")
	start := time.Now()
	fmt.Println("[INFO] HDFS to Kafka start Time : " + start.Format("20060102150405"))
	nTime := time.Now()
	yesTime := nTime.AddDate(0, 0, -1)
	ymdPath := yesTime.Format("2006/01/02")
	hdfsDirPath := fmt.Sprintf("/user/history/done/%s/", ymdPath)
	//fmt.Println(hdfsDirPath)
	//讀取 HDFS 的目錄
	hdfsFiles, _ := hdfsClient.ReadDir(hdfsDirPath)

	for _, hdfsDir := range hdfsFiles {
		hdfsFileLevel1, _ := hdfsClient.ReadDir(hdfsDirPath + "/" + hdfsDir.Name())
		var exeCount = 0
		var m sync.Mutex
		var wg sync.WaitGroup
		wg.Add(len(hdfsFileLevel1))
		for _, hdfsFile := range hdfsFileLevel1 {
			var hdfsnm string
			hdfsnm = hdfsFile.Name()
			fmt.Println(fmt.Sprintf("[HDFS] 正在处理目录： %s%s/", hdfsDirPath, hdfsDir.Name()))
			go func() {
				defer wg.Done()
				// 檔名為 part 開頭
				if strings.HasSuffix(hdfsnm, "_conf.xml") {
					hdfsFile := fmt.Sprintf("%s%s/%s", hdfsDirPath, hdfsDir.Name(), hdfsnm)
					//取得 HDFS 的 File
					//fmt.Println("hdfsFile " + hdfsFile)
					//fmt.Println("hdfsnm " + hdfsnm)
					//fmt.Println("hdfsDir " + hdfsDir.Name())
					file, _ := hdfsClient.Open(hdfsFile)
					defer file.Close()
					bytes, err := ioutil.ReadAll(file)
					if err != nil {
						fmt.Print(err)
					}
					m.Lock()
					exeCount++
					//fmt.Println(string(bytes))
					var a Configuration
					err1 := xml.Unmarshal(bytes, &a)
					if err1 !=nil{
						fmt.Println("ERROR")
					}
					for _, i := range a.Property {
						if i.Name == "mapreduce.workflow.name" {
							var hdfsnm_array = strings.Split(hdfsnm, "_")
							var value = fmt.Sprintf("application_%s_%s^%s", hdfsnm_array[1], hdfsnm_array[2], i.Value)
							value = strings.Replace(value, "\n", "", -1)
							value = fmt.Sprintf("%s\n",value)
							sendKafkaMsg(value, "log_audit_supply")
						}
					}
					m.Unlock()
				}
			}()
		}

		wg.Wait()
		end := time.Now()
		executeTime := end.Sub(start)
		fmt.Println("[INFO] HDFS to Kafka end Time : " + end.Format("20060102150405"))
		fmt.Printf("[INFO] HDFS to Kafka executeTime : %v , executeCount : %d ", executeTime, exeCount)
	}
}

func sendKafkaMsg(mdata string, Topic string) {

	config := sarama.NewConfig()
	//等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	//随机向partition发送消息
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//是否等待成功和失败后的响应,只有上面的RequireAcks设置不是NoReponse这里才有用.
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	//设置使用的kafka版本,如果低于V0_10_0_0版本,消息中的timestrap没有作用.需要消费和生产同时配置
	//注意，版本设置不对的话，kafka会返回很奇怪的错误，并且无法成功发送消息
	config.Version = sarama.V0_10_0_1

	producer, err := sarama.NewSyncProducer(Address, config)

	if err != nil {
		panic(err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: Topic,
	}
	msg.Value = sarama.ByteEncoder(mdata)

	fmt.Println(producer.SendMessage(msg))
}
