package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

var ALERT_PREFIX string = ""
var DING_TALK_TOKEN string = "REPLACE THIS WITH YOUR TOKEN"
var DING_TALK_API string = "https://oapi.dingtalk.com/robot/send?access_token=REPLACE THIS WITH YOUR ACCESS TOKEN"

func init() {
	ALERT_PREFIX = os.Getenv("ALERT_PREFIX")
	token := os.Getenv("DING_TALK_TOKEN")
	if token != "" {
		DING_TALK_TOKEN = token
	}
	DING_TALK_API = fmt.Sprint("https://oapi.dingtalk.com/robot/send?access_token=", DING_TALK_TOKEN)
}

func formatMinutes(minute int) string {
	h := minute / 60
	m := minute % 60
	d := h / 24
	h = h % 24
	if d > 0 {
		return fmt.Sprintf("%d天%d小时%d分钟", d, h, m)
	} else if h > 0 {
		return fmt.Sprintf("%d小时%d分钟", h, m)
	}
	return fmt.Sprintf("%d分钟", m)
}

func sendAlertMessage(msg string) {
	if ALERT_PREFIX != "" {
		msg = fmt.Sprintf("【%s】%s", ALERT_PREFIX, msg)
	}
	values := map[string]interface{}{"msgtype": "text", "text": map[string]string{
		"content": msg,
	}}

	json_data, err := json.Marshal(values)

	if err != nil {
		logWrapper("ERROR", err)
	}

	resp, err := http.Post(DING_TALK_API, "application/json",
		bytes.NewBuffer(json_data))

	if err != nil {
		logWrapper("ERROR", err)
		return
	}
	defer resp.Body.Close()

	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)

	logWrapper("SEND Alert Message Results:", res)
}

func monitor(ctx context.Context) {
	logWrapper("START MONITOR")
	alertingStatus := make(map[string]time.Time)
	emptyTime := time.Time{}
	activeMsgCounts := make(map[string]int64)

	t := time.Tick(time.Minute)
	iterQueue := func() {
		var cursor uint64 = 0
		var next uint64 = 10000
		var allQueues []string
		var err error
		for next != 0 {
			allQueues, next, err = Rdb.Scan(ctx, cursor, "info:*", 1000).Result()

			if err != nil {
				logWrapper("ERROR", err)
			}
			for _, name := range allQueues {
				queue := name[5:]

				alertCountStr, _ := Rdb.HGet(ctx, "info:"+queue, "alert_active_msg_count").Result()
				var alertCount int64 = 0
				if alertCountStr != "" {
					// check worker alert setting
					alertCount, _ = strconv.ParseInt(alertCountStr, 10, 64)
				} else {
					alertCount = DEFAULT_ALERT_ACTIVE_MSG_IN_MINUTE
				}

				if alertCount > 0 {
					// check active message count is greate than count
					// logWrapper(queue, "Alert Message Setting", alertCount)
					activeLen, err := Rdb.LLen(ctx, "active:"+queue).Result()
					if err != nil {
						logWrapper("ERROR", err)
					}
					previousActiveLen := activeMsgCounts[queue]
					activeMsgCounts[queue] = activeLen
					if activeLen > alertCount {
						logWrapper(queue, "Active Message Count", activeLen)
						if previousActiveLen > alertCount {
							if alertingStatus[queue] == emptyTime {
								alertingStatus[queue] = time.Now()
								logWrapper("ALERT TRIGGERED", queue, "Active Message Count", activeLen)
								sendAlertMessage(fmt.Sprintf("MQS 报警触发【%s】活跃消息个数【%d】", queue, activeLen))
							} else {
								diffInMinute := int(math.Round(time.Since(alertingStatus[queue]).Minutes()))
								if diffInMinute == 10 || diffInMinute == 60 || (diffInMinute >= 24*60 && diffInMinute%(24*60) == 0) { // 十分钟后，一个小时后各报警一次，之后每天报警一次
									sendAlertMessage(fmt.Sprintf("MQS 报警持续【%s】【%s】活跃消息个数【%d】", formatMinutes(diffInMinute), queue, activeLen))
								}
							}
						}
					} else {
						if alertingStatus[queue] != emptyTime {
							diffInMinute := int(math.Round(time.Since(alertingStatus[queue]).Minutes()))
							alertingStatus[queue] = emptyTime
							logWrapper("ALERT CANCELLED", queue, "Active Message Count", activeLen)
							sendAlertMessage(fmt.Sprintf("MQS 报警解除【%s】持续时间【%s】目前活跃消息个数【%d】", queue, formatMinutes(diffInMinute), activeLen))
						}
					}
				}
			}
		}
	}
	iterQueue()
	for {
		select {
		case <-ctx.Done():
			logWrapper("STOP MONITOR")
			return
		case <-t:
			iterQueue()
		}
	}
}
