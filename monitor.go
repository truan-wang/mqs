package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

const DING_TALK_API string = "https://oapi.dingtalk.com/robot/send?access_token=7b62a9fb7782c7caa3541a35fe66f5f028cf9d4da129a83a119c851602b3643e"

func sendAlertMessage(msg string) {

	values := map[string]interface{}{"msgtype": "text", "text": map[string]string{
		"content": msg,
	}}

	json_data, err := json.Marshal(values)

	if err != nil {
		logWrapper(err)
	}

	resp, err := http.Post(DING_TALK_API, "application/json",
		bytes.NewBuffer(json_data))

	if err != nil {
		logWrapper(err)
		return
	}
	defer resp.Body.Close()

	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)

	logWrapper(res)
}

func monitor(ctx context.Context) {
	logWrapper("START MONITOR")
	alertingStatus := make(map[string]bool)
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
					logWrapper(queue, "Alert Message Setting", alertCount)
					activeLen, err := Rdb.LLen(ctx, "active:"+queue).Result()
					if err != nil {
						logWrapper("ERROR", err)
					}
					previousActiveLen := activeMsgCounts[queue]
					activeMsgCounts[queue] = activeLen
					if activeLen > alertCount {
						logWrapper(queue, "Active Message Count", activeLen)
						if !alertingStatus[queue] && previousActiveLen > alertCount {
							alertingStatus[queue] = true
							logWrapper("ALERT TRIGGERED", queue, "Active Message Count", activeLen)
							sendAlertMessage(fmt.Sprintf("MQS 报警触发【%s】活跃消息个数【%d】", queue, activeLen))
						}
					} else {
						if alertingStatus[queue] && previousActiveLen > alertCount {
							alertingStatus[queue] = false
							logWrapper("ALERT CANCELLED", queue, "Active Message Count", activeLen)
							sendAlertMessage(fmt.Sprintf("MQS 报警撤销【%s】活跃消息个数【%d】", queue, activeLen))
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
