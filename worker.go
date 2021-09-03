package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

func managerMain(ctx context.Context) {
	logWrapper("START WORKER MANAGER")

	workers := make(map[string]time.Time)

	t := time.Tick(time.Second * 10)
	iterQueue := func() {
		var cursor uint64 = 0
		var next uint64 = 10000
		var allQueues []string
		var err error
		for next != 0 {
			allQueues, next, err = Rdb.Scan(ctx, cursor, "info:*", 1000).Result()

			// allQueues, err := Rdb.Keys(ctx, "inactive:*").Result()
			if err != nil {
				logWrapper("ERROR", err)
			}
			for _, name := range allQueues {
				queue := name[5:]
				// TODO: use channel to check worker status
				// _, exist := workers[queue]
				// if exist {
				// 	continue
				// }

				latest, _ := Rdb.HGet(ctx, "info:"+queue, "latest_worker_check_time").Result()
				if latest == "" {
					go work(ctx, queue)
					workers[queue] = time.Now()
				} else {
					// check worker timeout
					ts, _ := strconv.ParseInt(latest, 10, 64)
					t := time.Unix(ts, 0)
					now := time.Now()
					if now.Sub(t) > time.Minute {
						logWrapper("WORKER TIMEOUT", queue)
						go work(ctx, queue)
						workers[queue] = time.Now()
					}
				}
			}
		}
	}
	iterQueue()
	for {
		select {
		case <-ctx.Done():
			logWrapper("STOP WORKER MANAGER")
			return
		case <-t:
			iterQueue()
		}
	}
}

func work(ctx context.Context, queueName string) {
	logWrapper("START WORKER", queueName)

	activeKey := "active:" + queueName
	inactiveKey := "inactive:" + queueName
	infoKey := "info:" + queueName

	for {
		select {
		case <-ctx.Done():
			logWrapper("STOP WORKER", queueName)
			return
		default:
		}
		now := fmt.Sprint(time.Now().Unix())
		Rdb.HSet(ctx, infoKey, "latest_worker_check_time", now)
		msgs, err := Rdb.ZRangeByScore(ctx, inactiveKey, &redis.ZRangeBy{Min: "-inf", Max: now}).Result()
		if err != nil {
			logWrapper("ERROR", err)
		}
		if len(msgs) != 0 {
			for _, id := range msgs {
				exist, err := Rdb2.Exists(ctx, id).Result()
				if err != nil {
					logWrapper("ERROR", err)
				}
				if exist != 0 {
					logWrapper("ACTIVE MSG", id, queueName)
					_, err = Rdb.RPush(ctx, activeKey, id).Result()
					if err != nil {
						logWrapper("ERROR", err)
					} else {
						_, err = Rdb.ZRem(ctx, inactiveKey, id).Result()
						if err != nil {
							logWrapper("ERROR", err)
						}
					}
				} else {
					logWrapper("INVALID MSG", id, queueName)
					_, err = Rdb.ZRem(ctx, inactiveKey, id).Result()
					if err != nil {
						logWrapper("ERROR", err)
					}
				}
			}
		}
		// clean deleted msg id in active queue, to solve problem that there has NOT any consumer pull from the active queue
		for {
			msgID, err := Rdb.LIndex(ctx, activeKey, 0).Result()
			if err != nil {
				break
			}
			exists, err := Rdb2.Exists(ctx, msgID).Result()
			if err != nil {
				logWrapper("ERROR", err)
				break
			} else {
				if exists == 0 { // msg not exist
					Rdb.LRem(ctx, activeKey, 1, msgID).Result()
				} else { // since first msg exist, we may suppose other msgs also exist
					break
				}
			}
		}

		time.Sleep(time.Second)
	}
}
