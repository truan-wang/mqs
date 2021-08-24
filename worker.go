package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

func manager(c context.Context) {
	log("START WORKER MANAGER")
	workers := make(map[string]time.Time)

	t := time.Tick(time.Second * 10)
	iterQueue := func() {
		var cursor uint64 = 0
		var next uint64 = 10000
		var allQueues []string
		var err error
		for next != 0 {
			allQueues, next, err = Rdb.Scan(c, cursor, "info:*", 1000).Result()

			// allQueues, err := Rdb.Keys(c, "inactive:*").Result()
			if err != nil {
				log("ERROR", err)
			}
			for _, name := range allQueues {
				queue := name[5:]
				// TODO: use channel to check worker status
				// _, exist := workers[queue]
				// if exist {
				// 	continue
				// }

				latest, _ := Rdb.HGet(c, "info:"+queue, "latest_worker_check_time").Result()
				if latest == "" {
					go work(c, queue)
					workers[queue] = time.Now()
				} else {
					// check worker timeout
					ts, _ := strconv.ParseInt(latest, 10, 64)
					t := time.Unix(ts, 0)
					now := time.Now()
					if now.Sub(t) > time.Minute {
						log("WORKER TIMEOUT", queue)
						go work(c, queue)
						workers[queue] = time.Now()
					}
				}
			}
		}
	}
	iterQueue()
	for {
		select {
		case <-c.Done():
			log("STOP WORKER MANAGER")
			return
		case <-t:
			iterQueue()
		}
	}
}

func work(c context.Context, queueName string) {
	log("START WORKER", queueName)

	activeKey := "active:" + queueName
	inactiveKey := "inactive:" + queueName
	infoKey := "info:" + queueName

	for {
		select {
		case <-c.Done():
			log("STOP WORKER", queueName)
			return
		default:
		}
		now := fmt.Sprint(time.Now().Unix())
		Rdb.HSet(c, infoKey, "latest_worker_check_time", now)
		msgs, err := Rdb.ZRangeByScore(c, inactiveKey, &redis.ZRangeBy{Min: "-inf", Max: now}).Result()
		if err != nil {
			log("ERROR", err)
		}
		if len(msgs) != 0 {
			for _, id := range msgs {
				exist, err := Rdb2.Exists(c, id).Result()
				if err != nil {
					log("ERROR", err)
				}
				if exist != 0 {
					log("ACTIVE MSG", id)
					_, err = Rdb.RPush(c, activeKey, id).Result()
					if err != nil {
						log("ERROR", err)
					} else {
						_, err = Rdb.ZRem(c, inactiveKey, id).Result()
						if err != nil {
							log("ERROR", err)
						}
					}
				} else {
					log("INVALID MSG", id)
					_, err = Rdb.ZRem(c, inactiveKey, id).Result()
					if err != nil {
						log("ERROR", err)
					}
				}
			}
		} else {
			time.Sleep(time.Second)
		}
	}
}
