package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

var Rdb *redis.Client
var Rdb2 *redis.Client

func init() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	pwd := os.Getenv("REDIS_PWD")
	if pwd == "" {
		pwd = ""
	}
	db := os.Getenv("REDIS_DB")
	if db == "" {
		db = "10"
	}
	dbInt, _ := strconv.Atoi(db)

	Rdb = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
		DB:       dbInt,
	})
	db2 := os.Getenv("REDIS_DB2")
	if db2 == "" {
		db2 = "11"
	}
	db2Int, _ := strconv.Atoi(db2)

	Rdb2 = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
		DB:       db2Int,
	})
}

func log(a ...interface{}) {
	fmt.Print(time.Now().Format("2006.01.02-15:04:05 "))
	fmt.Println(a...)
}

func getQueueListName(c *fiber.Ctx) (string, error) {
	name := c.Params("queueName")
	// TODO: valid queue name
	return name, nil
}

func getQueueDefaultDelaySeconds(name string) int {
	// TODO: get queue setting from redis
	return 0
}

func sendMessage(c *fiber.Ctx) error {
	name, err := getQueueListName(c)
	if err != nil {
		return err
	}
	if name == "" {
		return fiber.NewError(400, "queue name invalid")
	}
	delaySecondsStr := c.Query("delay-seconds")
	delaySeconds := 0
	if delaySecondsStr == "" {
		delaySeconds = getQueueDefaultDelaySeconds(name)
	} else {
		delaySeconds, _ = strconv.Atoi(delaySecondsStr)
	}
	msg := c.Body()
	msgID := uuid.New().String()
	_, err = Rdb2.Set(c.Context(), msgID, msg, time.Hour*24*15).Result()
	log("CREATE MSG", msgID)
	if err != nil {
		return err
	}
	if delaySeconds == 0 {
		Rdb.LPush(c.Context(), "active:"+name, msgID)
		log("ACTIVE MSG", msgID)
	} else {
		activeAt := time.Now().Add(time.Second * time.Duration(delaySeconds))
		z := redis.Z{
			Score:  float64(activeAt.Unix()),
			Member: msgID,
		}
		_, err := Rdb.ZAdd(c.Context(), "inactive:"+name, &z).Result()
		log("DELAY  MSG", msgID, "IN", delaySeconds, "SECONDS")
		if err != nil {
			return err
		}
	}
	c.WriteString(msgID)
	c.Status(200)
	return nil
}

func getMessage(c *fiber.Ctx) error {
	name, err := getQueueListName(c)
	if err != nil {
		return err
	}
	if name == "" {
		return fiber.NewError(400, "queue name invalid")
	}
	queueName := "active:" + name
	msgs, _ := Rdb.BLPop(c.Context(), time.Second*10, queueName).Result()
	if len(msgs) > 1 && msgs[0] == queueName {
		msgID := msgs[1]
		log("GET    MSG", msgID)
		msg, err := Rdb2.Get(c.Context(), msgID).Result()
		// if msg has been deleted, return null
		if err == nil {
			c.Response().Header.Add("MQS-MsgID", msgID)
			c.WriteString(msg)
		} else {
			log("INVALID MSG", msgID)
		}
	}
	c.Status(200)
	return nil
}

func modifyMessage(c *fiber.Ctx) error {
	return nil
}

func deleteMessage(c *fiber.Ctx) error {
	// name := c.Params("queueName")
	// if name == "" {
	// 	return fiber.NewError(400, "queue name required")
	// }
	msgID := c.Params("msgID")
	if msgID == "" {
		return fiber.NewError(400, "message ID required")
	}
	log("DELETE MSG", msgID)
	count, err := Rdb2.Del(c.Context(), msgID).Result()
	if err != nil {
		return err
	}
	if count > 0 {
		log("DELETD MSG", msgID)
		c.Status(200)
	} else {
		c.Status(404)
	}
	return nil
}

func getMessageInfo(c *fiber.Ctx) error {
	return nil
}

func modifyMessageInfo(c *fiber.Ctx) error {
	return nil
}

func summary(c *fiber.Ctx) error {
	return nil
}

func main() {
	c := context.Background()
	c, cancel := context.WithCancel(c)

	go manager(c)

	app := fiber.New()

	app.Get("/api/mqs/:queueName/info", getMessageInfo)
	app.Put("/api/mqs/:queueName/info", modifyMessageInfo)
	app.Get("/api/mqs/:queueName", getMessage)
	app.Put("/api/mqs/:queueName", modifyMessage)
	app.Post("/api/mqs/:queueName", sendMessage)
	app.Delete("/api/mqs/:queueName/:msgID", deleteMessage)
	app.Get("/api/mqs/summary", summary)

	app.Listen(":3000")
	cancel()
}
