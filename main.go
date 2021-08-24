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
var Token string

const DEFAULT_MAX_TTL time.Duration = time.Hour * 24 * 15
const DEFAULT_MAX_PROCESS_SECONDS time.Duration = time.Minute
const DEFAULT_DELAY_SECONDS time.Duration = 0

func init() {
	Token = os.Getenv("AUTH_TOKEN")
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
		Addr:         addr,
		Password:     pwd,
		DB:           dbInt,
		PoolSize:     4096,
		MinIdleConns: 10,
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

func login(c *fiber.Ctx) error {
	token := c.Get("Auth-Token")
	if token != Token {
		return c.SendStatus(403)
	}
	return c.Next()
}

func getQueueName(c *fiber.Ctx) (string, error) {
	name := c.Params("queueName")
	// TODO: valid queue name
	return name, nil
}

func getQueueMaxTTL(c context.Context, name string) time.Duration {
	ttl, err := Rdb.HGet(c, "info:"+name, "max_ttl").Result()
	if err != nil && ttl != "" {
		ttlInt, err := strconv.Atoi(ttl)
		if err == nil && ttlInt != 0 {
			return time.Second * time.Duration(ttlInt)
		}
	}
	return DEFAULT_MAX_TTL
}
func getQueueMaxProcessTime(c context.Context, name string) time.Duration {
	seconds, err := Rdb.HGet(c, "info:"+name, "max_process_seconds").Result()
	if err != nil && seconds != "" {
		secondsInt, err := strconv.Atoi(seconds)
		if err == nil && secondsInt != 0 {
			return time.Second * time.Duration(secondsInt)
		}
	}
	return DEFAULT_MAX_PROCESS_SECONDS
}

func getQueueDefaultDelaySeconds(c context.Context, name string) int {
	seconds, err := Rdb.HGet(c, "info:"+name, "max_process_seconds").Result()
	if err != nil && seconds != "" {
		secondsInt, err := strconv.Atoi(seconds)
		if err == nil && secondsInt != 0 {
			return secondsInt
		}
	}
	return int(DEFAULT_DELAY_SECONDS)
}

func sendMessage(c *fiber.Ctx) error {
	name, err := getQueueName(c)
	if err != nil {
		return err
	}
	if name == "" {
		return fiber.NewError(400, "queue name invalid")
	}
	delaySecondsStr := c.Query("delay-seconds")
	delaySeconds := 0
	if delaySecondsStr == "" {
		delaySeconds = getQueueDefaultDelaySeconds(c.Context(), name)
	} else {
		delaySeconds, _ = strconv.Atoi(delaySecondsStr)
	}
	msg := c.Body()
	msgID := uuid.New().String()
	_, err = Rdb2.Set(c.Context(), msgID, msg, getQueueMaxTTL(c.Context(), name)).Result()
	log("CREATE MSG", msgID)
	if err != nil {
		return err
	}
	Rdb.HIncrBy(c.Context(), "info:"+name, "created_messages_count", 1).Result()
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
	name, err := getQueueName(c)
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
		if err == nil && msg != "" {
			activeAt := time.Now().Add(getQueueMaxProcessTime(c.Context(), name))
			z := redis.Z{
				Score:  float64(activeAt.Unix()),
				Member: msgID,
			}
			_, err := Rdb.ZAdd(c.Context(), "inactive:"+name, &z).Result()
			if err != nil {
				log("ERROR", err)
			}
			c.Response().Header.Add("mqs-msgid", msgID)
			c.WriteString(msg)
			Rdb.HIncrBy(c.Context(), "info:"+name, "get_messages_count", 1).Result()
		} else {
			log("INVALID MSG", msgID)
		}
	}
	c.Status(200)
	return nil
}

func modifyMessage(c *fiber.Ctx) error {
	name, err := getQueueName(c)
	if err != nil {
		return err
	}
	if name == "" {
		return fiber.NewError(400, "queue name invalid")
	}
	msgID := c.Params("msgID")
	if msgID == "" {
		return fiber.NewError(400, "message ID required")
	}
	delaySecondsStr := c.Query("delay-seconds")
	delaySeconds := 0
	if delaySecondsStr == "" {
		return fiber.NewError(400, "delay-seconds required")
	} else {
		delaySeconds, _ = strconv.Atoi(delaySecondsStr)
	}
	if delaySeconds < 0 {
		return fiber.NewError(400, "delay-seconds should not less than 0")
	}

	activeAt := time.Now().Add(time.Second * time.Duration(delaySeconds))
	z := redis.Z{
		Score:  float64(activeAt.Unix()),
		Member: msgID,
	}
	Rdb.ZAdd(c.Context(), "inactive:"+name, &z).Result()
	log("DELAY  MSG", msgID, "IN", delaySeconds, "SECONDS")

	return nil
}

func deleteMessage(c *fiber.Ctx) error {
	name := c.Params("queueName")
	if name == "" {
		return fiber.NewError(400, "queue name required")
	}
	msgID := c.Params("msgID")
	if msgID == "" {
		return fiber.NewError(400, "message ID required")
	}
	log("DELETE MSG", msgID)
	Rdb.ZRem(c.Context(), "inactive:"+name, msgID).Result()
	Rdb.HIncrBy(c.Context(), "info:"+name, "consumed_messages_count", 1).Result()
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
	name, err := getQueueName(c)
	if err != nil {
		return err
	}
	if name == "" {
		return fiber.NewError(400, "queue name invalid")
	}
	info, err := Rdb.HGetAll(c.Context(), "info:"+name).Result()
	if err != nil {
		log("ERROR", err)
	}
	activeLen, err := Rdb.LLen(c.Context(), "active:"+name).Result()
	if err != nil {
		log("ERROR", err)
	}
	info["active_messages_count"] = strconv.Itoa(int(activeLen))
	inactiveLen, err := Rdb.ZCount(c.Context(), "inactive:"+name, "-inf", "+inf").Result()
	if err != nil {
		log("ERROR", err)
	}
	info["inactive_messages_count"] = strconv.Itoa(int(inactiveLen))
	_, ok := info["max_ttl"]
	if !ok {
		info["max_ttl"] = DEFAULT_MAX_TTL.String()
	}
	_, ok = info["max_process_seconds"]
	if !ok {
		info["max_process_seconds"] = DEFAULT_MAX_PROCESS_SECONDS.String()
	}

	_, ok = info["delay_seconds"]
	if !ok {
		info["delay_seconds"] = DEFAULT_DELAY_SECONDS.String()
	}
	ts := info["latest_worker_check_time"]
	if ts != "" {
		tsInt64, _ := strconv.ParseInt(ts, 10, 64)
		t := time.Unix(tsInt64, 0)
		info["latest_worker_check_time"] = t.Local().Format("2006.01.02-15:04:05")
	}

	c.Status(200).JSON(info)
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
	c, cancelWorkers := context.WithCancel(c)

	go manager(c)

	app := fiber.New()

	app.Use(login)

	app.Get("/api/mqs/:queueName/info", getMessageInfo)
	app.Put("/api/mqs/:queueName/info", modifyMessageInfo)
	app.Get("/api/mqs/:queueName", getMessage)
	app.Post("/api/mqs/:queueName", sendMessage)
	app.Put("/api/mqs/:queueName/:msgID", modifyMessage)
	app.Delete("/api/mqs/:queueName/:msgID", deleteMessage)
	app.Get("/api/mqs/summary", summary)

	app.Listen(":3000")
	cancelWorkers()
}
