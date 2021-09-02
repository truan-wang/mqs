package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

var Rdb *redis.Client
var Rdb2 *redis.Client
var AUTH_TOKEN string = os.Getenv("AUTH_TOKEN")
var infoCache *Cache = NewCache(time.Minute)

const MAX_GET_MESSAGE_TIMEOUT time.Duration = 30 * time.Second
const DEFAULT_MAX_TTL time.Duration = time.Hour * 24 * 15
const DEFAULT_MAX_PROCESS_SECONDS time.Duration = time.Minute
const DEFAULT_DELAY_SECONDS time.Duration = 0
const DEFAULT_ALERT_ACTIVE_MSG_IN_MINUTE int = 0 // no alert

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

func logWrapper(a ...interface{}) {
	fmt.Print(time.Now().Format("2006.01.02-15:04:05 "))
	fmt.Println(a...)
}

func login(c *fiber.Ctx) error {
	token := c.Query("auth-token")
	if token == "" {
		token = c.Get("auth-token")
	}
	if token != AUTH_TOKEN {
		return c.SendStatus(403)
	}
	return c.Next()
}

func getQueueName(c *fiber.Ctx) (string, error) {
	name := c.Params("queueName")
	return name, nil
}

func createQueueMaxTTL(key interface{}) interface{} {
	name := key.(string)
	ttl, err := Rdb.HGet(context.Background(), "info:"+name, "max_ttl").Result()
	if err == nil && ttl != "" {
		ttlInt, err := strconv.Atoi(ttl)
		if err == nil && ttlInt != 0 {
			return time.Second * time.Duration(ttlInt)
		}
	}
	return DEFAULT_MAX_TTL
}

func getQueueMaxTTL(c context.Context, name string) time.Duration {
	d, err := infoCache.Get(name+":max_ttl", createQueueMaxTTL, time.Minute*10)
	if err != nil {
		logWrapper("ERROR", err)
		return DEFAULT_MAX_TTL
	}
	t, ok := d.(time.Duration)
	if ok {
		return t
	}
	return DEFAULT_MAX_TTL
}

func createQueueMaxProcessTime(key interface{}) interface{} {
	name := key.(string)
	seconds, err := Rdb.HGet(context.Background(), "info:"+name, "max_process_seconds").Result()
	if err == nil && seconds != "" {
		secondsInt, err := strconv.Atoi(seconds)
		if err == nil && secondsInt != 0 {
			return time.Second * time.Duration(secondsInt)
		}
	}
	return DEFAULT_MAX_PROCESS_SECONDS
}

func getQueueMaxProcessTime(c context.Context, name string) time.Duration {
	d, err := infoCache.Get(name+":max_process_seconds", createQueueMaxProcessTime, time.Minute*10)
	if err != nil {
		logWrapper("ERROR", err)
		return DEFAULT_MAX_PROCESS_SECONDS
	}
	t, ok := d.(time.Duration)
	if ok {
		return t
	}
	return DEFAULT_MAX_PROCESS_SECONDS
}

func createQueueDefaultDelaySeconds(key interface{}) interface{} {
	name := key.(string)
	seconds, err := Rdb.HGet(context.Background(), "info:"+name, "delay_seconds").Result()
	if err == nil && seconds != "" {
		secondsInt, err := strconv.Atoi(seconds)
		if err == nil && secondsInt != 0 {
			return secondsInt
		}
	}
	return int(DEFAULT_DELAY_SECONDS)
}

func getQueueDefaultDelaySeconds(c context.Context, name string) int {
	d, err := infoCache.Get(name+":delay_seconds", createQueueDefaultDelaySeconds, time.Minute*10)
	if err != nil {
		logWrapper("ERROR", err)
		return int(DEFAULT_DELAY_SECONDS)
	}
	t, ok := d.(int)
	if ok {
		return t
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
	logWrapper("CREATE MSG", msgID)
	if err != nil {
		return err
	}
	Rdb.HIncrBy(c.Context(), "info:"+name, "created_messages_count", 1).Result()
	if delaySeconds == 0 {
		Rdb.RPush(c.Context(), "active:"+name, msgID)
		logWrapper("ACTIVE MSG", msgID)
	} else {
		activeAt := time.Now().Add(time.Second * time.Duration(delaySeconds))
		z := redis.Z{
			Score:  float64(activeAt.Unix()),
			Member: msgID,
		}
		_, err := Rdb.ZAdd(c.Context(), "inactive:"+name, &z).Result()
		logWrapper("DELAY  MSG", msgID, "IN", delaySeconds, "SECONDS")
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
	startAt := time.Now()
	for timeout := time.Duration(0); timeout < MAX_GET_MESSAGE_TIMEOUT; {
		msgs, _ := Rdb.BLPop(c.Context(), MAX_GET_MESSAGE_TIMEOUT-timeout, queueName).Result()
		timeout = time.Since(startAt)
		if len(msgs) > 1 && msgs[0] == queueName {
			msgID := msgs[1]
			logWrapper("GET    MSG", msgID)
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
					logWrapper("ERROR", err)
				}
				c.Response().Header.Add("mqs-msgid", msgID)
				c.WriteString(msg)
				Rdb.HIncrBy(c.Context(), "info:"+name, "get_messages_count", 1).Result()
				break
			} else {
				logWrapper("INVALID MSG", msgID)
			}
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
	logWrapper("DELAY  MSG", msgID, "IN", delaySeconds, "SECONDS")

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
	logWrapper("DELETE MSG", msgID)
	Rdb.ZRem(c.Context(), "inactive:"+name, msgID).Result()
	Rdb.HIncrBy(c.Context(), "info:"+name, "consumed_messages_count", 1).Result()
	count, err := Rdb2.Del(c.Context(), msgID).Result()
	if err != nil {
		return err
	}
	if count > 0 {
		logWrapper("DELETD MSG", msgID)
		c.Status(200)
	} else {
		c.Status(404)
	}
	return nil
}

func getQueueInfo(name string, c *fiber.Ctx) map[string]string {
	info, err := Rdb.HGetAll(c.Context(), "info:"+name).Result()
	if err != nil {
		logWrapper("ERROR", err)
	}
	activeLen, err := Rdb.LLen(c.Context(), "active:"+name).Result()
	if err != nil {
		logWrapper("ERROR", err)
	}
	info["active_messages_count"] = strconv.Itoa(int(activeLen))
	inactiveLen, err := Rdb.ZCount(c.Context(), "inactive:"+name, "-inf", "+inf").Result()
	if err != nil {
		logWrapper("ERROR", err)
	}
	info["inactive_messages_count"] = strconv.Itoa(int(inactiveLen))
	_, ok := info["max_ttl"]
	if !ok {
		info["max_ttl"] = fmt.Sprintf("%.0f", DEFAULT_MAX_TTL.Seconds())
	}
	_, ok = info["max_process_seconds"]
	if !ok {
		info["max_process_seconds"] = fmt.Sprintf("%.0f", DEFAULT_MAX_PROCESS_SECONDS.Seconds())
	}

	_, ok = info["delay_seconds"]
	if !ok {
		info["delay_seconds"] = fmt.Sprintf("%.0f", DEFAULT_DELAY_SECONDS.Seconds())
	}
	ts := info["latest_worker_check_time"]
	if ts != "" {
		tsInt64, _ := strconv.ParseInt(ts, 10, 64)
		t := time.Unix(tsInt64, 0)
		info["latest_worker_check_time"] = t.Local().Format("2006.01.02-15:04:05")
	}

	_, ok = info["alert_active_msg_count"]
	if !ok {
		info["alert_active_msg_count"] = fmt.Sprintf("%d", DEFAULT_ALERT_ACTIVE_MSG_IN_MINUTE)
	}
	return info
}

func getMessageInfo(c *fiber.Ctx) error {
	name, err := getQueueName(c)
	if err != nil {
		return err
	}
	if name == "" {
		return fiber.NewError(400, "queue name invalid")
	}

	info := getQueueInfo(name, c)
	c.Status(200).JSON(info)
	return nil
}

func modifyMessageInfo(c *fiber.Ctx) error {
	name, err := getQueueName(c)
	if err != nil {
		return err
	}
	if name == "" {
		return fiber.NewError(400, "queue name invalid")
	}
	infoKey := "info:" + name
	delaySeconds := c.Query("delay-seconds")
	if delaySeconds != "" {
		_, err := strconv.ParseInt(delaySeconds, 10, 64)
		if err == nil {
			Rdb.HSet(c.Context(), infoKey, "delay_seconds", delaySeconds).Result()
		}
	}
	maxProcessSeconds := c.Query("max-process-seconds")
	if maxProcessSeconds != "" {
		_, err := strconv.ParseInt(maxProcessSeconds, 10, 64)
		if err == nil {
			Rdb.HSet(c.Context(), infoKey, "max_process_seconds", maxProcessSeconds).Result()
		}
	}
	maxTTL := c.Query("max-ttl")
	if maxTTL != "" {
		_, err := strconv.ParseInt(maxTTL, 10, 64)
		if err == nil {
			Rdb.HSet(c.Context(), infoKey, "max_ttl", maxTTL).Result()
		}
	}

	alertActiveMsg := c.Query("alert-active-msg-count")
	if alertActiveMsg != "" {
		_, err := strconv.ParseInt(alertActiveMsg, 10, 32)
		if err == nil {
			Rdb.HSet(c.Context(), infoKey, "alert_active_msg_count", alertActiveMsg).Result()
		}
	}

	info := getQueueInfo(name, c)

	c.Status(200).JSON(info)

	return nil
}

func summary(c *fiber.Ctx) error {
	result := make([]map[string]string, 0, 100)
	var cursor uint64 = 0
	var next uint64 = 10000
	var allQueues []string
	var err error
	for next != 0 {
		allQueues, next, err = Rdb.Scan(c.Context(), cursor, "info:*", 1000).Result()

		if err != nil {
			logWrapper("ERROR", err)
		}
		for _, name := range allQueues {
			queue := name[5:]
			info := getQueueInfo(queue, c)
			info["name"] = queue
			result = append(result, info)
		}
	}

	c.Status(200).JSON(result)
	return nil
}

func workers_main() {
	ctx := context.Background()
	ctx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		fmt.Println("Gracefully shutting down workers...")
		cancelWorkers()
	}()

	managerMain(ctx)
}

func main() {
	if os.Getenv("QMS_WORKER_ONLY") != "" {
		workers_main()
		return
	}
	if os.Getenv("QMS_API_ONLY") == "" {
		go workers_main()
	}

	app := fiber.New()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		fmt.Println("Gracefully shutting down fiber...")
		_ = app.Shutdown()
	}()

	app.Use(login)

	app.Get("/api/mqs/:queueName/info", getMessageInfo)
	app.Put("/api/mqs/:queueName/info", modifyMessageInfo)
	app.Put("/api/mqs/:queueName/:msgID", modifyMessage)
	app.Delete("/api/mqs/:queueName/:msgID", deleteMessage)
	app.Get("/api/mqs/:queueName", getMessage)
	app.Post("/api/mqs/:queueName", sendMessage)
	app.Get("/api/mqs/", summary)

	app.Listen(":3000")
}
