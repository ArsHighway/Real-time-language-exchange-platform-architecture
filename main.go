package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	ID          int
	Name        string
	Language    string
	Learning    []string
	sendMessage chan string
	conn        *websocket.Conn
}

type Hub struct {
	Clients    map[int]*Client
	MatchChan  chan MatchResult
	Register   chan *Client
	Unregister chan *Client
}

type MatchResult struct {
	RoomID string
	User1  int
	User2  int
}

type MatchWorker struct {
	client  *redis.Client
	sendRes chan MatchResult
}

func GenerateRoomID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case v, ok := <-h.MatchChan:
			if !ok {
				fmt.Println("Нечего слушать")
				return
			}
			if c1, ok := h.Clients[v.User1]; ok {
				c1.conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Найдена комната %s", v.RoomID)))
			}
			if c2, ok := h.Clients[v.User2]; ok {
				c2.conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Найдена комната %s", v.RoomID)))
			}
		case v, ok := <-h.Register:
			if !ok {
				fmt.Println("Канал закрыт")
				return
			}
			h.Clients[v.ID] = v
		case v, ok := <-h.Unregister:
			if !ok {
				fmt.Println("Пользователь не найден")
				return
			}
			delete(h.Clients, v.ID)
		case <-ctx.Done():
			fmt.Println("Отмена")
			return
		}
	}
}

func (c *Client) ReadPump(ctx context.Context, hub *Hub) {
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			hub.Unregister <- c
			c.conn.Close()
			return
		}
		c.sendMessage <- string(msg)

	}
}

func (c *Client) WritePump(ctx context.Context) {
	for {
		v, ok := <-c.sendMessage
		if !ok {
			return
		}
		c.conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("%s", v)))
	}
}

func (m MatchWorker) Run(ctx context.Context) {
	bufer := map[string]string{}
	for {
		queues, err := m.client.SMembers(ctx, "active_queues").Result()
		if err != nil {
			return
		}
		u, err := m.client.BLPop(ctx, 0, queues...).Result()
		if err != nil {
			return
		}
		userID, err := strconv.Atoi(u[1])
		if err != nil {
			return
		}

		if waitingUserID, ok := bufer[u[0]]; ok {
			waitingID, err := strconv.Atoi(waitingUserID)
			if err != nil {
				return
			}
			m.sendRes <- MatchResult{
				RoomID: GenerateRoomID(),
				User1:  userID,
				User2:  waitingID,
			}
			delete(bufer, u[0])
		} else {
			bufer[u[0]] = u[1]
		}

	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func wsHandler(w http.ResponseWriter, r *http.Request, hub *Hub, mWorker *MatchWorker) {
	ctx, cancle := context.WithCancel(r.Context())
	defer cancle()
	if r.Method != http.MethodGet {
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		return
	}
	userInfro, err := mWorker.client.Get(ctx, userID).Result()
	if err != nil {
		return
	}
	var c Client
	err = json.Unmarshal([]byte(userInfro), &c)
	if err != nil {
		return
	}
	c.conn = conn
	hub.Register <- &c
	go c.ReadPump(ctx, hub)
	go c.WritePump(ctx)
}

func main() {
	сtx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "",
	})
	err := rdb.Ping(сtx).Err()
	if err != nil {
		panic(err)
	}
	matchChan := make(chan MatchResult, 100)
	hub := Hub{
		Clients:    make(map[int]*Client),
		MatchChan:  matchChan,
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
	}
	matchWorker := MatchWorker{
		client:  rdb,
		sendRes: matchChan,
	}
	go hub.Run(сtx)
	go matchWorker.Run(сtx)
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHandler(w, r, &hub, &matchWorker)
	})
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
