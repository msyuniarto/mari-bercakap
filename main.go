package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// =============================================
// STRUCTS
// =============================================

// Client merepresentasikan satu koneksi WebSocket
type Client struct {
	conn     *websocket.Conn
	send     chan Message
	username string
	hub      *Hub
}

// Message adalah struktur pesan yang dikirim antar client
type Message struct {
	Type          string   `json:"type"` // "message", "join", "leave", "typing", "read"
	ID            string   `json:"id"`   // ID unik per pesan
	Username      string   `json:"username"`
	Text          string   `json:"text"`
	Timestamp     string   `json:"timestamp"`     // singkat: "15:04"
	FullTimestamp string   `json:"fullTimestamp"` // lengkap: "Senin, 07 Apr 2026 · 15:04:32"
	UserCount     int      `json:"userCount"`
	ReadBy        []string `json:"readBy"` // daftar username yang sudah baca
}

// IncomingMessage adalah pesan yang diterima dari client
type IncomingMessage struct {
	Type  string `json:"type"` // "message", "typing", "read"
	Text  string `json:"text"`
	MsgID string `json:"msgId"` // diisi saat type == "read"
}

// TypingEvent membawa pesan typing beserta pointer pengirimnya
type TypingEvent struct {
	msg    Message
	sender *Client
}

// ReadEvent membawa info pesan mana yang dibaca oleh siapa
type ReadEvent struct {
	msgID  string
	reader string
}

// Hub mengelola semua client yang terhubung
type Hub struct {
	clients    map[*Client]bool
	messages   map[string]*Message // menyimpan semua pesan berdasarkan ID
	broadcast  chan Message
	typing     chan TypingEvent
	readEvent  chan ReadEvent
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

// =============================================
// HUB
// =============================================

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		messages:   make(map[string]*Message),
		broadcast:  make(chan Message, 256),
		typing:     make(chan TypingEvent, 64),
		readEvent:  make(chan ReadEvent, 128),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			count := len(h.clients)
			h.mu.Unlock()

			// Broadcast pesan "user join" ke semua client
			h.broadcast <- Message{
				Type:          "join",
				Username:      client.username,
				Text:          client.username + " bergabung ke chat",
				Timestamp:     now(),
				FullTimestamp: fullNow(),
				UserCount:     count,
			}

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			count := len(h.clients)
			h.mu.Unlock()

			// Broadcast pesan "user leave"
			h.broadcast <- Message{
				Type:          "leave",
				Username:      client.username,
				Text:          client.username + " keluar dari chat",
				Timestamp:     now(),
				FullTimestamp: fullNow(),
				UserCount:     count,
			}

		case msg := <-h.broadcast:
			// Simpan pesan ke map jika punya ID (pesan chat biasa)
			if msg.ID != "" {
				h.mu.Lock()
				msgCopy := msg
				h.messages[msg.ID] = &msgCopy
				h.mu.Unlock()
			}

			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- msg:
				default:
					// Jika buffer penuh, tutup koneksi client
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()

		case event := <-h.typing:
			// Kirim typing hanya ke client LAIN (bukan pengirimnya)
			h.mu.RLock()
			for client := range h.clients {
				if client == event.sender {
					continue
				}
				select {
				case client.send <- event.msg:
				default:
				}
			}
			h.mu.RUnlock()

		case re := <-h.readEvent:
			// Update readBy di message store
			h.mu.Lock()
			msg, ok := h.messages[re.msgID]
			if ok {
				// Cegah duplikat
				alreadyRead := false
				for _, u := range msg.ReadBy {
					if u == re.reader {
						alreadyRead = true
						break
					}
				}
				if !alreadyRead {
					msg.ReadBy = append(msg.ReadBy, re.reader)
				}
			}
			var readBy []string
			if ok {
				readBy = msg.ReadBy
			}
			h.mu.Unlock()

			if !ok {
				continue
			}

			// Broadcast update "read" ke semua client
			h.mu.RLock()
			updateMsg := Message{
				Type:   "read",
				ID:     re.msgID,
				ReadBy: readBy,
			}
			for client := range h.clients {
				select {
				case client.send <- updateMsg:
				default:
				}
			}
			h.mu.RUnlock()
		}
	}
}

// =============================================
// CLIENT
// =============================================

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Izinkan semua origin (untuk development)
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// readPump membaca pesan dari client dan meneruskan ke hub
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(512 * 1024) // max 512KB per pesan
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, rawMsg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}

		// Parse pesan dari client
		var incoming IncomingMessage
		if err := json.Unmarshal(rawMsg, &incoming); err != nil {
			continue
		}

		switch incoming.Type {
		case "typing":
			c.hub.typing <- TypingEvent{
				msg: Message{
					Type:     "typing",
					Username: c.username,
					Text:     incoming.Text, // "start" atau "stop"
				},
				sender: c,
			}

		case "read":
			if incoming.MsgID != "" {
				c.hub.readEvent <- ReadEvent{
					msgID:  incoming.MsgID,
					reader: c.username,
				}
			}

		default:
			// Kirim ke hub untuk di-broadcast sebagai pesan chat
			c.hub.broadcast <- Message{
				Type:          "message",
				ID:            generateID(),
				Username:      c.username,
				Text:          incoming.Text,
				Timestamp:     now(),
				FullTimestamp: fullNow(),
				ReadBy:        []string{},
			}
		}
	}
}

// writePump mengirim pesan dari channel ke WebSocket client
func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			data, err := json.Marshal(msg)
			if err != nil {
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}

		case <-ticker.C:
			// Kirim ping secara berkala agar koneksi tetap hidup
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// =============================================
// HTTP HANDLER
// =============================================

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "username diperlukan", http.StatusBadRequest)
		return
	}

	// Upgrade koneksi HTTP ke WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}

	client := &Client{
		conn:     conn,
		send:     make(chan Message, 256),
		username: username,
		hub:      hub,
	}

	hub.register <- client

	// Jalankan read dan write di goroutine terpisah
	go client.writePump()
	go client.readPump()
}

// =============================================
// MAIN
// =============================================

func main() {
	hub := newHub()
	go hub.run()

	// Serve file statis (index.html)
	http.Handle("/", http.FileServer(http.Dir("./static")))

	// Endpoint WebSocket
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	addr := ":8080"
	log.Printf("Server berjalan di http://localhost%s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func now() string {
	return time.Now().Format("15:04")
}

func fullNow() string {
	t := time.Now()
	// Format: "Senin, 07 Apr 2026 · 15:04:32"
	days := []string{"Minggu", "Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"}
	months := []string{"", "Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"}
	return days[t.Weekday()] + ", " +
		t.Format("02") + " " + months[t.Month()] + " " + t.Format("2006") +
		" · " + t.Format("15:04:05")
}

// generateID membuat ID unik sederhana berdasarkan waktu
func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
