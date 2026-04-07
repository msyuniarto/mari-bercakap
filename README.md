# MariBercakap 💬

Aplikasi chat realtime sederhana yang dibangun menggunakan **Go** di backend dan **Vanilla JavaScript** di frontend, berkomunikasi melalui protokol **WebSocket**.

## Demo Fitur

- 💬 **Chat realtime** — pesan terkirim instan ke semua user yang terhubung
- ✍️ **Indikator mengetik** — tampilkan siapa yang sedang mengetik
- 🔔 **Notifikasi suara** — bunyi berbeda untuk pesan masuk, user join, dan user leave
- 🕐 **Timestamp lengkap** — hover pada waktu pesan untuk melihat tanggal & jam lengkap
- 🟢 **Status koneksi** — indikator realtime apakah terhubung atau terputus
- 🔄 **Auto-reconnect** — otomatis menyambung kembali jika koneksi terputus

## Tech Stack

| Layer    | Teknologi |
|----------|-----------|
| Backend  | Go + [gorilla/websocket](https://github.com/gorilla/websocket) |
| Frontend | HTML + CSS + Vanilla JavaScript |
| Protocol | WebSocket (RFC 6455) |

## Struktur Proyek

```
mari-bercakap/
├── main.go          # Server WebSocket (Hub, Client, HTTP handler)
├── go.mod
├── go.sum
└── static/
    └── index.html   # Frontend (HTML + CSS + JS)
```

## Cara Menjalankan

### Prasyarat
- [Go](https://golang.org/dl/) versi 1.21 atau lebih baru

### Langkah-langkah

```bash
### 1. Clone repo
git clone https://github.com/msyuniarto/mari-bercakap.git
cd mari-bercakap

# 2. Download dependency
go mod tidy

# 3. Jalankan server
go run main.go
```

Buka browser dan akses:
```
http://localhost:8080
```

Untuk mencoba chat, buka di **dua tab atau dua browser berbeda** dengan username yang berbeda.

## Cara Kerja

```
Browser A                  Server Go (Hub)         Browser B
   |                             |                      |
   |--- WebSocket Upgrade -----> |                      |
   |                             | <-- WS Upgrade ----- |
   |                             |                      |
   |--- {"type":"message"} ----> |                      |
   |                             |--- broadcast ------> |
   |                             |                      |
   |--- {"type":"typing"} -----> |                      |
   |                             |--- forward --------> |
```

1. Browser membuka koneksi WebSocket ke `/ws?username=nama`
2. **Hub** menyimpan semua koneksi aktif dalam `map[*Client]bool`
3. Setiap pesan masuk di-broadcast ke semua client via goroutine
4. Event `typing` hanya diteruskan ke client lain (bukan pengirim)
5. Ping/pong setiap 54 detik menjaga koneksi tetap hidup
6. Frontend auto-reconnect setiap 3 detik jika koneksi terputus

## Pengembangan Selanjutnya

- [ ] Emoji picker
- [ ] History pesan (SQLite/PostgreSQL)
- [ ] Multiple room/channel
- [ ] Autentikasi user (JWT)
- [ ] Indikator pesan dibaca (✓✓)
- [ ] Kirim gambar/file