# 📖 Cheat Sheet: Kamus Sistem Terdistribusi & Microservices (Go, gRPC, Redis, RabbitMQ, Kafka, Grafana)

Catatan praktis dan komparasi arsitektur untuk memahami konsep distributed systems, backend concurrency, dan observabilitas modern tanpa pusing.

---

## 🐹 1. Pondasi Inti Bahasa Go (Concurrency & Networking)

| Istilah | Penjelasan Sederhana | Contoh Nyata di Proyek |
|---|---|---|
| **`struct`** | Cetakan / wadah data buatan kita sendiri. | `type server struct { ... }` |
| **Pointer (`&` dan `*`)** | `&` mengambil alamat memori, `*` mengakses isi data dari alamat tersebut (menghindari duplikasi memori). | `s := &server{}` / `func (s *server)` |
| **Goroutine (`go func()`)** | Thread super ringan di Go (memori awal hanya ~2KB) untuk menjalankan proses di latar belakang secara bersamaan. | `go func() { stream.Recv() }()` |
| **Go Channels (`chan`)** | Saluran pipa komunikasi aman antar Goroutine tanpa risiko *data race*. | `errChan := make(chan error)` |
| **`context.Context`** | "Stopwatch" dan pengendali batas waktu (*timeout*) serta propagasi sinyal pembatalan ke seluruh alur pemanggilan. | `ctx, cancel := context.WithTimeout(ctx, 1*time.Second)` |
| **`select` Statement** | Switch-case khusus untuk mendengarkan beberapa channel secara bersamaan (misal menunggu pesan masuk ATAU sinyal timeout). | `select { case msg := <-ch: ... case <-ctx.Done(): ... }` |

---

## ⚡ 2. gRPC & Protocol Buffers v3

| Istilah | Penjelasan Sederhana | Nilai Tambah vs REST JSON |
|---|---|---|
| **Protocol Buffers (`.proto`)** | Bahasa kontrak data biner independen platform. | Tipe data ketat (*strict typing*), tanpa human typo. |
| **Unary RPC** | 1 Request $\rightarrow$ 1 Response. | Pengganti endpoint REST biasa dengan kecepatan biner HTTP/2. |
| **Bidirectional Streaming** | 1 Stream Connection terbuka persisten $\rightarrow$ Client & Server bebas kirim data bolak-balik bersamaan. | Jauh lebih efisien dibanding polling REST / WebSocket manual. |
| **Metadata** | Header informasi rahasia atau kontekstual yang disematkan dalam `context`. | Tempat meletakkan API Secret Token untuk otentikasi. |
| **Server/Client Interceptors** | Middleware terpusat untuk otentikasi, logging, dan metrik. | DRY (*Don't Repeat Yourself*), kode handler tetap bersih. |
| **Status Codes** | Kode balasan standar gRPC (`OK`, `Unauthenticated`, `DeadlineExceeded`). | Standar industri pengganti HTTP status 200, 401, 504. |

---

## ⚖️ 3. Perbandingan Mendalam: RabbitMQ vs Apache Kafka

Dua teknologi ini sering disalahpahami oleh engineer pemula. Keduanya melayani kebutuhan yang berbeda:

| Aspek | 📬 RabbitMQ (Message Broker) | 📊 Apache Kafka (Event Streaming Log) |
|---|---|---|
| **Model Utama** | **Smart Broker, Dumb Consumer** (Broker membagi-bagikan pesan). | **Dumb Broker, Smart Consumer** (Broker hanya menyimpan log, consumer membaca sendiri offset-nya). |
| **Penyimpanan Pesan** | Pesan **dihapus** dari queue begitu selesai diproses (*Ack*). | Pesan **disimpan persisten** di disk sesuai retensi (misal 7 hari), tidak hilang setelah dibaca. |
| **Replay / Putar Ulang** | Tidak bisa (pesan sudah hilang). | **Bisa diputar ulang** kapan saja dengan mengubah nilai *Offset*. |
| **Kasus Penggunaan Ideal** | Task Queue, cetak invoice, kirim email/SMS, pemrosesan pesanan tunggal. | Big Data analytics, clickstream tracking, event sourcing, auditing transaksi. |
| **Penerapan di Proyek Kita** | Digunakan oleh `worker/main.go` untuk mencetak invoice produk. | Digunakan oleh `analytics/main.go` untuk log audit & agregasi analitik. |

---

## ⚡ 4. Pola Caching & Observability

| Konsep | Penjelasan | Implementasi di Proyek |
|---|---|---|
| **Cache-Aside Pattern** | Aplikasi membaca ke Redis dulu (*Cache Hit*). Jika tidak ditemukan (*Cache Miss*), ambil dari Database internal, lalu simpan ke Redis dengan batas waktu (TTL). | `server/main.go` di method `GetProduct` (TTL 60s). |
| **Prometheus Pull Model** | Prometheus secara aktif "menarik" (*scrape*) data metrik dari aplikasi kita setiap 2 detik via endpoint HTTP `/metrics`. | Target `host.docker.internal:2112` di `prometheus.yml`. |
| **Grafana Dashboard** | Lapisan visualisasi real-time berbasis web yang meminta query data dari Prometheus. | Dashboard `grpc-overview` dengan auto-refresh 5 detik. |
| **Counter vs Gauge vs Histogram** | • **Counter**: Angka yang hanya bisa naik (misal: jumlah transaksi, jumlah chat).<br>• **Gauge**: Nilai yang bisa naik-turun (misal: suhu CPU, memori).<br>• **Histogram**: Mengukur durasi/latensi waktu dalam bucket. | `grpc_requests_total` (Counter), `grpc_chat_messages_total` (Counter). |

---

## 🛠️ 5. Cheat Sheet Perintah Penting

### Docker Compose
```powershell
# 1. Menyalakan seluruh infrastruktur di latar belakang
docker compose up -d

# 2. Melihat status seluruh kontainer
docker compose ps

# 3. Melihat log kontainer tertentu (misal: kafka)
docker compose logs -f kafka

# 4. Mematikan seluruh infrastruktur
docker compose down
```

### Menjalankan Microservices di Terminal
```powershell
# Core Server gRPC (Port :50051 & Metrik :2112)
go run ./server/main.go

# RabbitMQ Worker (Cetak Invoice & Notifikasi)
go run ./worker/main.go

# Kafka Analytics Engine (Event Log Consumer)
go run ./analytics/main.go

# Client Kasir Interaktif
go run ./client/client.go
```

### Git & GitHub Workflow (Kotak Hijau 🟢)
```powershell
git add .
git commit -m "feat: arsitektur lengkap distributed system"
git push origin main
```
