# ⚡ Enterprise Distributed Microservice System (Go, gRPC, Redis, RabbitMQ, Kafka, Prometheus & Grafana)

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![gRPC](https://img.shields.io/badge/gRPC-v1.84-244c5a?style=flat&logo=grpc)](https://grpc.io/)
[![Protocol Buffers](https://img.shields.io/badge/Protocol%20Buffers-v3-blue?style=flat)](https://protobuf.dev/)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker)](https://www.docker.com/)
[![Redis](https://img.shields.io/badge/Redis-Cache-DC382D?style=flat&logo=redis)](https://redis.io/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-AMQP-FF6600?style=flat&logo=rabbitmq)](https://www.rabbitmq.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?style=flat&logo=prometheus)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-Observability-F46800?style=flat&logo=grafana)](https://grafana.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Implementasi arsitektur **Distributed Microservices Enterprise** dari nol menggunakan bahasa pemrograman **Go**. Sistem ini menggabungkan protokol transfer berkecepatan tinggi (**gRPC & Protobuf v3**), in-memory caching (**Redis**), pemrosesan antrean tugas asinkron (**RabbitMQ**), pemrosesan streaming log data berskala besar (**Apache Kafka KRaft**), serta observabilitas dan metrik real-time (**Prometheus & Grafana**).

---

## 🏗️ Arsitektur Sistem Terdistribusi

```text
                                  ┌───────────────────────────┐
                                  │    CLIENT (Kasir App)     │
                                  │   - Unary Request         │
                                  │   - LiveChat Bidi Stream  │
                                  │   - Auto-Inject Token     │
                                  └─────────────┬─────────────┘
                                                │ HTTP/2 (Binary Protobuf)
                                                ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                                   CORE gRPC SERVER (:50051)                               │
│                                                                                           │
│  [Server Interceptor / Middleware Auth & Logging]                                         │
│  - Verifikasi Secret Token Metadata                                                       │
│  - Expose Metrics HTTP :2112/metrics                                                      │
│                                                                                           │
│       ┌───────────────────────┬────────────────────────┬───────────────────────┐          │
│       ▼                       ▼                        ▼                       ▼          │
│  [GetProduct]          [ListProducts]           [LiveChat Stream]     [Cache-Aside Layer] │
│  - Async Processing    - Filter Kategori        - Realtime Chat       - Redis (:6379)     │
│  - Produce AMQP &      - Repeated Product       - Goroutine & Chan    - TTL 60s           │
│    Kafka Events          Binary Payload         - Message Metrics     - Sub-millisecond   │
└──────────┬─────────────────────────────────────────────────┬───────────────────┬──────────┘
           │ AMQP (Invoice Job)                              │ Event Streaming   │ Scrape Metrics
           ▼                                                 ▼                   ▼
┌──────────────────────┐                          ┌──────────────────────┐  ┌──────────────────┐
│   RabbitMQ (:5672)   │                          │  Apache Kafka (:9092)│  │Prometheus (:9090)│
│  - Task Queue        │                          │  - KRaft Mode Log    │  │ - Pull :2112     │
│  - Management :15672 │                          │  - Kafka-UI (:8080)  │  │ - 2s Interval    │
└──────────┬───────────┘                          └──────────┬───────────┘  └────────┬─────────┘
           │ Consume Job                                     │ Stream Log            │ Visualisasi
           ▼                                                 ▼                       ▼
┌──────────────────────┐                          ┌──────────────────────┐  ┌──────────────────┐
│  Worker (Go Service) │                          │ Analytics Engine (Go)│  │ Grafana (:3000)  │
│ - Cetak Invoice      │                          │ - Big Data Consumer  │  │ - Realtime Chart │
│ - Notifikasi Email   │                          │ - Sales & Audit Log  │  │ - Auto-Dashboard │
└──────────────────────┘                          └──────────────────────┘  └──────────────────┘
```

---

## 🧩 5 Pilar Arsitektur

### 1. ⚡ High-Performance RPC (gRPC & Protocol Buffers v3)
* **Binary Serialization**: Pengiriman payload biner yang jauh lebih ringkas dan hemat bandwidth dibandingkan JSON/REST.
* **Bidirectional Streaming (`LiveChat`)**: Komunikasi streaming dua arah persisten dengan Goroutine & Go Channels tanpa saling memblokir (*non-blocking*).
* **Interceptors (Middleware)**: Autentikasi token otomatis via Metadata gRPC (`codes.Unauthenticated`) dan central logging.
* **Deadline & Timeout Resiliency**: Propagasi pembatalan jaringan (`context.WithTimeout` & `<-ctx.Done()`) untuk membebaskan resource server seketika jika client terputus.

### 2. ⚡ In-Memory High-Speed Caching (Redis)
* **Cache-Aside Pattern**: Request data pertama membaca ke database/storage internal lalu disimpan ke Redis dengan TTL 60 detik. Request berikutnya disajikan langsung dari memori.
* **Performa Ekstrem**: Reduksi latensi dari **~120ms** menjadi **0.3ms** (peningkatan kecepatan hingga 400x lipat).

### 3. 📬 Asynchronous Message Broker (RabbitMQ)
* **Point-to-Point Task Queue**: Server gRPC tidak menahan client untuk urusan berat (seperti pembuatan PDF invoice atau pengiriman email). Pesan dipublikasikan ke RabbitMQ `order_invoice_queue`.
* **Worker Service**: Background worker independen yang mengonsumsi antrean secara asinkron dan menjamin keandalan pemrosesan (*task acknowledgment*).

### 4. 📊 Real-Time Event Streaming & Big Data (Apache Kafka)
* **Modern KRaft Mode**: Berjalan tanpa ketergantungan Apache Zookeeper (*Zookeeper-less*).
* **Distributed Commit Log**: Setiap transaksi produk dipublikasikan ke topic Kafka `product-events` sebagai stream log audit persisten.
* **Consumer Analytics Engine**: Microservice analitik terpisah yang memantau pergerakan penjualan dan tren produk secara real-time.

### 5. 🔭 Observability & Monitoring (Prometheus & Grafana)
* **Prometheus Metrics Exporter**: Mengukur `grpc_requests_total`, `grpc_request_duration_seconds`, `grpc_chat_messages_total`, `redis_cache_hits_total`, dan `redis_cache_misses_total` pada endpoint HTTP `:2112/metrics`.
* **Grafana Live Dashboard**: Menampilkan panel visual realtime dengan auto-refresh 5 detik untuk memantau performa, latensi, traffic gRPC, pesan chat, dan efisiensi cache hit ratio.
* **Zero-Touch Provisioning**: Dashboard dan datasource terkonfigurasi otomatis via file JSON dan YAML tanpa setup manual.

---

## 🌐 Port & Web Management Dashboard

| Service | Port | Tipe Akses | Kredensial Default |
| :--- | :--- | :--- | :--- |
| **gRPC Server** | `50051` | TCP / gRPC RPC | Metadata Token Auth |
| **Prometheus Exporter** | `2112` | HTTP (`/metrics`) | - |
| **Grafana Dashboard** | `3000` | Web UI | `admin` / `admin` |
| **Prometheus Web UI** | `9090` | Web UI | - |
| **Kafka Broker** | `9092` | TCP / Kafka Protocol | - |
| **Kafka Web UI** | `8080` | Web UI | - |
| **RabbitMQ Broker** | `5672` | AMQP Protocol | `guest` / `guest` |
| **RabbitMQ Management** | `15672` | Web UI | `guest` / `guest` |
| **Redis Server** | `6379` | TCP / Redis Protocol | - |

---

## 🚀 Panduan Menjalankan Sistem

### 1. Menyalakan Seluruh Infrastruktur (1-Click)
Jalankan seluruh infrastruktur (Redis, RabbitMQ, Kafka, Kafka-UI, Prometheus, Grafana) hanya dengan satu perintah:

```powershell
docker compose up -d
```

> **Verifikasi**: Buka **[http://localhost:3000/d/grpc-overview](http://localhost:3000/d/grpc-overview)** untuk membuka Dashboard Grafana langsung.

---

### 2. Menjalankan Microservices Backend (Terminal Terpisah)

Buka 4 terminal untuk menjalankan masing-masing service:

#### Terminal 1: Core gRPC Server
```powershell
go run ./server/main.go
```
*Server aktif di `:50051` dan mengekspos metrik Prometheus di `:2112/metrics`.*

#### Terminal 2: RabbitMQ Worker (Invoice & Email)
```powershell
go run ./worker/main.go
```
*Siap memproses antrean cetak invoice dan notifikasi asinkron.*

#### Terminal 3: Kafka Analytics Engine
```powershell
go run ./analytics/main.go
```
*Siap menyerap event transaksi masuk untuk agregasi data analitik.*

#### Terminal 4: Client Kasir Interaktif
```powershell
go run ./client/client.go
```

---

## 🧪 Menu Interaktif & Skenario Pengujian Client

Saat menjalankan `client/client.go`, Anda akan disambut menu interaktif:

```text
========================================
     APLIKASI KASIR INTERAKTIF (gRPC)
========================================
1. Ambil 1 Produk (Simulasi Timeout 1 Detik)
2. Tampilkan Katalog Lengkap
3. Live Chat Customer Service (Real-time Stream)
4. Benchmark Redis Cache (Uji Kecepatan 1000x Request)
5. Keluar
========================================
```

1. **Uji Coba Timeout Resiliency (Pilihan 1)**:
   * Kasir memanggil produk dengan batas waktu `context.WithTimeout(1 detik)`.
   * Server memproses selama 3 detik. Client otomatis membatalkan dengan status `DeadlineExceeded` dan server menghentikan kalkulasi seketika.

2. **Uji Coba Redis Benchmark (Pilihan 4)**:
   * Client menembak 1000 request berturut-turut.
   * Amati di terminal dan Grafana bagaimana Redis memangkas waktu respons dari ratusan milidetik menjadi sub-milidetik.

3. **Uji Coba LiveChat Dua Arah (Pilihan 3)**:
   * Ketik pesan di terminal client dan saksikan counter **`💬 Pesan Chat LiveChat Masuk`** di Grafana bertambah secara live!
   * Balas pesan dari terminal server gRPC secara simultan.

---

## 🛠️ Perintah Kompilasi Protobuf

Jika Anda memodifikasi file [proto/product.proto](proto/product.proto), kompilasi ulang dengan perintah:

```powershell
protoc --proto_path=. --go_out=. --go_opt=module=grpc-learning --go-grpc_out=. --go-grpc_opt=module=grpc-learning proto/product.proto
```

---

## 📁 Struktur Direktori Proyek

```text
.
├── docker-compose.yml                  # Orkestrasi 6 kontainer infrastruktur
├── prometheus.yml                      # Konfigurasi scrape target Prometheus
├── grafana/
│   ├── dashboards/
│   │   └── grpc-overview.json          # Dashboard Grafana siap pakai
│   └── provisioning/
│       ├── dashboards/dashboards.yml   # Auto-provisioning dashboard provider
│       └── datasources/prometheus.yml  # Auto-provisioning datasource Prometheus
├── proto/
│   └── product.proto                   # Kontrak Protocol Buffers v3
├── gen/product/                        # Kode Go hasil kompilasi protoc
├── server/
│   └── main.go                         # Core gRPC Server, Redis, RabbitMQ & Kafka Producer
├── worker/
│   └── main.go                         # RabbitMQ Async Worker Service
├── analytics/
│   └── main.go                         # Kafka Stream Consumer Service
├── client/
│   └── client.go                       # Interactive Terminal Client Kasir
├── CHEATSHEET.md                       # Kamus Arsitektur & Konsep Distributed Systems
└── README.md                           # Dokumentasi komprehensif sistem
```

---

## 📖 Referensi Belajar
* [CHEATSHEET.md](CHEATSHEET.md) — Kamus konsep distributed systems: gRPC, Redis Cache-Aside, RabbitMQ vs Kafka, dan Observability.
