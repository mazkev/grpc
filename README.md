# ⚡ Enterprise Distributed Microservice Ecosystem (Go, gRPC, Redis, RabbitMQ, Kafka, OpenTelemetry, Jaeger, Prometheus & Grafana)

[![Kubernetes](https://img.shields.io/badge/Kubernetes-K8s-326CE5?style=flat&logo=kubernetes)](https://kubernetes.io/)
[![Helm](https://img.shields.io/badge/Helm-v4-0F1689?style=flat&logo=helm)](https://helm.sh/)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-CI%2FCD-2088FF?style=flat&logo=githubactions)](https://github.com/features/actions)
[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![gRPC](https://img.shields.io/badge/gRPC-v1.84-244c5a?style=flat&logo=grpc)](https://grpc.io/)
[![Swagger](https://img.shields.io/badge/Swagger-OpenAPI%202.0-85EA2D?style=flat&logo=swagger)](https://swagger.io/)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Tracing-7A3997?style=flat&logo=opentelemetry)](https://opentelemetry.io/)
[![Jaeger](https://img.shields.io/badge/Jaeger-Distributed%20Tracing-60D0E4?style=flat&logo=jaeger)](https://www.jaegertracing.io/)
[![Redis](https://img.shields.io/badge/Redis-Cache-DC382D?style=flat&logo=redis)](https://redis.io/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-AMQP-FF6600?style=flat&logo=rabbitmq)](https://www.rabbitmq.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?style=flat&logo=prometheus)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-Observability-F46800?style=flat&logo=grafana)](https://grafana.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Implementasi arsitektur **Enterprise Microservices Ecosystem** dari nol menggunakan bahasa pemrograman **Go**. Sistem ini menggabungkan protokol transfer berkecepatan tinggi (**gRPC & Protobuf v3**), reverse proxy REST HTTP/JSON (**gRPC-Gateway & Swagger OpenAPI**), in-memory caching (**Redis**), antrean pemrosesan asinkron (**RabbitMQ**), pemrosesan streaming log berskala besar (**Apache Kafka KRaft**), observabilitas metrik real-time (**Prometheus & Grafana**), serta pelacakan jejak terdistribusi (**OpenTelemetry & Jaeger**).

---

## 🏗️ Arsitektur Sistem Terdistribusi

```text
                                   ┌───────────────────────────┐         ┌───────────────────────────┐
                                   │    CLIENT (Kasir App)     │         │ Web Browser / REST Client │
                                   │   - Unary Request         │         │ - HTTP GET /v1/products   │
                                   │   - LiveChat Bidi Stream  │         │ - Swagger UI (:8080/docs) │
                                   │   - Auto-Inject Token     │         │ - curl CLI                │
                                   └─────────────┬─────────────┘         └─────────────┬─────────────┘
                                                 │ HTTP/2 (Protobuf)                   │ HTTP/1.1 JSON
                                                 │                                     ▼
                                                 │                       ┌───────────────────────────┐
                                                 │                       │  gRPC-GATEWAY PROXY (:8080│
                                                 │                       │  - HTTP/JSON to gRPC      │
                                                 │                       │  - CORS & Header Matcher  │
                                                 │                       │  - Swagger UI /docs       │
                                                 │                       └─────────────┬─────────────┘
                                                 │                                     │ gRPC Unary
                                                 ▼                                     ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                                   CORE gRPC SERVER (:50051)                               │
│                                                                                           │
│  [Server Interceptor / Middleware Auth, Tracing & Metrics]                                │
│  - Verifikasi Secret Token Metadata                                                       │
│  - OpenTelemetry Distributed Tracing Handler (Jaeger)                                     │
│  - Expose Metrics HTTP :2112/metrics                                                      │
│                                                                                           │
│       ┌───────────────────────┬────────────────────────┬───────────────────────┐          │
│       ▼                       ▼                        ▼                       ▼          │
│  [GetProduct]          [ListProducts]           [LiveChat Stream]     [Cache Pre-Warming] │
│  - Sub-3ms Latency     - Filter Kategori        - Realtime Chat       - Redis (:6379)     │
│  - Non-blocking Async  - Repeated Product       - Goroutine & Chan    - Pre-warmed Memory │
│    AMQP & Kafka          Binary Payload         - Message Metrics     - 0ms Cold Penalty  │
└──────────┬─────────────────────────────────────────────────┬───────────────────┬──────────┘
           │ AMQP (Invoice Job)                              │ Event Streaming   │ OTLP Traces & Metrics
           ▼                                                 ▼                   ▼
┌──────────────────────┐                          ┌──────────────────────┐  ┌──────────────────┐
│   RabbitMQ (:5672)   │                          │  Apache Kafka (:9092)│  │ Jaeger (:16686)  │
│  - Task Queue        │                          │  - KRaft Mode Log    │  │ Prometheus(:9090)│
│  - Management :15672 │                          │  - Topic: activity   │  │ Grafana (:3000)  │
└──────────┬───────────┘                          └──────────┬───────────┘  └────────┬─────────┘
           │ Consume Job                                     │ Stream Log            │ Visualisasi
           ▼                                                 ▼                       ▼
┌──────────────────────┐                          ┌──────────────────────┐  ┌──────────────────┐
│  Worker (Go Service) │                          │ Analytics Engine (Go)│  │ Grafana Dashboard│
│ - Cetak Invoice PDF  │                          │ - Big Data Consumer  │  │ - Realtime Chart │
│ - Kirim Email Notif  │                          │ - Sales & Audit Log  │  │ - Auto-Dashboard │
└──────────────────────┘                          └──────────────────────┘  └──────────────────┘
```

---

## 🧩 7 Pilar Arsitektur Enterprise

### 1. ⚡ High-Performance RPC (gRPC & Protocol Buffers v3)
* **Binary Serialization**: Pengiriman payload biner yang jauh lebih ringkas dan hemat bandwidth dibandingkan JSON/REST.
* **Bidirectional Streaming (`LiveChat`)**: Komunikasi streaming dua arah persisten dengan Goroutine & Go Channels tanpa saling memblokir (*non-blocking*).
* **Interceptors (Middleware)**: Autentikasi token otomatis via Metadata gRPC (`codes.Unauthenticated`) dan central logging.
* **Deadline & Timeout Resiliency**: Propagasi pembatalan jaringan (`context.WithTimeout` & `<-ctx.Done()`) untuk membebaskan resource server seketika jika client terputus.

### 2. 🌐 gRPC-Gateway REST Reverse-Proxy & Swagger OpenAPI
* **REST Transcoding**: Menjembatani request HTTP/1.1 JSON (misal `GET /v1/products/E01`) menjadi panggilan gRPC biner secara otomatis.
* **CORS & Header Propagation**: Meneruskan header HTTP `Authorization` ke Metadata gRPC.
* **Interactive Swagger UI**: Tersedia antarmuka dokumentasi resmi di `http://localhost:8080/docs` dengan tombol `Authorize 🔓` untuk mencoba API langsung dari browser.

### 3. ⚡ In-Memory High-Speed Caching (Redis)
* **Cache-Aside Pattern & Pre-warming**: Memanaskan katalog produk saat server startup untuk mencegah **Cache Stampede / Thundering Herd Problem**.
* **Performa Ekstrem**: Reduksi latensi rata-rata dari **~90ms** menjadi **1–3ms** (Throughput meningkat hingga **1.870+ RPS**).

### 4. 📬 Asynchronous Message Broker (RabbitMQ)
* **Point-to-Point Task Queue**: Server gRPC tidak menahan client untuk urusan berat (seperti pembuatan PDF invoice atau pengiriman email). Pesan dilempar secara asinkron ke antrean `order_queue`.
* **Worker Service (`worker/main.go`)**: Background worker independen yang mengonsumsi antrean dan mengirimkan acknowledgment (`msg.Ack(false)`).

### 5. 📊 Real-Time Event Streaming (Apache Kafka KRaft)
* **Modern KRaft Mode**: Berjalan tanpa ketergantungan Zookeeper.
* **Distributed Commit Log**: Setiap aktivitas transaksi dipublikasikan ke topic Kafka `product-activity`.
* **Analytics Engine (`analytics/main.go`)**: Microservice analitik terpisah yang memantau pergerakan penjualan dan log audit secara real-time.

### 6. 🔭 Observability & Monitoring (Prometheus & Grafana)
* **Prometheus Metrics Exporter**: Mengukur metrik gRPC, efisiensi cache hit ratio, dan jumlah pesan chat di endpoint HTTP `:2112/metrics`.
* **Grafana Live Dashboard**: Menampilkan visualisasi performa real-time di `http://localhost:3000`.

### 7. 🔍 Distributed Tracing (OpenTelemetry & Jaeger)
* **End-to-End Waterfall Tracing**: Melacak perjalanan setiap request mulai dari Gateway -> gRPC Server -> Redis Lookup -> RabbitMQ Queue -> Kafka Stream di Jaeger UI (`http://localhost:16686`).

---

## ☸️ Enterprise Kubernetes (K8s) & Cloud-Native Deployment

Sistem telah sepenuhnya dikontainerisasi menggunakan **Multi-Stage Dockerfile** (~20 MB) dan siap dideploy ke cluster Kubernetes:

* **High-Availability & Self-Healing**: Dikelola oleh K8s Deployment dengan 3 replika Pod kembar.
* **Smart Load Balancing**: K8s Service (`type: LoadBalancer`) mendistribusikan traffic ke seluruh Pod.
* **12-Factor Decoupling**: Konfigurasi umum di **ConfigMap** (`REDIS_HOST`, `KAFKA_BROKER`), dan kredensial rahasia di **Secret** (`RABBITMQ_URL`).
* **Health Probes**: Dilengkapi sensor otomatis **Readiness Probe** dan **Liveness Probe** via endpoint `:2112/metrics`.
* **Horizontal Pod Autoscaler (HPA)**: Elastisitas otomatis menambah Pod hingga 10 buah saat CPU melewati 50%.

```powershell
# Deploy ke Kubernetes
kubectl apply -f k8s/
kubectl get pods -l app=grpc-server
kubectl get hpa
```

---

## ⎈ Helm Chart Management

Seluruh konfigurasi Kubernetes dikemas ke dalam **Helm Chart** modular di folder `helm/grpc-server/` dengan pusat kendali di `values.yaml`:

```powershell
# Install aplikasi via Helm
helm install my-grpc ./helm/grpc-server

# Scale ke 4 Pod
helm upgrade my-grpc ./helm/grpc-server --set replicaCount=4

# Rollback instan jika insiden
helm rollback my-grpc 1
```

---

## 🤖 CI/CD Pipeline Otomatis (GitHub Actions)

Alur integrasi dan pengiriman otomatis (*Continuous Integration & Delivery*) diatur dalam file `.github/workflows/ci-cd.yml`:
1. **CI (Test & Lint)**: Static analysis (`go vet`), automated unit tests (`go test`), dan validasi template Helm (`helm lint`).
2. **CD (Container Registry)**: Build Docker image dan push ke **GitHub Container Registry (GHCR)**: `ghcr.io/mazkev/grpc/grpc-server:latest`.

---

## 🌐 Port & Web Management Dashboard

| Service | Port / URL | Tipe Akses | Keterangan |
| :--- | :--- | :--- | :--- |
| **Swagger UI Interaktif** | [http://localhost:8080/docs](http://localhost:8080/docs) | Web UI | Portal dokumentasi & test REST API |
| **gRPC Gateway (REST)** | `8080` | HTTP / REST JSON | Reverse proxy ke gRPC |
| **Core gRPC Server** | `50051` | TCP / gRPC RPC | Metadata Token Auth |
| **Jaeger Tracing UI** | [http://localhost:16686](http://localhost:16686) | Web UI | Distributed Tracing Dashboard |
| **Grafana Dashboard** | [http://localhost:3000](http://localhost:3000) | Web UI | `admin` / `admin` |
| **Prometheus Exporter** | [http://localhost:2112/metrics](http://localhost:2112/metrics) | HTTP | Metrik Sensor Prometheus |
| **Prometheus Web UI** | [http://localhost:9090](http://localhost:9090) | Web UI | Metric Query Explorer |
| **RabbitMQ Management** | [http://localhost:15672](http://localhost:15672) | Web UI | `guest` / `guest` |
| **Redis Server** | `6379` | TCP / Redis Protocol | In-memory cache |
| **Apache Kafka Broker** | `9092` | TCP / Kafka Protocol | KRaft Event Broker |

---

## 🚀 Panduan Menjalankan Sistem

### 1. Menyalakan Infrastruktur (1-Click)
```powershell
docker compose up -d
```

### 2. Menjalankan Microservices Backend (Terminal Terpisah)

#### Terminal 1: Core gRPC Server
```powershell
go run ./server/main.go
```

#### Terminal 2: gRPC-Gateway REST & Swagger
```powershell
go run ./gateway/main.go
```

#### Terminal 3: RabbitMQ Worker (Invoice Printer)
```powershell
go run ./worker/main.go
```

#### Terminal 4: Kafka Analytics Engine
```powershell
go run ./analytics/main.go
```

#### Terminal 5: Client Kasir Interaktif (gRPC Client)
```powershell
go run ./client/main.go
```

---

### 🧪 Automated End-to-End (E2E) Integration Test Suite
Uji seluruh 6 pilar arsitektur microservices (Auth Interceptor, Redis Caching, RabbitMQ Task Queue, Kafka Event Commit Log, LiveChat Real-time Streaming, dan gRPC-Gateway REST Transcoding) hanya dengan satu komando:

```powershell
go test -v ./tests/...
```

---

### 🚀 High-Load Stress Testing
Jalankan load test 1.000 requests dengan 25 concurrent workers untuk menguji throughput dan latensi sub-milidetik:

```powershell
go run ./loadtest/main.go
```

---

## 🛠️ Perintah Kompilasi Protobuf
Jika Anda memodifikasi file [proto/product.proto](proto/product.proto), kompilasi ulang seluruh Go stub, gRPC Gateway, dan Swagger OpenAPI spec dengan:

```powershell
protoc -I proto --go_out=. --go_opt=module=grpc-learning --go-grpc_out=. --go-grpc_opt=module=grpc-learning --grpc-gateway_out=. --grpc-gateway_opt=module=grpc-learning --openapiv2_out=gen/openapiv2 proto/product.proto
```

---

## 📁 Struktur Direktori Proyek

```text
.
├── .github/workflows/
│   └── ci-cd.yml                       # Pipeline GitHub Actions (Test, Lint & GHCR Push)
├── Dockerfile                          # Multi-stage production build (~20MB)
├── docker-compose.yml                  # Orkestrasi kontainer infrastruktur
├── k8s/                                # Manifest deklaratif Kubernetes (Deployment, Service, HPA)
├── helm/grpc-server/                   # Helm Chart terstandar (values.yaml, templates)
├── proto/
│   ├── google/api/                     # Anotasi HTTP Google API untuk gRPC-Gateway
│   └── product.proto                   # Kontrak Protocol Buffers v3
├── gen/
│   ├── product/                        # Kode Go Protobuf & Gateway hasil kompilasi
│   └── openapiv2/                      # Swagger OpenAPI 2.0 JSON
├── server/
│   ├── main.go                         # Core gRPC Server, Redis Caching, Event Producers
│   └── main_test.go                    # Unit Tests
├── gateway/
│   └── main.go                         # gRPC-Gateway REST Proxy & Swagger UI Dashboard
├── worker/
│   └── main.go                         # RabbitMQ Async Invoice Worker
├── analytics/
│   └── main.go                         # Kafka Stream Analytics Consumer
├── client/
│   └── main.go                         # Interactive Cashier gRPC Terminal Client
├── loadtest/
│   └── main.go                         # High-load Stress Testing Tool (1.000 reqs, OTel traced)
├── tests/
│   └── e2e_test.go                     # Automated End-to-End Integration Test Suite
├── CHEATSHEET.md                       # Kamus konsep arsitektur sistem terdistribusi
└── README.md                           # Dokumentasi komprehensif proyek
```

---

## 📖 Referensi Belajar
* [CHEATSHEET.md](CHEATSHEET.md) — Kamus konsep distributed systems: gRPC, Redis Cache-Aside, RabbitMQ vs Kafka, OpenTelemetry Tracing, dan Observability.

---

## ?? Project Achievements & Quality
* Continuous integration and code quality standards enabled.

