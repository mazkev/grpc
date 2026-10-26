# ==========================================
# PANGGUNG 1: Dapur Kompilasi (Builder)
# ==========================================
FROM golang:alpine AS builder

WORKDIR /app

# 1. Salin daftar dependensi dan download library
COPY go.mod go.sum ./
RUN go mod download

# 2. Salin seluruh kode sumber proyek
COPY . .

# 3. Kompilasi aplikasi gRPC Server menjadi file biner tunggal (CGO_ENABLED=0 agar portabel)
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server_app ./server/main.go

# ==========================================
# PANGGUNG 2: Kontainer Produksi Super Ramping
# ==========================================
FROM alpine:latest

WORKDIR /app

# 4. HANYA salin file biner hasil kompilasi dari panggung 1 (Ukuran super mungil!)
COPY --from=builder /app/server_app .

# 5. Buka pintu port gRPC dan port Metrik Prometheus
EXPOSE 50051 2112

# 6. Perintah yang otomatis dijalankan saat kontainer menyala
CMD ["./server_app"]
