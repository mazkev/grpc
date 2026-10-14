# 🛍️ Studi Kasus Microservices: Order Service ➔ Payment Service via gRPC

Proyek ini mendemonstrasikan bagaimana **dua backend independen** saling berkomunikasi secara cepat dan efisien menggunakan **gRPC (HTTP/2 + Protocol Buffers)**.

---

## 🏛️ Arsitektur Sistem

```text
[Pembeli Buat Pesanan]
          │
          ▼
┌────────────────────────────────────────┐
│  🛒 BACKEND 1: ORDER SERVICE           │
│  (order-service/main.go)               │
│                                        │
│  • Menerima pesanan baru               │
│  • Mengontak Payment Service via gRPC  │
│  • Mengonfirmasi / membatalkan order   │
└──────────────────┬─────────────────────┘
                   │
                   │ ⚡ gRPC Call: ProcessPayment()
                   │    Input : { order_id, user_id, amount }
                   │    Output: { payment_id, success, message }
                   ▼
┌────────────────────────────────────────┐
│  🏦 BACKEND 2: PAYMENT SERVICE         │
│  (payment-service/main.go)             │
│                                        │
│  • Port :50051 (gRPC Server)           │
│  • Mengelola saldo dompet pengguna     │
│  • Memotong saldo & mengembalikan ID   │
└────────────────────────────────────────┘
```

---

## 📁 Struktur Direktori

```text
.
├── proto/
│   └── payment/
│       └── payment.proto      # KONTRAK: Format pesan & method ProcessPayment
├── gen/
│   └── payment/               # Kode Go hasil kompilasi protoc
│       ├── payment.pb.go
│       └── payment_grpc.pb.go
├── payment-service/
│   └── main.go                # Backend 1: Payment Service (gRPC Server)
├── order-service/
│   └── main.go                # Backend 2: Order Service (gRPC Client)
├── .gitignore
├── go.mod
└── go.sum
```

---

## 🚀 Cara Menjalankan

Buka **dua jendela terminal** terpisah di folder ini:

### Langkah 1: Jalankan Backend Payment Service (Terminal 1)
```powershell
go run ./payment-service/main.go
```
*Payment Service akan aktif di port `:50051` menunggu permintaan pembayaran.*

### Langkah 2: Jalankan Backend Order Service (Terminal 2)
```powershell
go run ./order-service/main.go
```
*Order Service akan mensimulasikan 2 transaksi pembelian nyata:*
1. **Transaksi 1 (Berhasil)**: `USER-1` belanja Sepatu Rp 200.000 (Saldo Rp 500.000 $\rightarrow$ Saldo dipotong dan order dikonfirmasi).
2. **Transaksi 2 (Gagal)**: `USER-2` belanja Laptop Rp 1.500.000 (Saldo Rp 50.000 $\rightarrow$ Pembayaran ditolak dan order dibatalkan).
