# 🛒 Katalog Produk (Product Service gRPC)

Contoh gRPC sederhana untuk backend Katalog Produk di Go.

## Struktur Project
```text
.
├── proto/
│   └── product.proto    # Kontrak service & model Product
├── gen/
│   └── product/         # Hasil generate protoc
├── server/
│   └── main.go          # gRPC Server Katalog Produk (:50051)
└── client/
    └── client.go        # Client pemanggil gRPC

## Cara Menjalankan

1. **Terminal 1 (Jalankan Server):**
   ```powershell
   go run ./server/main.go
   ```

2. **Terminal 2 (Jalankan Client):**
   ```powershell
   go run ./client/client.go
   ```

---

## 📖 Dokumentasi Pendukung
- [CHEATSHEET.md](CHEATSHEET.md) - Kamus istilah Go & gRPC untuk pemula.
