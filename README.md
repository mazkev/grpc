# 🚀 gRPC Hello World (Contoh Paling Sederhana)

Contoh dasar gRPC di Go tanpa database, tanpa streaming, dan tanpa logika rumit.

## Struktur Project
```text
.
├── proto/
│   └── hello.proto    # Kontrak service & pesan
├── gen/
│   └── hello/         # Hasil generate protoc
├── server/
│   └── main.go        # Server gRPC (:50051)
└── client/
    └── main.go        # Client gRPC pemanggil
```

## Cara Menjalankan

1. **Terminal 1 (Jalankan Server):**
   ```powershell
   go run ./server/main.go
   ```

2. **Terminal 2 (Jalankan Client):**
   ```powershell
   go run ./client/main.go
   ```
