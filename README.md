# 🚀 Belajar gRPC dengan Go (Golang)

Repository ini berisi materi dan kode implementasi hands-on untuk memahami konsep dasar hingga praktik nyata **gRPC (Google Remote Procedure Call)** di Go.

---

## 📌 Apa yang Dipelajari?
Lengkap dengan **4 Pola Komunikasi Utama gRPC**:
1. **Unary RPC (`GetUser`)**: 1 request masuk -> 1 response keluar (mirip REST API biasa).
2. **Server Streaming RPC (`ListUsers`)**: 1 request masuk -> server mengalirkan data satu per satu ke client.
3. **Client Streaming RPC (`CreateUsers`)**: Client mengalirkan batch data ke server -> server membalas 1 ringkasan (*summary*).
4. **Bidirectional Streaming RPC (`UserChat`)**: Client dan server saling bertukar stream data secara bersamaan (*full-duplex real-time*).
- **Protocol Buffers (Protobuf v3)**: Kontrak data terstruktur yang dikompilasi dengan `protoc`.
- **gRPC Server & Client**: Arsitektur jaringan berbasis HTTP/2 di Go.
- **Error Handling**: Kode error terstandar gRPC (`codes.NotFound`).

---

## 📁 Struktur Direktori
```text
.
├── proto/
│   └── user/
│       └── user.proto      # Kontrak Protobuf (Data model & RPC service)
├── gen/
│   └── user/               # Hasil generate protoc (JANGAN diedit manual)
│       ├── user.pb.go
│       └── user_grpc.pb.go
├── server/
│   └── main.go             # Implementasi gRPC Server
├── client/
│   └── main.go             # Implementasi gRPC Client
├── .gitignore
├── go.mod
└── go.sum
```

---

## 🛠️ Cara Menjalankan

### 1. Compile Protobuf (Opsional jika mengubah `.proto`)
```powershell
protoc --proto_path=. --go_out=. --go_opt=module=grpc-learning --go-grpc_out=. --go-grpc_opt=module=grpc-learning proto/user/user.proto
```

### 2. Jalankan Server (Terminal 1)
```powershell
go run ./server/main.go
```
*Server akan berjalan di port `:50051`.*

### 3. Jalankan Client (Terminal 2)
```powershell
go run ./client/main.go
```
*Client akan memanggil RPC `GetUser` untuk user yang ada (`usr-1`) dan menguji error handling untuk user yang tidak ada (`usr-999`).*
