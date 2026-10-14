# 🚀 Belajar gRPC dengan Go (Golang)

Repository ini berisi materi dan kode implementasi hands-on untuk memahami konsep dasar hingga praktik nyata **gRPC (Google Remote Procedure Call)** di Go.

---

## 📌 Apa yang Dipelajari?
- **Protocol Buffers (Protobuf v3)**: Membuat file kontrak `.proto` (`User`, `GetUserRequest`, `UserResponse`, `ListUsersRequest`).
- **Compiler `protoc`**: Melakukan generate otomatis struct Go dan interface RPC dari schema proto.
- **Unary RPC (`GetUser`)**: 1 request masuk -> 1 response keluar.
- **Server Streaming RPC (`ListUsers`)**: 1 request masuk -> server mengalirkan data satu per satu.
- **gRPC Server & Client**: Mengimplementasikan service dan menghubungkan client via HTTP/2.
- **Error Handling**: Menangani kode error terstandar gRPC (`codes.NotFound`).

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
