# 📖 Cheat Sheet: Kamus Istilah Go & gRPC untuk Pemula

Catatan praktis untuk memahami istilah-istilah inti dalam ekosistem Go dan gRPC tanpa pusing.

---

## 🐹 1. Pondasi Bahasa Go

| Istilah | Penjelasan Sederhana | Contoh Nyata |
|---|---|---|
| **`struct`** | Cetakan / wadah data buatan kita sendiri. | `type User struct { Name string }` |
| **Pointer (`&`)** | Mengambil **alamat memori** suatu data (bukan menduplikasi fisik datanya). | `u := &User{Name: "Kevin"}` |
| **Pointer (`*`)** | Tanda tipe data yang menerima alamat memori. | `func Proses(u *User)` |
| **Method** | Fungsi yang **menempel** pada struct tertentu (ditandai dengan `(s *server)` di depan nama fungsinya). | `func (s *server) SayHello() string` |
| **`interface`** | Daftar syarat / kontrak fungsi yang wajib dimiliki oleh suatu struct. | `type Greeter interface { SayHello() }` |
| **`context.Context`** | "Stopwatch" atau pengendali batas waktu (timeout) agar aplikasi tidak menggantung selamanya jika jaringan terputus. | `ctx, cancel := context.WithTimeout(...)` |

---

## ⚡ 2. Istilah gRPC & Protocol Buffers

| Istilah | Penjelasan Sederhana | Contoh Nyata |
|---|---|---|
| **File `.proto`** | Surat perjanjian / kontrak resmi antara pemanggil dan penyedia service. | [proto/hello.proto](file:///d:/prog/grpc/proto/hello.proto) |
| **`protoc`** | Mesin compiler yang membaca file `.proto` lalu otomatis menghasilkan kode Go di folder `gen/`. | `protoc --go_out=. ...` |
| **Unary RPC** | Pola standar: Client kirim 1 request $\rightarrow$ Server balas 1 response. | Fungsi `SayHello` |
| **Streaming RPC** | Pola data mengalir bertahap (bisa dari Server, dari Client, atau dua arah bolak-balik). | Chat, realtime sync, download file besar |
| **Metadata** | Wadah data ekstra (mirip HTTP Headers) untuk mengirim token, session ID, atau info auth via `context`. | `metadata.AppendToOutgoingContext(...)` |
| **Interceptor** | Middleware gRPC (gerbang lobi utama) untuk mencegat dan memeriksa SEMUA request sebelum masuk ke fungsi tujuan (auth, logging, metric). | `grpc.UnaryInterceptor(...)`, `grpc.StreamInterceptor(...)` |
| **Status Codes** | Kode status resmi gRPC (pengganti HTTP 404, 401, 500) misal `codes.Unauthenticated`, `codes.NotFound`. | `status.Errorf(codes.Unauthenticated, ...)` |
| **`Unimplemented...`** | "Ban serep pengaman" bawaan gRPC yang di-embed ke struct server agar program tidak crash jika ada fungsi baru yang belum sempat kita buat. | `hellopb.UnimplementedGreeterServer` |
| **Client Stub** | Objek perantara di sisi client untuk memanggil fungsi server seolah-olah fungsi lokal. | `client := hellopb.NewGreeterClient(conn)` |

---

## 🛠️ 3. Perintah Rutin di Terminal

```powershell
# 1. Menjalankan Server
go run ./server/main.go

# 2. Menjalankan Client
go run ./client/main.go

# 3. Kompilasi ulang file .proto ke Go
protoc --proto_path=. --go_out=. --go_opt=module=grpc-learning --go-grpc_out=. --go-grpc_opt=module=grpc-learning proto/hello.proto

# 4. Simpan ke GitHub (Bikin Kotak Hijau 🟢)
git add .
git commit -m "docs: perbarui catatan belajar"
git push origin main
```
