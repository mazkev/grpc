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
| **`repeated`** | Tipe data List / Array di Protobuf (menjadi slice `[]*Product` di Go). | `repeated Product products = 1;` |
| **`enum`** | Daftar pilihan opsi tetap bernomor (wajib ada nilai default bernilai 0). | `enum Category { UNSPECIFIED = 0; FOOD = 1; }` |
| **Unary RPC** | Pola standar: Client kirim 1 request $\rightarrow$ Server balas 1 response. | Fungsi `SayHello` |
| **Streaming RPC** | Pola data mengalir bertahap (bisa dari Server, dari Client, atau dua arah bolak-balik). | Chat, realtime sync, download file besar |
| **Metadata** | Wadah data ekstra (mirip HTTP Headers) untuk mengirim token, session ID, atau info auth via `context`. | `metadata.AppendToOutgoingContext(...)` |
| **Server Interceptor** | Satpam di sisi Server: memeriksa token / catat log sebelum fungsi diproses. | `grpc.UnaryInterceptor`, `grpc.StreamInterceptor` |
| **Client Interceptor** | Asisten di sisi Client: otomatis menyisipkan token / header ke setiap panggilan RPC. | `grpc.WithUnaryInterceptor`, `grpc.WithStreamInterceptor` |
| **Deadline / Timeout** | Batas maksimal waktu tunggu panggilan RPC agar aplikasi tidak macet jika jaringan/server lambat. | `context.WithTimeout(ctx, 2*time.Second)` |
| **Status Codes** | Kode status resmi gRPC (pengganti HTTP 404, 401, 500) misal `codes.Unauthenticated`, `codes.DeadlineExceeded`. | `status.Errorf(codes.Unauthenticated, ...)` |
| **`Unimplemented...`** | "Ban serep pengaman" bawaan gRPC yang di-embed ke struct server agar program tidak crash jika ada fungsi baru yang belum sempat kita buat. | `hellopb.UnimplementedGreeterServer` |
| **Client Stub** | Objek perantara di sisi client untuk memanggil fungsi server seolah-olah fungsi lokal. | `client := hellopb.NewGreeterClient(conn)` |
| **Redis Cache-Aside** | Pola cek data di Redis dulu (Cache Hit $\rightarrow$ cepat). Jika belum ada (Cache Miss), ambil dari DB lalu simpan ke Redis dengan TTL. | `rdb.Get(ctx, key)`, `rdb.Set(ctx, key, val, ttl)` |
| **RabbitMQ (Message Broker)** | Pengirim & penampung antrean pesan asinkron (*Event-Driven*) antara gRPC Producer dan Worker Consumer. | `ch.PublishWithContext(...)`, `ch.Consume(...)` |

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
