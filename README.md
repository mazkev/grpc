# ⚡ gRPC Microservice: Product & Real-time LiveChat System

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![gRPC](https://img.shields.io/badge/gRPC-v1.84-244c5a?style=flat&logo=grpc)](https://grpc.io/)
[![Protocol Buffers](https://img.shields.io/badge/Protocol%20Buffers-v3-blue?style=flat)](https://protobuf.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Implementasi layanan microservices berbasis **gRPC** dan **Protocol Buffers v3** menggunakan bahasa pemrograman **Go**. Proyek ini mendemonstrasikan pola arsitektur standar industri, mulai dari *Unary RPC*, *Bidirectional Streaming*, *Metadata Authentication*, *Server & Client Interceptors*, hingga *Context Deadline / Timeout Handling*.

---

## 🏗️ Arsitektur & Alur Kerja Sistem

```text
┌────────────────────────────────────────────────────────┐
│                   CLIENT (Kasir App)                   │
│                                                        │
│  [Client Interceptor]                                  │
│  - Otomatis inject Token ke Metadata Outgoing          │
│  - Logging panggilan RPC keluar                        │
└──────────────────────────┬─────────────────────────────┘
                           │  HTTP/2 (Binary Protobuf)
                           ▼
┌────────────────────────────────────────────────────────┐
│                   SERVER (Gudang Core)                 │
│                                                        │
│  [Server Interceptor / Middleware Lobi Utama]          │
│  - Ekstrak & Validasi Token Metadata                   │
│  - Tolak akses ilegal (codes.Unauthenticated)          │
│  - Logging durasi & nama RPC                           │
│                          │                             │
│         ┌────────────────┴───────────────┐             │
│         ▼                                ▼             │
│  ┌───────────────┐              ┌────────────────┐     │
│  │   Unary RPC   │              │  Bidi Stream   │     │
│  │ - GetProduct  │              │  - LiveChat    │     │
│  │ - ListProducts│              │    (Real-time) │     │
│  └───────────────┘              └────────────────┘     │
└────────────────────────────────────────────────────────┘
```

---

## ✨ Fitur-Fitur Utama

### 1. 📜 Kontrak Protocol Buffers v3 Terstruktur
* **`enum Category`**: Pengelompokan kategori produk (`ELECTRONICS`, `FOOD`, `FASHION`) dengan nilai dasar `CATEGORY_UNSPECIFIED = 0`.
* **`repeated Product`**: Pengiriman sekumpulan data (list/slice) produk sekaligus secara efisien dalam format biner terkompresi.

### 2. ⚡ Unary RPC
* **`GetProduct`**: Mengambil detail satu produk berdasarkan ID, dilengkapi simulasi pemrosesan asinkron.
* **`ListProducts`**: Menampilkan katalog seluruh produk dengan opsi filter berdasarkan kategori.

### 3. 💬 Bidirectional Streaming RPC (`LiveChat`)
* Komunikasi real-time dua arah antara Kasir (Client) dan Gudang (Server) dalam satu koneksi HTTP/2 persisten.
* Menggunakan **Goroutine** dan **Go Channels (`chan`)** untuk membaca ketikan terminal (`bufio.Scanner`) secara simultan tanpa saling memblokir (*non-blocking*).

### 4. 🛡️ Keamanan & Autentikasi Metadata
* Mengirimkan API Token rahasia lewat `google.golang.org/grpc/metadata` (setara HTTP Authorization Header di REST).
* Menolak permintaan tidak sah dengan kode status resmi gRPC: `codes.Unauthenticated` (setara HTTP 401).

### 5. 🚪 Interceptor / Middleware Pattern (Server & Client)
* **Server Interceptor**: Bertindak sebagai satpam lobi terpusat. Memeriksa tiket token sebelum request menyentuh fungsi manapun (*Don't Repeat Yourself*).
* **Client Interceptor**: Asisten otomatis di sisi client yang menyelipkan token ke setiap request keluar secara transparan.

### 6. ⏱️ Deadline & Timeout Resiliency
* Menggunakan `context.WithTimeout` untuk mencegah aplikasi kasir menggantung (*freeze*) saat jaringan bermasalah.
* **Propagasi Pembatalan Jaringan**: Server mendengarkan sinyal `<-ctx.Done()`. Begitu client timeout, server seketika menghentikan proses untuk menghemat CPU dan daya database.

---

## 📁 Struktur Direktori

```text
.
├── proto/
│   └── product.proto           # Kontrak Protocol Buffers v3
├── gen/
│   └── product/                # Kode Go hasil generate compiler protoc
│       ├── product.pb.go
│       └── product_grpc.pb.go
├── server/
│   └── main.go                 # Implementasi Server gRPC (:50051)
├── client/
│   └── client.go               # Implementasi Client Interaktif gRPC
├── CHEATSHEET.md               # Kamus istilah Go & gRPC lengkap untuk pemula
└── README.md                   # Dokumentasi proyek
```

---

## 🚀 Panduan Menjalankan Proyek

### Prasyarat
* **Go** 1.20+ terpasang di komputer.
* **Protoc** (Protocol Buffers Compiler) terpasang jika ingin mengedit `.proto`.

### 1. Jalankan Server gRPC
Buka terminal pertama:
```powershell
go run ./server/main.go
```
*Server akan berjalan di port `:50051` dan siap menyambut koneksi.*

### 2. Jalankan Client gRPC
Buka terminal kedua:
```powershell
go run ./client/client.go
```

---

## 🧪 Skenario Pengujian

Saat Client dijalankan, sistem akan otomatis melakukan serangkaian simulasi:

1. **Uji Coba Timeout (1 Detik)**:
   Kasir meminta produk dengan batas waktu 1 detik, sedangkan server membutuhkan waktu 3 detik.
   * *Hasil Client*: Membatalkan permintaan dengan status `DeadlineExceeded`.
   * *Hasil Server*: Menerima sinyal pembatalan `context deadline exceeded` dan menghentikan proses.

2. **Uji Coba Katalog Produk (`repeated` & `enum`)**:
   Kasir meminta seluruh daftar barang. Server mengembalikan katalog dalam bentuk tabel ASCII:
   ```text
   --------------------------------------------------------------------------
   ID    | NAMA BARANG                | KATEGORI      | HARGA         | STOK 
   --------------------------------------------------------------------------
   E01   | Laptop Gaming ROG          | ELECTRONICS   | Rp 25000000   | 5    
   E02   | Mechanical Keyboard RGB    | ELECTRONICS   | Rp 1200000    | 15   
   F01   | Kopi Susu Gula Aren        | FOOD          | Rp 18000      | 100  
   F02   | Croissant Cokelat          | FOOD          | Rp 25000      | 20   
   C01   | Jaket Hoodie Hitam         | FASHION       | Rp 350000     | 12   
   --------------------------------------------------------------------------
   ```

3. **Uji Coba Live Chat Dua Arah**:
   * Ketik pesan Anda di terminal Client, tekan `[ENTER]` $\rightarrow$ Server menerima pesan secara real-time.
   * Balas pesan dari terminal Server $\rightarrow$ Client menerima balasan.
   * Ketik `exit` di Client untuk menutup sesi secara elegan.

---

## 🛠️ Perintah Kompilasi Protobuf

Jika Anda melakukan perubahan pada file [proto/product.proto](proto/product.proto), kompilasi ulang dengan perintah:

```powershell
protoc --proto_path=. --go_out=. --go_opt=module=grpc-learning --go-grpc_out=. --go-grpc_opt=module=grpc-learning proto/product.proto
```

---

## 📖 Referensi Belajar
* [CHEATSHEET.md](CHEATSHEET.md) — Kamus istilah praktis Go & gRPC untuk memahami pointer, struct, interface, context, interceptor, metadata, dan status codes.
