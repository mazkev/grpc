package main

import "fmt"

// -------------------------------------------------------------
// 1. STRUCT (Cetakan Data)
// Mirip class di bahasa lain, tapi tanpa method di dalamnya.
// -------------------------------------------------------------
type User struct {
	Name string
	Age  int
}

// -------------------------------------------------------------
// 2. METHOD (Fungsi yang Menempel pada Struct)
// Perhatikan: (u *User) di depan nama fungsi.
// Ini artinya: Fungsi SayHello() milik struct User!
// -------------------------------------------------------------
func (u *User) SayHello() string {
	return fmt.Sprintf("Halo, nama saya %s dan umur saya %d tahun.", u.Name, u.Age)
}

func main() {
	fmt.Println("==================================================")
	fmt.Println("📚 BELAJAR DASAR GO: STRUCT, POINTER, & METHOD")
	fmt.Println("==================================================")

	// CONTOH A: Membuat data biasa (Value)
	userA := User{Name: "Budi", Age: 20}
	fmt.Println("Data User A:", userA)

	// CONTOH B: Menggunakan Tanda Dan (&) -> POINTER
	// Tanda & artinya: "Ambil alamat memori dari objek ini"
	// Di gRPC, kita hampir SELALU memakai & agar data tidak di-copy berulang kali.
	userB := &User{Name: "Kevin", Age: 25}
	fmt.Println("Alamat Memori User B:", userB)
	fmt.Println("Nama User B         :", userB.Name)

	// CONTOH C: Memanggil Method yang menempel
	pesan := userB.SayHello()
	fmt.Println("\nHasil Panggilan Method:")
	fmt.Println(pesan)
}
