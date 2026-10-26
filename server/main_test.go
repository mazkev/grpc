package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

// Test 1: Menguji apakah katalog produk dasar tersedia di memori
func TestProductDatabase(t *testing.T) {
	expectedID := "E01"
	product, exists := productDB[expectedID]

	if !exists {
		t.Fatalf("❌ Produk dengan ID '%s' seharusnya ada di productDB", expectedID)
	}

	if product.GetName() != "Laptop Gaming ROG" {
		t.Errorf("❌ Nama produk salah. Harapan: 'Laptop Gaming ROG', Dapat: '%s'", product.GetName())
	}
}

// Test 2: Menguji Satpam Interceptor ketika tamu tidak membawa token (Harus Ditolak)
func TestAuthorize_NoToken(t *testing.T) {
	ctx := context.Background() // Context kosong tanpa metadata

	err := authorize(ctx)
	if err == nil {
		t.Error("❌ Satpam seharusnya menolak akses jika tidak ada metadata token!")
	}
}


// Test 3: Menguji Satpam Interceptor ketika tamu membawa token yang benar (Harus Lolos)
func TestAuthorize_ValidToken(t *testing.T) {
	// Buat context dengan tiket metadata rahasia yang sah
	md := metadata.New(map[string]string{
		"authorization": "token-rahasia-123",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := authorize(ctx)
	if err != nil {
		t.Errorf("❌ Satpam seharusnya mengizinkan token yang sah, tapi muncul error: %v", err)
	}
}
