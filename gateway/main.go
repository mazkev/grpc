package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	pb "grpc-learning/gen/product"
	"grpc-learning/pkg/tracer"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Custom Header Matcher: Meneruskan header HTTP Authorization ke gRPC Metadata
func customHeaderMatcher(key string) (string, bool) {
	switch strings.ToLower(key) {
	case "authorization":
		return "authorization", true
	default:
		return runtime.DefaultHeaderMatcher(key)
	}
}

// Middleware CORS agar API REST dapat dipanggil dari Web Browser mana pun (Frontend React/Vue)
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	ctx := context.Background()

	// 1. Inisialisasi OpenTelemetry Tracing untuk Reverse-Proxy Gateway
	tp, err := tracer.InitTracer(ctx, "grpc-gateway-proxy")
	if err == nil {
		defer tp.Shutdown(ctx)
	}

	grpcServerEndpoint := os.Getenv("GRPC_SERVER_ENDPOINT")
	if grpcServerEndpoint == "" {
		grpcServerEndpoint = "localhost:50051"
	}

	gatewayPort := os.Getenv("GATEWAY_PORT")
	if gatewayPort == "" {
		gatewayPort = "8080"
	}

	// 2. Buat Multiplexer gRPC-Gateway dengan Header Matcher khusus
	gwmux := runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(customHeaderMatcher),
	)

	// 3. Dial Options ke gRPC Backend (dengan OTel tracing client handler)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}

	// 4. Daftarkan Service Handler dari Endpoint gRPC
	err = pb.RegisterProductServiceHandlerFromEndpoint(ctx, gwmux, grpcServerEndpoint, opts)
	if err != nil {
		log.Fatalf("❌ Gagal mendaftarkan gRPC Gateway handler: %v", err)
	}

	// 5. Root HTTP Router untuk melayani REST API & Swagger OpenAPI JSON
	mux := http.NewServeMux()

	// Endpoint Dokumentasi Swagger OpenAPI JSON
	mux.HandleFunc("/swagger.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		http.ServeFile(w, r, "gen/openapiv2/product.swagger.json")
	})

	// Endpoint Visual Swagger UI Interaktif di Browser
	mux.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(swaggerUIHTML))
	})
	mux.HandleFunc("/docs/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(swaggerUIHTML))
	})
	mux.HandleFunc("/swagger", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/docs", http.StatusMovedPermanently)
	})

	// Endpoint Utama: Teruskan seluruh request REST ke gRPC Gateway Mux
	mux.Handle("/", gwmux)

	// Bungkus dengan CORS dan OpenTelemetry HTTP Handler
	handlerWithCORS := corsMiddleware(mux)
	tracedHandler := otelhttp.NewHandler(handlerWithCORS, "HTTP REST Gateway")

	fmt.Println("==================================================")
	fmt.Printf("🌐 gRPC-GATEWAY (REST HTTP/JSON PROXY) AKTIF\n")
	fmt.Printf("   HTTP Port       : http://localhost:%s\n", gatewayPort)
	fmt.Printf("   Backend gRPC    : %s\n", grpcServerEndpoint)
	fmt.Printf("   Interactive UI  : http://localhost:%s/docs 🚀\n", gatewayPort)
	fmt.Printf("   Swagger JSON    : http://localhost:%s/swagger.json\n", gatewayPort)
	fmt.Printf("   Contoh Endpoint : GET http://localhost:%s/v1/products/E01\n", gatewayPort)
	fmt.Printf("                   : GET http://localhost:%s/v1/products\n", gatewayPort)
	fmt.Println("==================================================")

	if err := http.ListenAndServe(":"+gatewayPort, tracedHandler); err != nil {
		log.Fatalf("❌ Gateway HTTP Server terhenti: %v", err)
	}
}

// Template HTML Swagger UI Interaktif dengan CDN Resmi & Dark/Modern Design
const swaggerUIHTML = `<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Enterprise gRPC-Gateway API Portal</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui.css" />
  <link rel="icon" type="image/png" href="https://unpkg.com/swagger-ui-dist@5.11.0/favicon-32x32.png" />
  <style>
    body {
      margin: 0;
      background: #0f172a;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    .topbar { display: none !important; }
    .custom-header {
      background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
      border-bottom: 1px solid #334155;
      padding: 24px 32px;
      color: white;
    }
    .custom-header h1 {
      margin: 0 0 8px 0;
      font-size: 26px;
      font-weight: 700;
      color: #38bdf8;
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .custom-header p {
      margin: 0;
      color: #94a3b8;
      font-size: 14px;
    }
    .swagger-ui {
      max-width: 1200px;
      margin: 0 auto;
      padding: 20px 24px 60px;
    }
    .swagger-ui .info {
      margin: 20px 0;
    }
    .swagger-ui .scheme-container {
      background: #1e293b;
      border: 1px solid #334155;
      border-radius: 12px;
      padding: 16px 24px;
      box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.4);
      margin-bottom: 24px;
    }
    .swagger-ui .opblock {
      border-radius: 10px;
      margin-bottom: 16px;
      border: 1px solid rgba(255, 255, 255, 0.08);
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    }
    .swagger-ui .btn.authorize {
      border-color: #38bdf8;
      color: #38bdf8;
    }
    .swagger-ui .btn.authorize svg {
      fill: #38bdf8;
    }
  </style>
</head>
<body>
  <div class="custom-header">
    <h1>⚡ Enterprise gRPC-Gateway Developer Portal</h1>
    <p>RESTful HTTP/JSON Interface yang otomatis di-transcode ke Backend Core gRPC (:50051) dengan Redis Cache & Distributed Tracing</p>
  </div>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-bundle.js" charset="UTF-8"></script>
  <script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
  <script>
    window.onload = function() {
      window.ui = SwaggerUIBundle({
        url: "/swagger.json",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout"
      });
    };
  </script>
</body>
</html>`

