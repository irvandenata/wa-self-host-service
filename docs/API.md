# WA Queue Service API

Dokumentasi ini menjelaskan API HTTP untuk mengelola koneksi WhatsApp, memasukkan pesan ke antrean, dan memantau status pengiriman.

## Base URL

Development:

```text
http://localhost:8080
```

Semua response JSON menggunakan:

```http
Content-Type: application/json; charset=utf-8
```

Spesifikasi OpenAPI dan Swagger UI tersedia saat service berjalan:

- Swagger UI: `GET /docs`
- OpenAPI: `GET /openapi.yaml`
- Postman Collection: `postman/wa-service.postman_collection.json`

## Autentikasi

### API key

Semua endpoint `/v1/*` memerlukan header:

```http
X-API-Key: <api-key>
```

Terdapat dua jenis key:

| Key | Environment variable | Akses |
| --- | --- | --- |
| API key biasa | `API_KEY` | Pesan, job, dan statistik queue |
| Admin API key | `ADMIN_API_KEY` | Semua akses API key biasa, drain queue, dan seluruh endpoint WhatsApp |

Endpoint berikut wajib menggunakan `ADMIN_API_KEY`:

- `DELETE /v1/queue/drain`
- `GET /v1/wa/status`
- `POST /v1/wa/connect`
- `GET /v1/wa/qr`
- `GET /v1/wa/qr.png`
- `POST /v1/wa/logout`

### HMAC signature

Jika `API_HMAC_SECRET` tidak kosong, setiap request `/v1/*` juga wajib mengirim:

```http
X-Timestamp: <unix-time-milliseconds>
X-Signature: <hex-hmac-sha256>
```

Signature dihitung dari byte berikut:

```text
<timestamp>\n<raw-request-body>
```

Rumusnya:

```text
hex(HMAC-SHA256(API_HMAC_SECRET, timestamp + "\n" + rawBody))
```

Ketentuan:

- Timestamp menggunakan Unix time dalam milidetik.
- Selisih waktu maksimum terhadap server adalah lima menit.
- Untuk request tanpa body, `rawBody` adalah string kosong.
- Body yang ditandatangani harus identik dengan body yang dikirim, termasuk spasi dan line break.

Contoh helper Bash:

```bash
BASE_URL="http://localhost:8080"
API_KEY_VALUE="your-api-key"
HMAC_SECRET="your-hmac-secret"

signed_request() {
  method="$1"
  path="$2"
  body="${3:-}"
  timestamp=$(date +%s000)
  signature=$(printf '%s\n%s' "$timestamp" "$body" \
    | openssl dgst -sha256 -hmac "$HMAC_SECRET" -hex \
    | awk '{print $NF}')

  if [ -n "$body" ]; then
    curl --silent --show-error \
      --request "$method" "$BASE_URL$path" \
      --header "X-API-Key: $API_KEY_VALUE" \
      --header "X-Timestamp: $timestamp" \
      --header "X-Signature: $signature" \
      --header "Content-Type: application/json" \
      --data-raw "$body"
  else
    curl --silent --show-error \
      --request "$method" "$BASE_URL$path" \
      --header "X-API-Key: $API_KEY_VALUE" \
      --header "X-Timestamp: $timestamp" \
      --header "X-Signature: $signature"
  fi
}
```

Untuk endpoint admin, isi `API_KEY_VALUE` dengan nilai `ADMIN_API_KEY`.

## Endpoint publik

Endpoint publik tidak memerlukan API key atau HMAC signature.

### Health check

```http
GET /healthz
```

Response `200 OK`:

```json
{
  "ok": true
}
```

Endpoint ini hanya menandakan proses service sedang hidup.

### Readiness check

```http
GET /readyz
```

Response ketika Redis tersedia:

```http
200 OK
```

```json
{
  "ok": true
}
```

Response ketika Redis tidak tersedia:

```http
503 Service Unavailable
```

```json
{
  "ok": false
}
```

### Metrics

```http
GET /metrics
```

Response menggunakan format teks Prometheus:

```text
wa_queue_waiting 2
wa_queue_active 1
wa_queue_completed 5
wa_queue_failed 0
wa_queue_delayed 0
```

## Messages

### Mengirim satu pesan

```http
POST /v1/messages
```

Headers:

```http
X-API-Key: <API_KEY atau ADMIN_API_KEY>
Content-Type: application/json
Idempotency-Key: <opsional>
```

Request body:

```json
{
  "receiver": "0812-3456-7890",
  "message": "Halo dari WA Queue Service",
  "enableTypingEffect": false,
  "typingSpeedMs": 50
}
```

| Field | Tipe | Wajib | Keterangan |
| --- | --- | --- | --- |
| `receiver` | string | Ya | Nomor WhatsApp. Nomor lokal berawalan `0` otomatis diubah menjadi kode negara `62`. |
| `message` | string | Ya | Isi pesan dan tidak boleh hanya whitespace. |
| `enableTypingEffect` | boolean | Tidak | Diterima untuk kompatibilitas kontrak; saat ini belum memengaruhi proses pengiriman. |
| `typingSpeedMs` | integer | Tidak | Jika dikirim harus lebih besar dari nol; saat ini belum memengaruhi proses pengiriman. |

Format nomor yang diterima dapat mengandung awalan `+`, spasi, tanda hubung, dan tanda kurung. Contoh berikut dinormalisasi menjadi `628123456789`:

- `08123456789`
- `+628123456789`
- `62 812-3456-789`

Response `200 OK`:

```json
{
  "enqueued": true,
  "jobId": "99af2fdf82da5bb6f18790ef92f22f72"
}
```

Contoh dengan helper HMAC:

```bash
signed_request POST /v1/messages \
  '{"receiver":"08123456789","message":"Halo dari API"}'
```

#### Idempotency

Header `Idempotency-Key` mencegah pembuatan job duplikat untuk kombinasi key, receiver, dan message yang sama.

- Key dinormalisasi menjadi lowercase dan whitespace di kedua ujungnya dihapus.
- Masa berlaku idempotency adalah satu jam.
- Request duplikat mengembalikan `jobId` yang sama.
- Gunakan key baru untuk pesan baru yang memang ingin dikirim ulang.

### Mengirim pesan bulk

```http
POST /v1/messages/bulk
```

Request body:

```json
{
  "receivers": [
    "081234567890",
    "6281234567891"
  ],
  "message": "Pesan yang sama untuk beberapa penerima",
  "enableTypingEffect": false,
  "typingSpeedMs": 50
}
```

Response `200 OK`:

```json
{
  "enqueued": true,
  "jobs": [
    "e4fcdf1ec84822c9bbc76ac841c8aef4",
    "5af747551694515903b236b989efcb13"
  ],
  "count": 2
}
```

Jika salah satu nomor tidak valid atau Redis gagal saat proses berjalan, request berhenti. Job yang sudah dibuat sebelum kegagalan tidak dibatalkan otomatis.

## Jobs

### Mendapatkan status job

```http
GET /v1/jobs/{id}
```

Contoh:

```bash
signed_request GET /v1/jobs/99af2fdf82da5bb6f18790ef92f22f72
```

Response job menunggu:

```json
{
  "id": "99af2fdf82da5bb6f18790ef92f22f72",
  "name": "send",
  "attemptsMade": 0,
  "failedReason": "",
  "returnValue": null,
  "state": "waiting"
}
```

Response job selesai:

```json
{
  "id": "99af2fdf82da5bb6f18790ef92f22f72",
  "name": "send",
  "attemptsMade": 0,
  "failedReason": "",
  "returnValue": {
    "success": true,
    "message": "Message sent"
  },
  "state": "completed"
}
```

Kemungkinan nilai `state`:

| State | Keterangan |
| --- | --- |
| `waiting` | Menunggu diproses worker. |
| `active` | Sedang diproses. |
| `completed` | Pesan berhasil dikirim. |
| `failed` | Pengiriman gagal setelah batas percobaan. |

Job `completed` dan `failed` disimpan selama satu jam. Setelah itu endpoint dapat mengembalikan `404 not_found`.

## Queue

### Statistik queue

```http
GET /v1/queue/stats
```

Response `200 OK`:

```json
{
  "waiting": 2,
  "active": 1,
  "completed": 5,
  "failed": 0,
  "delayed": 0
}
```

### Menghapus job yang menunggu

```http
DELETE /v1/queue/drain
```

Endpoint ini wajib menggunakan `ADMIN_API_KEY`. Hanya job berstatus `waiting` yang dihapus.

Response `200 OK`:

```json
{
  "drained": true
}
```

## WhatsApp

Semua endpoint pada bagian ini wajib menggunakan `ADMIN_API_KEY`.

### Alur koneksi awal

1. Panggil `POST /v1/wa/connect`.
2. Tunggu sampai status berubah menjadi `pairing`.
3. Segera ambil gambar dari `GET /v1/wa/qr.png`.
4. Pada aplikasi WhatsApp, buka **Settings → Linked Devices → Link a Device**.
5. Pindai PNG secara langsung. Jangan mengubah raw payload melalui layanan QR pihak ketiga.
6. Poll `GET /v1/wa/status` sampai status menjadi `connected`.

QR berubah dan kedaluwarsa dalam waktu singkat. Jika mendapat `timeout` atau `qr_unavailable`, mulai lagi dari endpoint connect.

Setelah pairing berhasil, session disimpan di lokasi `WA_SESSION_DB`. Service akan mencoba menghubungkan ulang session tersebut ketika dijalankan kembali.

### Status koneksi

```http
GET /v1/wa/status
```

Response sebelum terhubung:

```json
{
  "status": "disconnected",
  "jid": ""
}
```

Response setelah terhubung:

```json
{
  "status": "connected",
  "jid": "628123456789:12@s.whatsapp.net"
}
```

Jika proses terakhir gagal, field `error` ikut dikembalikan:

```json
{
  "status": "disconnected",
  "jid": "",
  "error": "timeout"
}
```

Nilai status yang umum:

| Status | Keterangan |
| --- | --- |
| `disconnected` | Tidak terhubung. |
| `connecting` | Sedang memulai koneksi. |
| `pairing` | Menunggu QR dipindai. |
| `connected` | Terhubung dan siap mengirim. |
| `logged_out` | Session telah di-logout. |

### Memulai koneksi atau pairing

```http
POST /v1/wa/connect
```

Request ini tidak memiliki body.

Response session baru:

```json
{
  "status": "pairing",
  "jid": ""
}
```

Response session tersimpan:

```json
{
  "status": "connecting",
  "jid": "628123456789:12@s.whatsapp.net"
}
```

Endpoint bersifat non-blocking. Gunakan endpoint status untuk mengetahui hasil akhirnya.

### Mendapatkan raw QR payload

```http
GET /v1/wa/qr
```

Response ketika QR tersedia:

```json
{
  "status": "pairing",
  "qr": "<raw-pairing-payload>"
}
```

Raw payload bersifat sensitif. Untuk proses pairing, gunakan endpoint PNG agar byte payload tidak berubah saat dikonversi.

### Mendapatkan QR sebagai PNG

```http
GET /v1/wa/qr.png
```

Response sukses:

```http
200 OK
Content-Type: image/png
Cache-Control: no-store
```

Response ketika QR belum tersedia atau sudah kedaluwarsa:

```http
409 Conflict
```

```json
{
  "status": "disconnected",
  "error": "qr_unavailable"
}
```

Contoh menyimpan PNG dengan HMAC:

```bash
timestamp=$(date +%s000)
signature=$(printf '%s\n' "$timestamp" \
  | openssl dgst -sha256 -hmac "$HMAC_SECRET" -hex \
  | awk '{print $NF}')

curl --silent --show-error \
  "$BASE_URL/v1/wa/qr.png" \
  --header "X-API-Key: $ADMIN_API_KEY_VALUE" \
  --header "X-Timestamp: $timestamp" \
  --header "X-Signature: $signature" \
  --output whatsapp-qr.png
```

Buka `whatsapp-qr.png` dan segera pindai dari aplikasi WhatsApp.

### Logout

```http
POST /v1/wa/logout
```

Request ini mengeluarkan device dari akun WhatsApp dan menghapus session aktif dari sisi WhatsApp.

Response `200 OK`:

```json
{
  "status": "logged_out"
}
```

Untuk terhubung kembali setelah logout, lakukan pairing QR baru.

## Error response

Format umum:

```json
{
  "error": "error_code"
}
```

Payload tidak valid dapat menyertakan detail:

```json
{
  "error": "invalid_payload",
  "details": [
    {
      "message": "receiver and message are required"
    }
  ]
}
```

| HTTP status | Error | Keterangan |
| --- | --- | --- |
| `400` | `invalid_payload` | JSON atau field request tidak valid. |
| `401` | `unauthorized` | `X-API-Key` kosong atau salah. |
| `401` | `invalid_signature` | Timestamp atau HMAC signature tidak valid. |
| `403` | `forbidden` | Endpoint admin dipanggil menggunakan API key biasa. |
| `404` | `not_found` | Job tidak ditemukan atau telah kedaluwarsa. |
| `409` | `qr_unavailable` | QR belum tersedia atau sudah kedaluwarsa. |
| `500` | `connect_failed` | WhatsApp gagal memulai koneksi. |
| `500` | `logout_failed` | WhatsApp gagal melakukan logout. |
| `500` | `qr_encode_failed` | Server gagal menghasilkan PNG QR. |
| `503` | `queue_unavailable` | Redis atau queue tidak tersedia. |

## Batasan dan perilaku penting

- Ukuran request body maksimum adalah 1 MiB.
- Endpoint message hanya memasukkan pekerjaan ke queue; response `enqueued` tidak berarti pesan sudah terkirim.
- Gunakan endpoint job untuk mengetahui hasil pengiriman.
- Pengiriman hanya berhasil ketika status WhatsApp adalah `connected`.
- Worker melakukan retry sesuai `WA_MAX_ATTEMPTS`.
- Rate pengiriman diatur oleh `WA_QUEUE_MAX` dan `WA_QUEUE_DURATION_MS`.
- QR pairing adalah credential sementara. Jangan mencatat, membagikan, atau mengirim payload QR ke layanan pihak ketiga.
- CORS mengizinkan semua origin. Batasi akses pada reverse proxy jika service dipublikasikan ke internet.
