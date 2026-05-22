/**
 * ============================================================
 *  SMART HOME MONITORING SYSTEM
 *  Implementasi IoT untuk Monitoring Keamanan Rumah Real-Time
 *  dengan ESP32 dan Dashboard ThingsBoard
 * ============================================================
 *  Universitas Palangka Raya
 *  Jurusan Teknik Informatika - Fakultas Teknik
 *
 *  Kelompok:
 *    - Rusma Watie WN              (213030503139)
 *    - Ariel Ebenia Ezeria         (213030503140)
 *    - M. Ferdy Ridwan Syahbana    (193130503079)
 * ============================================================
 *  ARSITEKTUR 3-LAYER IoT:
 *    [ Perception ]  DHT22 + PIR HC-SR501 + MQ-2
 *           │
 *    [ Network    ]  ESP32 → WiFi → MQTT → ThingsBoard Broker
 *           │
 *    [ Application]  Dashboard ThingsBoard (Web Real-Time)
 * ============================================================
 *  HARDWARE WIRING:
 *    ESP32 DevKit V1          Komponen
 *    ─────────────────────────────────────────────
 *    GPIO 4   ───────────►   DHT22 (Data)
 *    GPIO 14  ───────────►   PIR HC-SR501 (OUT)
 *    GPIO 34  ───────────►   MQ-2 (AO / Analog Output)
 *    GPIO 26  ───────────►   LED Merah (Alarm)   + R 220Ω
 *    GPIO 27  ───────────►   LED Hijau (Status)  + R 220Ω
 *    GPIO 25  ───────────►   Buzzer Aktif (+)
 *    3V3      ───────────►   VCC sensor (DHT22, PIR)
 *    5V (VIN) ───────────►   VCC MQ-2
 *    GND      ───────────►   GND bersama
 * ============================================================
 *  PROTOKOL  : MQTT over WiFi (Publish / Subscribe)
 *  BROKER    : ThingsBoard Demo (demo.thingsboard.io:1883)
 *              -> Dapat diganti broker self-hosted / HiveMQ
 *  AUTHENT.  : Device Access Token (username MQTT)
 * ============================================================
 *  LIBRARY DEPENDENCY (Arduino IDE Library Manager):
 *    1. DHT sensor library          (Adafruit)
 *    2. Adafruit Unified Sensor     (Adafruit)
 *    3. PubSubClient                (Nick O'Leary)
 *    4. ArduinoJson v6.x            (Benoit Blanchon)
 * ============================================================
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <ArduinoJson.h>

// ╔════════════════════════════════════════════════════════════╗
// ║ 1. KONFIGURASI JARINGAN & BROKER                           ║
// ╚════════════════════════════════════════════════════════════╝

// ── WiFi (Sesuaikan dengan jaringan rumah/hotspot) ───────────
#define WIFI_SSID       "NAMA_WIFI_KAMU"
#define WIFI_PASSWORD   "PASSWORD_WIFI"

// ── ThingsBoard MQTT Broker ──────────────────────────────────
// Untuk testing: gunakan demo.thingsboard.io (gratis, public)
// Untuk produksi: ganti dengan host self-hosted ThingsBoard
#define TB_SERVER       "demo.thingsboard.io"
#define TB_PORT         1883

// Token didapat dari: Device → Manage Credentials → Access Token
#define TB_TOKEN        "GANTI_DENGAN_ACCESS_TOKEN_DEVICE_THINGSBOARD"

#define MQTT_CLIENT_ID  "smarthome_esp32_001"   // harus unik per device

// ── ThingsBoard MQTT API Topics ──────────────────────────────
// Dokumentasi: https://thingsboard.io/docs/reference/mqtt-api/
#define TOPIC_TELEMETRY    "v1/devices/me/telemetry"
#define TOPIC_ATTRIBUTES   "v1/devices/me/attributes"
#define TOPIC_RPC_REQUEST  "v1/devices/me/rpc/request/+"
#define TOPIC_RPC_RESPONSE "v1/devices/me/rpc/response/"

// ╔════════════════════════════════════════════════════════════╗
// ║ 2. PIN DEFINITION                                          ║
// ╚════════════════════════════════════════════════════════════╝
#define PIN_DHT         4
#define PIN_PIR         14
#define PIN_MQ2         34    // hanya GPIO 32-39 yang bisa ADC saat WiFi aktif
#define PIN_LED_RED     26
#define PIN_LED_GREEN   27
#define PIN_BUZZER      25

#define DHT_TYPE        DHT22

// ╔════════════════════════════════════════════════════════════╗
// ║ 3. THRESHOLD & INTERVAL                                    ║
// ╚════════════════════════════════════════════════════════════╝
#define TEMP_MAX            35.0     // °C — batas atas suhu
#define HUM_MAX             80.0     // % — batas atas kelembaban
#define SMOKE_THRESHOLD     2000     // nilai ADC 0-4095, ambang asap

#define INTERVAL_TELEMETRY  5000     // kirim telemetry tiap 5 detik
#define INTERVAL_MOTION     500      // cek PIR tiap 0.5 detik
#define INTERVAL_RECONNECT  5000     // jeda retry koneksi
#define ALERT_COOLDOWN      10000    // jeda minimal antar alarm (10 detik)

// ╔════════════════════════════════════════════════════════════╗
// ║ 4. OBJEK & VARIABEL GLOBAL                                 ║
// ╚════════════════════════════════════════════════════════════╝
DHT          dht(PIN_DHT, DHT_TYPE);
WiFiClient   wifiClient;
PubSubClient mqtt(wifiClient);

unsigned long lastTelemetryTime  = 0;
unsigned long lastMotionTime     = 0;
unsigned long lastReconnectTime  = 0;
unsigned long lastAlertTime      = 0;

bool alertActive    = false;
bool lastMotionState = false;

// ╔════════════════════════════════════════════════════════════╗
// ║ 5. DEKLARASI FUNGSI                                        ║
// ╚════════════════════════════════════════════════════════════╝
void setupWiFi();
bool reconnectMQTT();
void publishTelemetry();
void checkMotion();
void triggerAlert(const char* jenis, const char* nilai);
void resetAlert();
void mqttCallback(char* topic, byte* payload, unsigned int length);
void publishAttributes();

// ╔════════════════════════════════════════════════════════════╗
// ║ 6. SETUP                                                   ║
// ╚════════════════════════════════════════════════════════════╝
void setup() {
  Serial.begin(115200);
  delay(500);

  Serial.println();
  Serial.println(F("================================================"));
  Serial.println(F("  SMART HOME MONITORING SYSTEM (ESP32)"));
  Serial.println(F("  Dashboard: ThingsBoard via MQTT"));
  Serial.println(F("================================================"));

  // ── Inisialisasi pin I/O ───────────────────────────────────
  pinMode(PIN_PIR,        INPUT);
  pinMode(PIN_LED_RED,    OUTPUT);
  pinMode(PIN_LED_GREEN,  OUTPUT);
  pinMode(PIN_BUZZER,     OUTPUT);

  digitalWrite(PIN_LED_RED,   LOW);
  digitalWrite(PIN_LED_GREEN, LOW);
  digitalWrite(PIN_BUZZER,    LOW);

  // ── Self-test indikator (LED + buzzer berkedip) ────────────
  Serial.println(F("[BOOT]  Self-test indikator..."));
  digitalWrite(PIN_LED_RED,   HIGH); delay(250);
  digitalWrite(PIN_LED_RED,   LOW);
  digitalWrite(PIN_LED_GREEN, HIGH); delay(250);
  digitalWrite(PIN_LED_GREEN, LOW);
  tone(PIN_BUZZER, 1500, 150);       delay(250);

  // ── Inisialisasi sensor DHT22 ──────────────────────────────
  dht.begin();
  Serial.println(F("[DHT22] Inisialisasi OK"));

  // ── Resolusi ADC untuk MQ-2 (12 bit, 0-4095) ───────────────
  analogReadResolution(12);
  Serial.println(F("[MQ-2]  ADC 12-bit aktif (0-4095)"));

  // ── Hangatkan PIR 30 detik pertama (sesuai datasheet) ──────
  Serial.println(F("[PIR]   Warming up 5 detik..."));
  delay(5000);

  // ── Koneksi WiFi & MQTT ────────────────────────────────────
  setupWiFi();

  mqtt.setServer(TB_SERVER, TB_PORT);
  mqtt.setKeepAlive(60);
  mqtt.setBufferSize(512);
  mqtt.setCallback(mqttCallback);

  reconnectMQTT();
  publishAttributes();   // kirim metadata device sekali di awal

  // ── Tandai sistem siap ─────────────────────────────────────
  digitalWrite(PIN_LED_GREEN, HIGH);
  Serial.println(F("[READY] Sistem siap - mulai monitoring."));
  Serial.println(F("------------------------------------------------"));
}

// ╔════════════════════════════════════════════════════════════╗
// ║ 7. MAIN LOOP                                               ║
// ╚════════════════════════════════════════════════════════════╝
void loop() {
  unsigned long now = millis();

  // (1) Jaga koneksi MQTT — reconnect non-blocking
  if (!mqtt.connected()) {
    if (now - lastReconnectTime >= INTERVAL_RECONNECT) {
      lastReconnectTime = now;
      reconnectMQTT();
    }
  } else {
    mqtt.loop();   // handle subscribe callback & keepalive
  }

  // (2) Polling sensor gerak (PIR) — cepat untuk respons instan
  if (now - lastMotionTime >= INTERVAL_MOTION) {
    lastMotionTime = now;
    checkMotion();
  }

  // (3) Kirim telemetry suhu/kelembaban/asap tiap 5 detik
  if (now - lastTelemetryTime >= INTERVAL_TELEMETRY) {
    lastTelemetryTime = now;
    publishTelemetry();
    Serial.println(F("------------------------------------------------"));
  }
}

// ╔════════════════════════════════════════════════════════════╗
// ║ 8. NETWORK: WiFi & MQTT                                    ║
// ╚════════════════════════════════════════════════════════════╝

// ─── Koneksi WiFi (blocking saat boot) ────────────────────────
void setupWiFi() {
  Serial.printf("[WiFi]  Menghubungkan ke SSID: %s", WIFI_SSID);

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  int attempt = 0;
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
    if (++attempt > 40) {
      Serial.println(F("\n[WiFi]  Gagal terhubung! Restart ESP..."));
      ESP.restart();
    }
  }

  Serial.println();
  Serial.printf("[WiFi]  Terhubung! IP: %s | RSSI: %d dBm\n",
                WiFi.localIP().toString().c_str(), WiFi.RSSI());
}

// ─── Koneksi/Re-koneksi MQTT ke ThingsBoard ───────────────────
bool reconnectMQTT() {
  Serial.print(F("[MQTT]  Connect ke ThingsBoard..."));

  // ThingsBoard menggunakan Access Token sebagai username (password kosong)
  if (mqtt.connect(MQTT_CLIENT_ID, TB_TOKEN, NULL)) {
    Serial.println(F(" OK"));

    // Subscribe RPC supaya dashboard bisa kirim perintah ke ESP32
    mqtt.subscribe(TOPIC_RPC_REQUEST);

    // Publish status online sebagai atribut
    mqtt.publish(TOPIC_ATTRIBUTES, "{\"status\":\"online\"}");
    return true;
  }

  Serial.printf(" GAGAL (rc=%d)\n", mqtt.state());
  return false;
}

// ─── Callback MQTT — handle RPC dari dashboard ────────────────
// Contoh dashboard kirim: { "method":"setAlarm", "params":true }
void mqttCallback(char* topic, byte* payload, unsigned int length) {
  Serial.printf("[MQTT]  RX [%s] ", topic);

  StaticJsonDocument<200> doc;
  DeserializationError err = deserializeJson(doc, payload, length);
  if (err) {
    Serial.println(F("payload bukan JSON valid"));
    return;
  }

  const char* method = doc["method"];
  if (method == nullptr) return;

  // Ambil request ID dari topik: v1/devices/me/rpc/request/{id}
  String topicStr = String(topic);
  String reqId    = topicStr.substring(topicStr.lastIndexOf('/') + 1);
  String respTopic = String(TOPIC_RPC_RESPONSE) + reqId;

  // ── RPC: setAlarm (true/false) ───────────────────────────────
  if (strcmp(method, "setAlarm") == 0) {
    bool on = doc["params"] | false;
    if (on) triggerAlert("MANUAL_RPC", "dashboard");
    else    resetAlert();
    mqtt.publish(respTopic.c_str(), on ? "{\"alarm\":true}" : "{\"alarm\":false}");
    Serial.printf("setAlarm=%s\n", on ? "ON" : "OFF");
  }
  // ── RPC: getStatus (ping balik kondisi terkini) ──────────────
  else if (strcmp(method, "getStatus") == 0) {
    char buf[96];
    snprintf(buf, sizeof(buf),
             "{\"alert\":%s,\"uptime\":%lu}",
             alertActive ? "true" : "false", millis() / 1000);
    mqtt.publish(respTopic.c_str(), buf);
    Serial.println(F("getStatus -> dijawab"));
  }
  else {
    Serial.printf("method tidak dikenal: %s\n", method);
  }
}

// ╔════════════════════════════════════════════════════════════╗
// ║ 9. SENSOR: BACA & KIRIM TELEMETRY                          ║
// ╚════════════════════════════════════════════════════════════╝

// ─── Publish seluruh telemetry sebagai 1 paket JSON ──────────
// ThingsBoard akan otomatis membuat time-series untuk tiap key.
void publishTelemetry() {
  float suhu       = dht.readTemperature();
  float kelembaban = dht.readHumidity();
  int   smokeVal   = analogRead(PIN_MQ2);
  int   motion     = digitalRead(PIN_PIR);

  // Validasi pembacaan DHT22 (sering NaN saat kabel longgar)
  if (isnan(suhu) || isnan(kelembaban)) {
    Serial.println(F("[DHT22] Pembacaan gagal — skip telemetry"));
    return;
  }

  // ── Persentase asap (0-100%) untuk visualisasi lebih bersih
  float smokePercent = (smokeVal / 4095.0) * 100.0;

  // ── Susun payload JSON sesuai format ThingsBoard ────────────
  StaticJsonDocument<256> doc;
  doc["temperature"]  = round(suhu * 10) / 10.0;
  doc["humidity"]     = round(kelembaban * 10) / 10.0;
  doc["smoke_raw"]    = smokeVal;
  doc["smoke_level"]  = round(smokePercent * 10) / 10.0;
  doc["motion"]       = (motion == HIGH);
  doc["alert"]        = alertActive;
  doc["rssi"]         = WiFi.RSSI();

  char payload[256];
  size_t n = serializeJson(doc, payload);

  if (mqtt.publish(TOPIC_TELEMETRY, payload, n)) {
    Serial.printf("[TX]    %s\n", payload);
  } else {
    Serial.println(F("[TX]    publish telemetry GAGAL"));
  }

  // ── Evaluasi threshold ─────────────────────────────────────
  bool anomali = false;
  if (suhu       >= TEMP_MAX)         { triggerAlert("SUHU_TINGGI",    String(suhu, 1).c_str());        anomali = true; }
  if (kelembaban >= HUM_MAX)          { triggerAlert("KELEMBABAN",     String(kelembaban, 1).c_str()); anomali = true; }
  if (smokeVal   >= SMOKE_THRESHOLD)  { triggerAlert("ASAP_GAS",       String(smokeVal).c_str());      anomali = true; }

  // Reset alarm bila semua parameter kembali normal & cooldown lewat
  if (!anomali && alertActive && (millis() - lastAlertTime) > ALERT_COOLDOWN) {
    resetAlert();
  }
}

// ─── Cek PIR — dipanggil 2x per detik untuk respons instan ────
// Edge-trigger: hanya kirim event saat transisi LOW → HIGH.
void checkMotion() {
  bool motion = (digitalRead(PIN_PIR) == HIGH);

  if (motion && !lastMotionState) {
    Serial.println(F("[PIR]   ⚠ Gerakan terdeteksi!"));

    // Kirim event terpisah (selain agregat di publishTelemetry)
    mqtt.publish(TOPIC_TELEMETRY, "{\"motion_event\":true}");
    triggerAlert("GERAKAN", "terdeteksi");
  }

  lastMotionState = motion;
}

// ╔════════════════════════════════════════════════════════════╗
// ║ 10. AKTUATOR: ALARM (LED + BUZZER)                         ║
// ╚════════════════════════════════════════════════════════════╝

// ─── Aktifkan alarm + kirim notifikasi ke dashboard ──────────
void triggerAlert(const char* jenis, const char* nilai) {
  // Cooldown supaya tidak spam alarm tiap iterasi loop
  if (alertActive && (millis() - lastAlertTime) < ALERT_COOLDOWN) return;

  lastAlertTime = millis();
  alertActive   = true;

  // Indikator fisik
  digitalWrite(PIN_LED_RED,   HIGH);
  digitalWrite(PIN_LED_GREEN, LOW);
  tone(PIN_BUZZER, 2000, 800);

  // Kirim alert sebagai telemetry khusus (bisa dipakai untuk rule engine)
  StaticJsonDocument<128> doc;
  doc["alert_type"]  = jenis;
  doc["alert_value"] = nilai;
  doc["alert_state"] = true;

  char payload[128];
  size_t n = serializeJson(doc, payload);
  mqtt.publish(TOPIC_TELEMETRY, payload, n);

  Serial.printf("[ALERT] %s = %s\n", jenis, nilai);
}

// ─── Reset alarm ke kondisi normal ────────────────────────────
void resetAlert() {
  if (!alertActive) return;

  alertActive = false;
  digitalWrite(PIN_LED_RED,   LOW);
  digitalWrite(PIN_LED_GREEN, HIGH);
  noTone(PIN_BUZZER);

  mqtt.publish(TOPIC_TELEMETRY, "{\"alert_state\":false}");
  Serial.println(F("[ALERT] Kondisi kembali NORMAL"));
}

// ╔════════════════════════════════════════════════════════════╗
// ║ 11. METADATA DEVICE (ATTRIBUTES)                           ║
// ╚════════════════════════════════════════════════════════════╝

// Dikirim sekali saat boot — muncul di tab "Attributes" device
// pada ThingsBoard. Berguna untuk identifikasi & dokumentasi.
void publishAttributes() {
  StaticJsonDocument<256> doc;
  doc["firmware_version"] = "1.0.0";
  doc["device_model"]     = "ESP32 DevKit V1";
  doc["sensors"]          = "DHT22, PIR HC-SR501, MQ-2";
  doc["mac_address"]      = WiFi.macAddress();
  doc["ip_address"]       = WiFi.localIP().toString();
  doc["temp_threshold"]   = TEMP_MAX;
  doc["smoke_threshold"]  = SMOKE_THRESHOLD;

  char payload[256];
  size_t n = serializeJson(doc, payload);
  mqtt.publish(TOPIC_ATTRIBUTES, payload, n);

  Serial.println(F("[ATTR]  Metadata device terkirim"));
}
