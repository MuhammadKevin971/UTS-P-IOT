from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import paho.mqtt.client as mqtt
import mysql.connector
import json
from datetime import datetime

# ==============================
# 1️⃣ Inisialisasi Flask
# ==============================
app = Flask(__name__)
CORS(app)

# ==============================
# 2️⃣ Koneksi ke Database (global)
# ==============================
try:
    # Pertama coba koneksi ke MySQL tanpa database
    temp_db = mysql.connector.connect(
        host="localhost",
        user="root",
        password=""  # Kosongkan password jika belum diset
    )
    temp_cursor = temp_db.cursor()
    
    # Buat database jika belum ada
    temp_cursor.execute("CREATE DATABASE IF NOT EXISTS sensor_db")
    temp_cursor.close()
    temp_db.close()
    
    # Sekarang koneksi ke database yang sudah dibuat
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",  # Kosongkan password jika belum diset
        database="sensor_db"
    )
    cursor = db.cursor()
    print("Berhasil terhubung ke database MySQL")
except mysql.connector.Error as err:
    print(f"Error: {err}")
    print("Pastikan:")
    print("1. MySQL server sudah terinstall dan berjalan")
    print("2. User dan password sesuai dengan konfigurasi MySQL Anda")
    exit(1)

# Buat tabel kalau belum ada
cursor.execute("""
CREATE TABLE IF NOT EXISTS data_sensor (
    id INT AUTO_INCREMENT PRIMARY KEY,
    suhu FLOAT,
    humidity FLOAT,
    lux FLOAT,
    relay_state VARCHAR(10),
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")
db.commit()

# ==============================
# 3️⃣ Variabel global untuk data terakhir
# ==============================
sensor_data = {
    "suhu": None,
    "humidity": None,
    "lux": None,
    "relay_state": None
}

# ==============================
# 4️⃣ Fungsi callback MQTT
# ==============================
def on_connect(client, userdata, flags, rc):
    print("Terhubung ke MQTT Broker dengan kode:", rc)
    client.subscribe("esp32/kevin/data")  # subscribe topic sensor

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print("Pesan MQTT diterima:", payload)

        # Parse JSON dari payload MQTT
        data = json.loads(payload)
        sensor_data["suhu"] = float(data.get("temperature", 0))
        sensor_data["humidity"] = float(data.get("humidity", 0))
        sensor_data["lux"] = float(data.get("lux", 0))
        sensor_data["relay_state"] = data.get("relay_state", "OFF")

        print("Data sensor diperbarui:", sensor_data)

        # ===== SIMPAN KE DATABASE =====
        try:
            # Cek dan reconnect jika perlu
            if not db.is_connected():
                db.reconnect()
            
            sql = """
                INSERT INTO data_sensor (suhu, humidity, lux, relay_state)
                VALUES (%s, %s, %s, %s)
            """
            val = (
                sensor_data["suhu"],
                sensor_data["humidity"],
                sensor_data["lux"],
                sensor_data["relay_state"]
            )
            cursor.execute(sql, val)
            db.commit()

            print("Data berhasil disimpan ke database sensor_db.data_sensor.")
        except mysql.connector.Error as db_err:
            print(f"Database error: {db_err}")
            # Coba reconnect dan insert ulang
            try:
                db.reconnect()
                cursor.execute(sql, val)
                db.commit()
                print("Data berhasil disimpan setelah reconnect.")
            except Exception as retry_err:
                print(f"Gagal insert data setelah retry: {retry_err}")

    except json.JSONDecodeError as e:
        print("Error parsing JSON:", e)
    except Exception as e:
        print("Error parsing/saving message:", e)

# ==============================
# 5️⃣ Setup MQTT Client
# ==============================
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

# ==============================
# 6️⃣ Route Flask
# ==============================
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/data", methods=["GET"])
def get_data():
    try:
        # Reconnect ke database jika connection hilang
        if not db.is_connected():
            db.reconnect()
        
        cur = db.cursor(dictionary=True)
        cur.execute("SELECT * FROM data_sensor ORDER BY timestamp DESC LIMIT 20")
        rows = cur.fetchall()

        if not rows:
            # Tetap return 200 dengan data kosong agar frontend bisa handle
            return jsonify({
                "message": "Belum ada data",
                "suhumax": 0,
                "suhumin": 0,
                "suhurata": 0,
                "humiditymax": 0,
                "humiditymin": 0,
                "humidityrata": 0,
                "records": []
            }), 200

        # Statistik sederhana
        suhu_values = [r["suhu"] for r in rows]
        hum_values = [r["humidity"] for r in rows]

        result = {
            "suhumax": max(suhu_values),
            "suhumin": min(suhu_values),
            "suhurata": round(sum(suhu_values) / len(suhu_values), 2),
            "humiditymax": max(hum_values),
            "humiditymin": min(hum_values),
            "humidityrata": round(sum(hum_values) / len(hum_values), 2),
            "records": rows
        }

        return jsonify(result)
    except Exception as e:
        print("Error mengambil data:", e)
        return jsonify({
            "error": str(e),
            "suhumax": 0,
            "suhumin": 0,
            "suhurata": 0,
            "humiditymax": 0,
            "humiditymin": 0,
            "humidityrata": 0,
            "records": []
        }), 200

@app.route("/relay", methods=["POST"])
def control_relay():
    try:
        data = request.get_json()
        state = data.get("state")

        if state not in ["ON", "OFF"]:
            return jsonify({"error": "State harus 'ON' atau 'OFF'"}), 400

        # Publish perintah ke MQTT
        mqtt_client.publish("esp32/relay", json.dumps({"relay": state}))
        print(f"Perintah relay dikirim ke MQTT: {state}")

        # Update status terakhir
        sensor_data["relay_state"] = state
        return jsonify({"status": f"Relay {state}"})
    except Exception as e:
        print("Error mengirim relay:", e)
        return jsonify({"error": str(e)}), 500

# ==============================
# 7️⃣ Jalankan Flask
# ==============================
if __name__ == "__main__":
    print("Menjalankan Flask Server di http://127.0.0.1:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)