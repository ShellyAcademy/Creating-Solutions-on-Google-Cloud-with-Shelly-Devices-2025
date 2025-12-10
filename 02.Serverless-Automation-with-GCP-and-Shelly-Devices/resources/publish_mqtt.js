let data = {
  "status": "connected"
}
let payload = JSON.stringify(data)

MQTT.publish("lab/test/connection", payload)