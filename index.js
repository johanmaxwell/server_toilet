require("dotenv").config();
const mqtt = require("mqtt");
const admin = require("firebase-admin");
const { Buffer } = require("buffer");

// Initialize Firebase
const serviceAccount = JSON.parse(
	Buffer.from(process.env.FIREBASE_CREDENTIALS, "base64").toString("utf8")
);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

// Connect to MQTT Broker
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL, {
	username: process.env.MQTT_USER,
	password: process.env.MQTT_PASS,
	clientId: "Local_Server",
});

mqttClient.on("connect", () => {
	console.log("Connected to MQTT Broker");
	mqttClient.subscribe("sensor/#", { qos: 0 });
});

mqttClient.on("message", async (topic, message) => {
	const data = message.toString();
	console.log(`Received: ${topic} -> ${data}`);

	const expireAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);

	const topicParts = topic.split("/");
	const [prefix, gedung, lantai, gender, type, number] = topicParts;

	if (prefix == "sensor") {
		try {
			const buildingRef = db.collection("sensor").doc(gedung);

			if (number !== undefined) {
				const docId = `${lantai}_${gender}_${number}`;

				await buildingRef.set({}, { merge: true });

				await buildingRef.collection(type).doc(docId).set(
					{
						gedung,
						lokasi: lantai,
						gender,
						nomor: number,
						status: data,
						last_updated: new Date(),
					},
					{ merge: true }
				);

				console.log(`Real time data updated: ${docId}`);

				await db.collection("logs").add({
					type,
					gedung,
					lokasi: lantai,
					gender,
					nomor: number,
					status: data,
					timestamp: new Date(),
					expireAt,
				});

				console.log("Log saved");
			} else {
				const docId = `${lantai}_${gender}`;

				await buildingRef.set({}, { merge: true });

				await buildingRef.collection(type).doc(docId).set(
					{
						gedung,
						lokasi: lantai,
						gender,
						status: data,
						last_updated: new Date(),
					},
					{ merge: true }
				);

				console.log(`Real time data updated: ${docId}`);

				await db.collection("logs").add({
					type,
					lokasi: lantai,
					gender,
					status: data,
					timestamp: new Date(),
					expireAt,
				});

				console.log("Log saved");
			}

			if (type == "okupansi" && data == "vacant") {
				const notifSnapshot = await db
					.collection("reminders")
					.where("gedung", "==", gedung)
					.where("lokasi", "==", lantai)
					.where("gender", "==", gender)
					.get();

				const sentKeys = new Set();

				notifSnapshot.forEach(async (doc) => {
					const notifData = doc.data();
					const fcmToken = notifData.fcmToken;
					const lokasi = notifData.lokasi;
					const gender = notifData.gender;
					const key = `${fcmToken}_${lokasi}_${gender}`;

					if (!sentKeys.has(key)) {
						sentKeys.add(key);

						const payload = {
							notification: {
								title: "Toilet Tersedia",
								body: `Toilet telah tersedia pada ${snakeToCapitalized(
									lokasi
								)} (${snakeToCapitalized(gender)})`,
							},
							token: fcmToken,
						};

						try {
							await admin.messaging().send(payload);
							console.log("Notification sent to:", fcmToken);
						} catch (err) {
							console.error("Error sending FCM:", err);
						}
					}

					await doc.ref.delete();
				});
			}
		} catch (err) {
			console.error("Firestore Error:", err);
		}
	} else if (prefix == "config") {
		//
	}
});

// Handle MQTT errors
mqttClient.on("error", (err) => console.error("MQTT Error:", err));

function snakeToCapitalized(str) {
	return str
		.split("_")
		.map((word) => word.charAt(0).toUpperCase() + word.slice(1))
		.join(" ");
}
