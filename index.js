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
	mqttClient.subscribe("#", { qos: 0 });
});

mqttClient.on("message", async (topic, message) => {
	const data = message.toString();
	console.log(`Received: ${topic} -> ${data}`);

	const topicParts = topic.split("/");
	const [prefix, gedung, lokasi, gender, type, nomor] = topicParts;

	const locationRef = db.collection("lokasi").doc(lokasi);
	const locationDoc = await locationRef.get();

	if (!locationDoc.exists) {
		await locationRef.set({}, { merge: true });
		console.log("Location added");
	}

	if (prefix == "sensor") {
		try {
			const buildingRef = db.collection("sensor").doc(gedung);

			if (nomor !== undefined) {
				const docId = `${lokasi}_${gender}_${nomor}`;

				await buildingRef.set({}, { merge: true });

				const sensorDocRef = buildingRef.collection(type).doc(docId);
				const sensorSnapshot = await sensorDocRef.get();
				const previousState = sensorSnapshot.exists
					? sensorSnapshot.data().status
					: null;

				if (type == "okupansi" || type == "bau") {
					await sensorDocRef.set(
						{
							gedung,
							lokasi,
							gender,
							nomor,
							status: data,
							last_updated: new Date(),
						},
						{ merge: true }
					);

					if (
						type == "okupansi" ||
						(type == "bau" && previousState == "bad" && data == "good")
					) {
						await db.collection("logs").add({
							type,
							gedung,
							lokasi,
							gender,
							nomor,
							status: data,
							timestamp: new Date(),
						});
					}

					if (type == "okupansi" && data == "vacant") {
						const notifSnapshot = await db
							.collection("reminders")
							.where("gedung", "==", gedung)
							.where("lokasi", "==", lokasi)
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

					if (type == "bau" && data == "bad") {
						const notifSnapshot = await db
							.collection("users")
							.where("role", "==", "janitor")
							.where("active", "==", true)
							.get();

						notifSnapshot.forEach(async (doc) => {
							const notifData = doc.data();
							const fcmToken = notifData["fcm_token"];

							const payload = {
								notification: {
									title: "Bau Tidak Sedap!",
									body: `Segera bersihkan toilet pada ${snakeToCapitalized(
										gedung
									)} di ${snakeToCapitalized(lokasi)} (${snakeToCapitalized(
										gender
									)}) kubikal: ${nomor}`,
								},
								token: fcmToken,
							};

							try {
								await admin.messaging().send(payload);
								console.log("Notification sent to:", fcmToken);
							} catch (err) {
								console.error("Error sending FCM:", err);
							}
						});
					}
				} else {
					const msgPart = data.split(";");
					const [status, amount] = msgPart;

					await sensorDocRef.set(
						{
							gedung,
							lokasi,
							gender,
							nomor,
							status,
							amount,
							last_updated: new Date(),
						},
						{ merge: true }
					);

					if (
						(type == "sabun" || type == "tisu") &&
						previousState === "bad" &&
						status === "good"
					) {
						await db.collection("logs").add({
							type,
							gedung,
							lokasi,
							gender,
							nomor,
							status,
							timestamp: new Date(),
						});
					}
				}

				console.log(`Real time data updated: ${docId}`);
				console.log("Log saved");
			} else {
				const docId = `${lokasi}_${gender}`;

				await buildingRef.set({}, { merge: true });

				await buildingRef.collection(type).doc(docId).set(
					{
						gedung,
						lokasi,
						gender,
						status: data,
						last_updated: new Date(),
					},
					{ merge: true }
				);

				console.log(`Real time data updated: ${docId}`);

				await db.collection("logs").add({
					type,
					lokasi,
					gender,
					status: data,
					timestamp: new Date(),
				});

				console.log("Log saved");
			}
		} catch (err) {
			console.error("Firestore Error:", err);
		}
	} else if (prefix == "config") {
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
