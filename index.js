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
	const [prefix, company, gedung, lokasi, gender, type, nomor] = topicParts;

	if (prefix == "sensor") {
		try {
			const buildingRef = db
				.collection("sensor")
				.doc(company)
				.collection(gender)
				.doc(gedung);

			if (nomor !== undefined) {
				const docId = `${gedung}_${lokasi}_${gender}_${nomor}`;

				//await sensorRef.set({}, { merge: true });

				const sensorRef = buildingRef.collection(type).doc(docId);

				const sensorSnapshot = await sensorRef.get();
				const previousState = sensorSnapshot.exists
					? sensorSnapshot.data().status
					: null;

				if (type == "okupansi" || type == "bau") {
					await sensorRef.set(
						{
							lokasi,
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
						await db.collection("logs").doc(company).collection(type).add({
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
				} else {
					const msgPart = data.split(";");
					const [status, amount] = msgPart;

					await sensorRef.set(
						{
							lokasi,
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
						await db.collection("logs").doc(company).collection(type).add({
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
						lokasi,
						status: data,
						last_updated: new Date(),
					},
					{ merge: true }
				);

				console.log(`Real time data updated: ${docId}`);

				await db.collection("logs").doc(company).collection(type).add({
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

		if (data == "bad") {
			switch (type) {
				case "tisu":
					sendNotification(
						"Tisu Tersisa Sedikit!",
						`Segera isi ulang tisu pada ${snakeToCapitalized(
							gedung
						)} di ${snakeToCapitalized(lokasi)} (${snakeToCapitalized(
							gender
						)}) nomor kubikal: ${nomor}`
					);
					break;
				case "bau":
					let msg = `Segera bersihkan toilet pada ${snakeToCapitalized(
						gedung
					)} di ${snakeToCapitalized(lokasi)} (${snakeToCapitalized(gender)})`;
					if (nomor < 1000) {
						msg += `nomor kubikal: ${nomor}`;
					}
					sendNotification("Bau Tidak Sedap Terdeteksi!", msg);
					break;
				case "sabun":
					sendNotification(
						"Sabun Hampir Habis!",
						`Segera isi ulang sabun pada ${snakeToCapitalized(
							gedung
						)} di ${snakeToCapitalized(lokasi)} (${snakeToCapitalized(
							gender
						)}) nomor dispenser: ${nomor}`
					);
					break;
				case "baterai":
					sendNotification(
						"Baterai Lemah!",
						`Segera ganti baterai pada ${snakeToCapitalized(
							gedung
						)} di ${snakeToCapitalized(lokasi)} (${snakeToCapitalized(
							gender
						)}) nomor perangkat: ${nomor}`
					);
					break;
			}
		}
	} else if (prefix == "config") {
		const mac_address = gedung;

		const msgPart = data.split(";");
		const [
			building,
			lokasi,
			gender,
			nomor,
			wifi_ssid,
			wifi_password,
			mqtt_server,
			mqtt_port,
			mqtt_user,
			mqtt_password,
			nomor_toilet,
			nomor_dispenser,
			luar,
			setting_jarak,
			setting_berat,
		] = msgPart;

		const gedungRef = db
			.collection("gedung")
			.doc(company)
			.collection("daftar")
			.doc(building);

		const gedungDoc = await gedungRef.get();

		if (!gedungDoc.exists) {
			await gedungRef.set(
				{
					company,
				},
				{ merge: true }
			);
			console.log("Gedung added");
		}

		const locationRef = db
			.collection("lokasi")
			.doc(company)
			.collection(building)
			.doc(lokasi);
		const locationDoc = await locationRef.get();

		if (!locationDoc.exists) {
			await locationRef.set(
				{
					company,
					building,
				},
				{ merge: true }
			);
			console.log("Location added");
		}

		const docId = `${building}_${lokasi}_${gender}_${nomor}`;
		const configRef = db
			.collection("config")
			.doc(company)
			.collection("data")
			.doc(docId);

		await configRef.set(
			{
				gedung: building,
				lokasi,
				gender,
				nomor_perangkat: nomor,
				mac_address,
				wifi_ssid,
				wifi_password,
				mqtt_server,
				mqtt_port,
				mqtt_user,
				mqtt_password,
				nomor_toilet,
				nomor_dispenser,
				luar,
				setting_jarak,
				setting_berat,
			},
			{ merge: true }
		);
	}
});

// Handle MQTT errors
mqttClient.on("error", (err) => console.error("MQTT Error:", err));

async function sendNotification(title, msg) {
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
				title: title,
				body: msg,
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

const usageBuffer = new Map();

function updateWriteCount(companyId) {
	const current = usageBuffer.get(companyId) || {
		count: 0,
		lastFlushed: Date.now(),
	};
	current.count += 1;
	usageBuffer.set(companyId, current);

	if (current.count >= 1000) {
		flushToFirestore(companyId, current.count);
		usageBuffer.set(companyId, { count: 0, lastFlushed: Date.now() });
	}
}

async function flushToFirestore(companyId, count) {
	const today = new Date().toISOString().split("T")[0];
	const usageRef = db
		.collection("usage_metrics")
		.doc(companyId)
		.collection("daily")
		.doc(today);
	await db.runTransaction(async (t) => {
		const snapshot = await t.get(usageRef);
		if (!snapshot.exists) {
			t.set(usageRef, { writes: 0, reads: 0 });
		}
		t.update(usageRef, {
			writes: admin.firestore.FieldValue.increment(count),
		});
	});
}

setInterval(() => {
	usageBuffer.forEach((value, companyId) => {
		if (value.count > 0) {
			flushToFirestore(companyId, value.count);
			usageBuffer.set(companyId, { count: 0, lastFlushed: Date.now() });
		}
	});
}, 60 * 60 * 1000);

function snakeToCapitalized(str) {
	return str
		.split("_")
		.map((word) => word.charAt(0).toUpperCase() + word.slice(1))
		.join(" ");
}
