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

// Usage buffer for tracking reads and writes
const usageBuffer = new Map();

function updateUsageCount(companyId, type, count = 1) {
	const current = usageBuffer.get(companyId) || {
		reads: 0,
		writes: 0,
		lastFlushed: Date.now(),
	};
	current[type] += count;
	usageBuffer.set(companyId, current);

	// Flush if threshold is reached
	if (current.reads + current.writes >= 2000) {
		flushToFirestore(companyId, current);
		usageBuffer.set(companyId, {
			reads: 0,
			writes: 0,
			lastFlushed: Date.now(),
		});
	}
}

async function flushToFirestore(companyId, usage) {
	const today = new Date().toISOString().split("T")[0];
	const usageRef = db
		.collection("usage_metrics")
		.doc(companyId)
		.collection("daily")
		.doc(today);
	await db.runTransaction(async (t) => {
		const snapshot = await t.get(usageRef);
		if (!snapshot.exists) {
			t.set(usageRef, { reads: 0, writes: 0 });
		}
		t.update(usageRef, {
			reads: admin.firestore.FieldValue.increment(usage.reads),
			writes: admin.firestore.FieldValue.increment(usage.writes),
		});
	});
	console.log(
		`Flushed ${usage.reads} reads and ${usage.writes} writes for company: ${companyId}`
	);
}

// Periodic flush to avoid data loss
setInterval(() => {
	usageBuffer.forEach((value, companyId) => {
		if (value.reads > 0 || value.writes > 0) {
			flushToFirestore(companyId, value);
			usageBuffer.set(companyId, {
				reads: 0,
				writes: 0,
				lastFlushed: Date.now(),
			});
		}
	});
}, 60 * 60 * 1000);

mqttClient.on("connect", () => {
	console.log("Connected to MQTT Broker");
	mqttClient.subscribe("#", { qos: 0 });
});

mqttClient.on("message", async (topic, message) => {
	const data = message.toString();
	console.log(`Received: ${topic} -> ${data}`);

	const topicParts = topic.split("/");
	const [prefix, company, gedung, lokasi, gender, type, nomor_perangkat] =
		topicParts;

	if (prefix != "sensor" && prefix != "config") {
		return;
	}

	const companyDoc = await db.collection("company").doc(company).get();
	if (!companyDoc.exists) {
		console.log(`Company '${company}' not found.`);
		return;
	}

	const companyData = companyDoc.data();
	if (companyData.is_deactivated === true) {
		console.log(`Company '${company}' is deactivated. Ignoring data.`);
		return;
	}

	if (prefix === "sensor") {
		try {
			const buildingRef = db
				.collection("sensor")
				.doc(company)
				.collection(gender)
				.doc(gedung);

			const docId = `${gedung}_${lokasi}_${gender}_${nomor_perangkat}`;
			const sensorRef = buildingRef.collection(type).doc(docId);

			const sensorSnapshot = await sensorRef.get();
			updateUsageCount(company, "reads", sensorSnapshot.exists ? 1 : 0);

			const previousState = sensorSnapshot.exists
				? sensorSnapshot.data().status
				: null;

			if (type === "okupansi" || type === "bau") {
				const msgPart = data.split(";");
				const [status, nomor] = msgPart;

				await sensorRef.set(
					{
						lokasi,
						nomor,
						status,
						last_updated: new Date(),
					},
					{ merge: true }
				);
				updateUsageCount(company, "writes");

				if (
					type === "okupansi" ||
					(type === "bau" && previousState === "bad" && status === "good")
				) {
					await db.collection("logs").doc(company).collection(type).add({
						type,
						gedung,
						lokasi,
						gender,
						nomor,
						status,
						timestamp: new Date(),
					});
					updateUsageCount(company, "writes");
				}

				if (type === "okupansi" && status === "vacant") {
					const notifSnapshot = await db
						.collection("reminders")
						.where("gedung", "==", gedung)
						.where("lokasi", "==", lokasi)
						.where("gender", "==", gender)
						.where("company", "==", company)
						.get();
					updateUsageCount(company, "reads", notifSnapshot.size);

					notifSnapshot.forEach(async (doc) => {
						const reminderData = doc.data();
						const fcmToken = reminderData.fcm_token;

						const payload = {
							notification: {
								title: "Toilet Tersedia",
								body: `Toilet pada ${snakeToCapitalized(
									gedung
								)} - ${snakeToCapitalized(lokasi)} sekarang tersedia.`,
							},
							token: fcmToken,
						};

						try {
							await admin.messaging().send(payload);
							console.log("Notification sent to:", fcmToken);
						} catch (err) {
							console.error("Error sending FCM:", err);
						}
						await doc.ref.delete();
					});
				}

				if (type === "bau" && status === "bad") {
					const usersSnapshot = await db
						.collection("users")
						.where("role", "==", "janitor")
						.where("active", "==", true)
						.where("company", "==", company)
						.get();
					updateUsageCount(company, "reads", usersSnapshot.size);

					usersSnapshot.forEach(async (doc) => {
						const userData = doc.data();
						const fcmToken = userData.fcm_token;

						const payload = {
							notification: {
								title: "Peringatan Bau Tidak Sedap",
								body: `(${snakeToCapitalized(
									company
								)}) Bau tidak sedap terdeteksi di ${snakeToCapitalized(
									gedung
								)} - ${snakeToCapitalized(lokasi)}${
									nomor > 1000 ? " - Luar" : ` - Toilet ${nomor}`
								}`,
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
			} else if (type === "pengunjung") {
				await buildingRef.set({}, { merge: true });
				updateUsageCount(company, "writes");

				await buildingRef.collection(type).doc(docId).set(
					{
						lokasi,
						status: data,
						last_updated: new Date(),
					},
					{ merge: true }
				);
				updateUsageCount(company, "writes");

				await db.collection("logs").doc(company).collection(type).add({
					lokasi,
					gender,
					status: data,
					timestamp: new Date(),
				});
				updateUsageCount(company, "writes");
			} else {
				const msgPart = data.split(";");
				const [status, amount, nomor] = msgPart;

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
				updateUsageCount(company, "writes");

				if (
					(type === "sabun" || type === "tisu") &&
					(previousState === "good" || previousState === "ok") &&
					status === "bad"
				) {
					await db.collection("logs").doc(company).collection(type).add({
						gedung,
						lokasi,
						gender,
						nomor,
						status,
						timestamp: new Date(),
					});
					updateUsageCount(company, "writes");
				}

				if (
					status === "bad" &&
					(previousState === "good" || previousState === "ok")
				) {
					const usersSnapshot = await db
						.collection("users")
						.where("role", "==", "janitor")
						.where("active", "==", true)
						.where("company", "==", company)
						.get();
					updateUsageCount(company, "reads", usersSnapshot.size);

					let title = "";
					let message = "";

					switch (type) {
						case "sabun":
							title = "Peringatan Sabun Habis";
							message = `(${snakeToCapitalized(
								company
							)}) Sabun akan segera habis di ${snakeToCapitalized(
								gedung
							)} - ${snakeToCapitalized(lokasi)} - Dispenser ${nomor}`;
							break;
						case "tisu":
							title = "Peringatan Tisu Habis";
							message = `(${snakeToCapitalized(
								company
							)}) Tisu akan segera habis di ${snakeToCapitalized(
								gedung
							)} - ${snakeToCapitalized(lokasi)} - Toilet ${nomor}`;
							break;
						case "baterai":
							title = "Peringatan Baterai Lemah";
							message = `(${snakeToCapitalized(
								company
							)}) Segera ganti baterai perangkat nomor ${nomor} di ${snakeToCapitalized(
								gedung
							)} - ${snakeToCapitalized(lokasi)}`;
							break;
					}

					if (title && message) {
						usersSnapshot.forEach(async (doc) => {
							const userData = doc.data();
							const fcmToken = userData.fcm_token;

							if (fcmToken) {
								const payload = {
									notification: {
										title: title,
										body: message,
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
						});
					}
				}
			}
			console.log(`Real-time data updated: ${docId}`);
			console.log("Log saved");
		} catch (err) {
			console.error("Firestore Error:", err);
		}
	} else if (prefix === "config") {
		const mac_address = gedung;

		const msgPart = data.split(";");
		const [
			gedung2,
			lokasi,
			gender,
			nomor_perangkat,
			wifi_ssid,
			wifi_password,
			mqtt_server,
			mqtt_port,
			mqtt_user,
			mqtt_password,
			okupansi,
			pengunjung,
			tisu,
			sabun,
			bau,
			nomor_toilet,
			nomor_dispenser,
			is_luar,
			jarak_deteksi,
			berat_tisu,
			placement,
		] = msgPart;

		const gedungRef = db
			.collection("gedung")
			.doc(company)
			.collection("daftar")
			.doc(gedung2);

		await gedungRef.set(
			{
				company,
			},
			{ merge: true }
		);
		updateUsageCount(company, "writes");
		console.log("Gedung added");

		const locationRef = db
			.collection("lokasi")
			.doc(company)
			.collection(gedung2)
			.doc(lokasi);

		await locationRef.set(
			{
				company,
				gedung: gedung2,
			},
			{ merge: true }
		);
		updateUsageCount(company, "writes");
		console.log("Location added");

		// Get old config data if exists
		const oldConfigRef = db
			.collection("config")
			.doc(company)
			.collection(gender)
			.where("mac_address", "==", mac_address)
			.limit(1);

		const oldConfigSnapshot = await oldConfigRef.get();
		updateUsageCount(company, "reads");

		if (oldConfigSnapshot.empty) {
			// New device, proceed with normal config setup

			const docId = `${gedung2}_${lokasi}_${gender}_${nomor_perangkat}`;
			const configParentRef = db.collection("config").doc(company);
			await configParentRef.set({}, { merge: true });

			const configRef = configParentRef.collection(gender).doc(docId);

			await configRef.set(
				{
					company,
					gedung: gedung2,
					lokasi,
					gender,
					nomor_perangkat,
					mac_address,
					wifi_ssid,
					wifi_password,
					mqtt_server,
					mqtt_port,
					mqtt_user,
					mqtt_password,
					okupansi,
					pengunjung,
					tisu,
					sabun,
					bau,
					nomor_toilet,
					nomor_dispenser,
					is_luar,
					jarak_deteksi,
					berat_tisu,
					placement,
					version: "1",
				},
				{ merge: true }
			);
			updateUsageCount(company, "writes");
		} else {
			// Existing device, check if key fields changed
			const oldConfig = oldConfigSnapshot.docs[0].data();
			const oldDocId = oldConfigSnapshot.docs[0].id;
			const newDocId = `${gedung2}_${lokasi}_${gender}_${nomor_perangkat}`;

			const hasChanges =
				oldConfig.gedung !== gedung2 ||
				oldConfig.lokasi !== lokasi ||
				oldConfig.gender !== gender ||
				oldConfig.nomor_perangkat !== nomor_perangkat;

			if (hasChanges) {
				// Migrate sensor data
				const types = [
					"baterai",
					"bau",
					"okupansi",
					"pengunjung",
					"sabun",
					"tisu",
				];

				for (const type of types) {
					const oldSensorRef = db
						.collection("sensor")
						.doc(company)
						.collection(oldConfig.gender)
						.doc(oldConfig.gedung)
						.collection(type)
						.doc(oldDocId);

					const oldSensorSnapshot = await oldSensorRef.get();
					updateUsageCount(company, "reads");

					if (oldSensorSnapshot.exists) {
						const oldSensorData = oldSensorSnapshot.data();

						// Update lokasi
						oldSensorData.lokasi = lokasi;

						// Update nomor based on type
						if (type === "baterai") {
							oldSensorData.nomor = nomor_perangkat;
						} else if (["bau", "okupansi", "tisu"].includes(type)) {
							oldSensorData.nomor = nomor_toilet;
						} else if (type === "sabun") {
							oldSensorData.nomor = nomor_dispenser;
						}

						// Delete old sensor data
						await oldSensorRef.delete();
						updateUsageCount(company, "writes");

						// Create new sensor data
						await db
							.collection("sensor")
							.doc(company)
							.collection(gender)
							.doc(gedung2)
							.collection(type)
							.doc(newDocId)
							.set(oldSensorData);
						updateUsageCount(company, "writes");
					}
				}

				// Check if old gedung is still in use
				const oldGedungConfigsPria = await db
					.collection("config")
					.doc(company)
					.collection("pria")
					.where("gedung", "==", oldConfig.gedung)
					.get();
				updateUsageCount(company, "reads");

				const oldGedungConfigsWanita = await db
					.collection("config")
					.doc(company)
					.collection("wanita")
					.where("gedung", "==", oldConfig.gedung)
					.get();
				updateUsageCount(company, "reads");

				// Check if old lokasi is still in use
				const oldLokasiConfigsPria = await db
					.collection("config")
					.doc(company)
					.collection("pria")
					.where("lokasi", "==", oldConfig.lokasi)
					.get();
				updateUsageCount(company, "reads");

				const oldLokasiConfigsWanita = await db
					.collection("config")
					.doc(company)
					.collection("wanita")
					.where("lokasi", "==", oldConfig.lokasi)
					.get();
				updateUsageCount(company, "reads");

				// Delete old config
				await oldConfigSnapshot.docs[0].ref.delete();
				updateUsageCount(company, "writes");

				// If old gedung has no more devices in both collections, remove it
				if (
					oldGedungConfigsPria.size === 0 &&
					oldGedungConfigsWanita.size === 0
				) {
					await db
						.collection("gedung")
						.doc(company)
						.collection("daftar")
						.doc(oldConfig.gedung)
						.delete();
					updateUsageCount(company, "writes");
					console.log(`Removed unused gedung: ${oldConfig.gedung}`);
				}

				// If old lokasi has no more devices in both collections, remove it
				if (
					oldLokasiConfigsPria.size === 0 &&
					oldLokasiConfigsWanita.size === 0
				) {
					await db
						.collection("lokasi")
						.doc(company)
						.collection(oldConfig.gedung)
						.doc(oldConfig.lokasi)
						.delete();
					updateUsageCount(company, "writes");
					console.log(`Removed unused lokasi: ${oldConfig.lokasi}`);
				}
			}

			// Update or create new config
			const configParentRef = db.collection("config").doc(company);
			await configParentRef.set({}, { merge: true });

			const configRef = configParentRef.collection(gender).doc(newDocId);

			await configRef.set(
				{
					company,
					gedung: gedung2,
					lokasi,
					gender,
					nomor_perangkat,
					mac_address,
					wifi_ssid,
					wifi_password,
					mqtt_server,
					mqtt_port,
					mqtt_user,
					mqtt_password,
					okupansi,
					pengunjung,
					tisu,
					sabun,
					bau,
					nomor_toilet,
					nomor_dispenser,
					is_luar,
					jarak_deteksi,
					berat_tisu,
					placement,
					version: "1",
				},
				{ merge: true }
			);
			updateUsageCount(company, "writes");
		}
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

//Send MQTT messsage on config update
function publishConfigUpdate(companyId, configData) {
	const macAddress = configData.mac_address;
	const topic = `update/${companyId}/${macAddress}`;
	const message = JSON.stringify(configData);

	mqttClient.publish(topic, message, { qos: 0, retain: true }, (err) => {
		if (err) {
			console.error(`Failed to publish config for ${companyId}:`, err);
		} else {
			console.log(`Published config update to ${topic}`);
		}
	});
}

function attachConfigListener(companyId) {
	const subcollections = ["pria", "wanita"];

	subcollections.forEach((subcollection) => {
		const configRef = db
			.collection("config")
			.doc(companyId)
			.collection(subcollection);

		configRef.onSnapshot((snapshot) => {
			snapshot.docChanges().forEach((change) => {
				if (change.type === "added" || change.type === "modified") {
					const configData = change.doc.data();
					publishConfigUpdate(companyId, configData);
				}
			});
		});

		console.log(
			`Listener attached for company: ${companyId}, subcollection: ${subcollection}`
		);
	});
}

function monitorCompanies() {
	const configCollection = db.collection("config");

	configCollection.get().then((snapshot) => {
		snapshot.forEach((doc) => {
			console.log(doc.id);
			attachConfigListener(doc.id);
		});
	});

	configCollection.onSnapshot((snapshot) => {
		snapshot.docChanges().forEach((change) => {
			if (change.type === "added") {
				attachConfigListener(change.doc.id);
			}
		});
	});
}

monitorCompanies();
