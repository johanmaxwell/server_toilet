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
	const [prefix, company, gedung, lokasi, gender, type, nomor] = topicParts;

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

			if (nomor !== undefined) {
				const docId = `${gedung}_${lokasi}_${gender}_${nomor}`;
				const sensorRef = buildingRef.collection(type).doc(docId);

				const sensorSnapshot = await sensorRef.get();
				updateUsageCount(company, "reads", sensorSnapshot.exists ? 1 : 0);

				const previousState = sensorSnapshot.exists
					? sensorSnapshot.data().status
					: null;

				if (type === "okupansi" || type === "bau") {
					const msgPart = data.split(";");
					const [status, nomor_tipe] = msgPart;

					await sensorRef.set(
						{
							lokasi,
							nomor: nomor_tipe,
							status,
							last_updated: new Date(),
						},
						{ merge: true }
					);
					updateUsageCount(company, "writes");

					if (
						type === "okupansi" ||
						(type === "bau" && previousState === "bad" && data === "good")
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
						updateUsageCount(company, "writes");
					}

					if (type === "okupansi" && data === "vacant") {
						const notifSnapshot = await db
							.collection("reminders")
							.where("gedung", "==", gedung)
							.where("lokasi", "==", lokasi)
							.where("gender", "==", gender)
							.get();
						updateUsageCount(company, "reads", notifSnapshot.size);

						notifSnapshot.forEach(async (doc) => {
							await doc.ref.delete();
						});
					}
				} else {
					const msgPart = data.split(";");
					const [status, amount, nomor_tipe] = msgPart;

					await sensorRef.set(
						{
							lokasi,
							nomor: nomor_tipe,
							status,
							amount,
							last_updated: new Date(),
						},
						{ merge: true }
					);
					updateUsageCount(company, "writes");

					if (
						(type === "sabun" || type === "tisu") &&
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
						updateUsageCount(company, "writes");
					}
				}
				console.log(`Real-time data updated: ${docId}`);
				console.log("Log saved");
			} else {
				const docId = `${lokasi}_${gender}`;

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

				console.log(`Real-time data updated: ${docId}`);
				console.log("Log saved");
			}
		} catch (err) {
			console.error("Firestore Error:", err);
		}
	} else if (prefix === "config") {
		const mac_address = gedung;

		const msgPart = data.split(";");
		const [
			gedung,
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
		] = msgPart;

		const gedungRef = db
			.collection("gedung")
			.doc(company)
			.collection("daftar")
			.doc(gedung);

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
			.collection(gedung)
			.doc(lokasi);

		await locationRef.set(
			{
				company,
				gedung,
			},
			{ merge: true }
		);
		updateUsageCount(company, "writes");
		console.log("Location added");

		const docId = `${gedung}_${lokasi}_${gender}_${nomor_perangkat}`;
		const configParentRef = db.collection("config").doc(company);
		await configParentRef.set({}, { merge: true });

		const configRef = configParentRef.collection(gender).doc(docId);

		await configRef.set(
			{
				company,
				gedung,
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
				version: 1,
			},
			{ merge: true }
		);
		updateUsageCount(company, "writes");
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
	updateUsageCount(company, "reads", notifSnapshot.size);

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
