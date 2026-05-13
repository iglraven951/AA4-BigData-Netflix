const express = require('express');
const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = 3000;

// MongoDB Atlas connection
const MONGO_URI = 'mongodb+srv://iglraven159:iglraven159%40@netflix.brw7jwq.mongodb.net/?appName=netflix';
const DB_NAME = 'netflix_analytics';

// Kafka configuration
const KAFKA_BROKERS = ['localhost:29092'];
const TOPIC_EVENTS = 'netflix-events';
const TOPIC_ALERTS = 'netflix-alerts';

let db = null;
let mongoClient = null;
let kafkaProducer = null;
let kafkaAdmin = null;
let isKafkaConnected = false;
let kafkaEvents = [];
let eventStats = { total: 0, byType: {}, alerts: 0 };

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// =============================================================================
// MONGODB CONNECTION
// =============================================================================
async function connectMongoDB() {
    try {
        mongoClient = new MongoClient(MONGO_URI, {
            serverSelectionTimeoutMS: 5000,
            connectTimeoutMS: 5000
        });
        await mongoClient.connect();
        db = mongoClient.db(DB_NAME);
        console.log('✅ MongoDB Atlas conectado');
        return true;
    } catch (error) {
        console.error('❌ Error MongoDB:', error.message);
        return false;
    }
}

async function checkMongoConnection() {
    try {
        if (!mongoClient || !db) return false;
        await db.command({ ping: 1 });
        return true;
    } catch {
        return false;
    }
}

// =============================================================================
// KAFKA CONNECTION
// =============================================================================
async function connectKafka() {
    try {
        const kafka = new Kafka({
            clientId: 'netflix-dashboard',
            brokers: KAFKA_BROKERS,
            connectionTimeout: 5000,
            retry: { retries: 3 }
        });

        kafkaAdmin = kafka.admin();
        kafkaProducer = kafka.producer();

        await kafkaAdmin.connect();
        await kafkaProducer.connect();

        // Create topics if not exist
        const existingTopics = await kafkaAdmin.listTopics();
        const topicsToCreate = [];

        if (!existingTopics.includes(TOPIC_EVENTS)) {
            topicsToCreate.push({ topic: TOPIC_EVENTS, numPartitions: 3 });
        }
        if (!existingTopics.includes(TOPIC_ALERTS)) {
            topicsToCreate.push({ topic: TOPIC_ALERTS, numPartitions: 1 });
        }

        if (topicsToCreate.length > 0) {
            await kafkaAdmin.createTopics({ topics: topicsToCreate });
            console.log('✅ Kafka topics creados');
        }

        isKafkaConnected = true;
        console.log('✅ Kafka conectado');
        return true;
    } catch (error) {
        console.error('❌ Error Kafka:', error.message);
        isKafkaConnected = false;
        return false;
    }
}

// =============================================================================
// EVENT GENERATORS
// =============================================================================
const USERS = Array.from({ length: 50 }, (_, i) => `USR${String(i + 1).padStart(5, '0')}`);
const CONTENT = Array.from({ length: 100 }, (_, i) => `CNT${String(i + 1).padStart(5, '0')}`);
const DEVICES = ['Smart TV', 'Mobile', 'Tablet', 'Laptop', 'Desktop', 'Gaming Console'];
const COUNTRIES = ['Mexico', 'USA', 'Spain', 'Argentina', 'Colombia', 'Peru', 'Chile'];
const QUALITIES = ['SD', 'HD', 'Full HD', '4K'];

function randomChoice(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
}

function generateEvent(type) {
    const base = {
        event_id: uuidv4(),
        event_type: type,
        timestamp: new Date().toISOString(),
        user_id: randomChoice(USERS),
        device: randomChoice(DEVICES),
        country: randomChoice(COUNTRIES)
    };

    switch (type) {
        case 'PLAY':
            return {
                ...base,
                content_id: randomChoice(CONTENT),
                quality: randomChoice(QUALITIES),
                position_seconds: Math.floor(Math.random() * 3600),
                buffering_time_ms: Math.floor(Math.random() * 2000)
            };
        case 'PAUSE':
            return {
                ...base,
                content_id: randomChoice(CONTENT),
                position_seconds: Math.floor(Math.random() * 3600),
                pause_duration: Math.floor(Math.random() * 300)
            };
        case 'RATE':
            return {
                ...base,
                content_id: randomChoice(CONTENT),
                rating: Math.floor(Math.random() * 5) + 1,
                feedback_type: randomChoice(['thumbs_up', 'thumbs_down', 'stars'])
            };
        case 'SEARCH':
            const queries = ['action movies', 'comedy', 'new releases', 'spanish films', 'anime', 'documentaries'];
            return {
                ...base,
                search_query: randomChoice(queries),
                results_count: Math.floor(Math.random() * 150)
            };
        case 'LOGIN':
            return {
                ...base,
                ip_address: `192.168.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`,
                session_id: uuidv4().substring(0, 8)
            };
        case 'ERROR':
            const errors = [
                { code: 'PLAYBACK_ERROR', msg: 'Video playback failed', severity: 'HIGH' },
                { code: 'NETWORK_ERROR', msg: 'Connection timeout', severity: 'MEDIUM' },
                { code: 'AUTH_ERROR', msg: 'Session expired', severity: 'CRITICAL' },
                { code: 'BUFFERING_CRITICAL', msg: 'Buffering exceeded threshold', severity: 'HIGH' }
            ];
            const error = randomChoice(errors);
            return {
                ...base,
                content_id: randomChoice(CONTENT),
                error_code: error.code,
                error_message: error.msg,
                severity: error.severity
            };
        default:
            return base;
    }
}

function generateRandomEvent() {
    const weights = [
        { type: 'PLAY', weight: 40 },
        { type: 'PAUSE', weight: 20 },
        { type: 'RATE', weight: 15 },
        { type: 'SEARCH', weight: 15 },
        { type: 'LOGIN', weight: 7 },
        { type: 'ERROR', weight: 3 }
    ];

    const totalWeight = weights.reduce((sum, w) => sum + w.weight, 0);
    let random = Math.floor(Math.random() * totalWeight);

    for (const { type, weight } of weights) {
        random -= weight;
        if (random < 0) return generateEvent(type);
    }
    return generateEvent('PLAY');
}

// =============================================================================
// API ROUTES - STATUS
// =============================================================================
app.get('/api/status', async (req, res) => {
    const mongoConnected = await checkMongoConnection();
    res.json({
        mongodb: { connected: mongoConnected, database: mongoConnected ? DB_NAME : null },
        kafka: { connected: isKafkaConnected, brokers: KAFKA_BROKERS },
        timestamp: new Date().toISOString()
    });
});

// =============================================================================
// API ROUTES - MONGODB
// =============================================================================
app.get('/api/collections', async (req, res) => {
    try {
        if (!await checkMongoConnection()) {
            return res.status(503).json({ error: 'MongoDB desconectado' });
        }
        const collections = await db.listCollections().toArray();
        const stats = [];
        for (const col of collections) {
            const count = await db.collection(col.name).countDocuments();
            stats.push({ name: col.name, count });
        }
        res.json(stats);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/collection/:name', async (req, res) => {
    try {
        if (!await checkMongoConnection()) {
            return res.status(503).json({ error: 'MongoDB desconectado' });
        }
        const limit = parseInt(req.query.limit) || 50;
        const docs = await db.collection(req.params.name).find({}).limit(limit).toArray();
        res.json(docs);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/stats', async (req, res) => {
    try {
        if (!await checkMongoConnection()) {
            return res.status(503).json({ error: 'MongoDB desconectado' });
        }

        const collections = ['catalogo', 'usuarios', 'visualizaciones', 'valoraciones',
            'catalogo_stats', 'usuarios_metricas', 'engagement'];
        const counts = {};

        for (const col of collections) {
            try {
                counts[col] = await db.collection(col).countDocuments();
            } catch {
                counts[col] = 0;
            }
        }

        const catalogoPorTipo = await db.collection('catalogo').aggregate([
            { $group: { _id: '$tipo', count: { $sum: 1 } } }
        ]).toArray();

        const usuariosPorPlan = await db.collection('usuarios').aggregate([
            { $group: { _id: '$plan', count: { $sum: 1 } } }
        ]).toArray();

        const usuariosPorPais = await db.collection('usuarios').aggregate([
            { $group: { _id: '$pais', count: { $sum: 1 } } },
            { $sort: { count: -1 } },
            { $limit: 5 }
        ]).toArray();

        res.json({ counts, catalogoPorTipo, usuariosPorPlan, usuariosPorPais });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// =============================================================================
// API ROUTES - LOAD DATA TO MONGODB
// =============================================================================
app.post('/api/load-data', async (req, res) => {
    try {
        if (!await checkMongoConnection()) {
            return res.status(503).json({ error: 'MongoDB desconectado' });
        }

        const dataDir = path.join(__dirname, '..', 'datos');
        const results = { loaded: [], errors: [] };

        // Files to load
        const filesToLoad = [
            { file: 'catalogo_completo.csv', collection: 'catalogo', type: 'csv' },
            { file: 'usuarios_completo.csv', collection: 'usuarios', type: 'csv' },
            { file: 'visualizaciones_historico.json', collection: 'visualizaciones', type: 'json' },
            { file: 'valoraciones_historico.json', collection: 'valoraciones', type: 'json' }
        ];

        // Fallback to original files if new ones don't exist
        const fallbackFiles = [
            { file: 'catalogo.csv', collection: 'catalogo', type: 'csv' },
            { file: 'usuarios.csv', collection: 'usuarios', type: 'csv' },
            { file: 'visualizaciones.json', collection: 'visualizaciones', type: 'json' },
            { file: 'valoraciones.json', collection: 'valoraciones', type: 'json' }
        ];

        for (let i = 0; i < filesToLoad.length; i++) {
            let { file, collection, type } = filesToLoad[i];
            let filePath = path.join(dataDir, file);

            // Use fallback if main file doesn't exist
            if (!fs.existsSync(filePath)) {
                const fallback = fallbackFiles[i];
                filePath = path.join(dataDir, fallback.file);
                file = fallback.file;
            }

            if (!fs.existsSync(filePath)) {
                results.errors.push(`${file}: archivo no encontrado`);
                continue;
            }

            try {
                let data;
                const content = fs.readFileSync(filePath, 'utf-8');

                if (type === 'json') {
                    data = JSON.parse(content);
                } else if (type === 'csv') {
                    const lines = content.trim().split('\n');
                    const headers = lines[0].split(',').map(h => h.trim());
                    data = lines.slice(1).map(line => {
                        const values = line.split(',');
                        const obj = {};
                        headers.forEach((h, idx) => {
                            let val = values[idx]?.trim() || '';
                            // Try to parse numbers
                            if (!isNaN(val) && val !== '') {
                                val = parseFloat(val);
                            } else if (val === 'true') {
                                val = true;
                            } else if (val === 'false') {
                                val = false;
                            }
                            obj[h] = val;
                        });
                        return obj;
                    });
                }

                // Clear and insert
                await db.collection(collection).deleteMany({});
                if (data.length > 0) {
                    // Insert in batches of 1000
                    const batchSize = 1000;
                    for (let j = 0; j < data.length; j += batchSize) {
                        const batch = data.slice(j, j + batchSize);
                        await db.collection(collection).insertMany(batch);
                    }
                }

                results.loaded.push({ collection, file, count: data.length });
            } catch (err) {
                results.errors.push(`${file}: ${err.message}`);
            }
        }

        // Generate aggregated collections
        try {
            // catalogo_stats
            const catalogoStats = await db.collection('catalogo').aggregate([
                {
                    $group: {
                        _id: '$genero',
                        count: { $sum: 1 },
                        avg_rating: { $avg: '$rating_promedio' }
                    }
                }
            ]).toArray();
            await db.collection('catalogo_stats').deleteMany({});
            if (catalogoStats.length > 0) {
                await db.collection('catalogo_stats').insertMany(catalogoStats);
            }
            results.loaded.push({ collection: 'catalogo_stats', count: catalogoStats.length });

            // usuarios_metricas
            const usuariosMetricas = await db.collection('usuarios').aggregate([
                {
                    $group: {
                        _id: { pais: '$pais', plan: '$plan' },
                        count: { $sum: 1 }
                    }
                }
            ]).toArray();
            await db.collection('usuarios_metricas').deleteMany({});
            if (usuariosMetricas.length > 0) {
                await db.collection('usuarios_metricas').insertMany(usuariosMetricas);
            }
            results.loaded.push({ collection: 'usuarios_metricas', count: usuariosMetricas.length });

            // engagement
            const engagement = await db.collection('visualizaciones').aggregate([
                {
                    $group: {
                        _id: '$content_id',
                        views: { $sum: 1 },
                        avg_watch_time: { $avg: '$minutos_vistos' },
                        completion_rate: {
                            $avg: {
                                $cond: [{ $eq: ['$completado', true] }, 1, 0]
                            }
                        }
                    }
                },
                { $limit: 100 }
            ]).toArray();
            await db.collection('engagement').deleteMany({});
            if (engagement.length > 0) {
                await db.collection('engagement').insertMany(engagement);
            }
            results.loaded.push({ collection: 'engagement', count: engagement.length });

        } catch (err) {
            results.errors.push(`aggregations: ${err.message}`);
        }

        res.json(results);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// =============================================================================
// API ROUTES - KAFKA
// =============================================================================
app.get('/api/kafka/status', (req, res) => {
    res.json({
        connected: isKafkaConnected,
        topics: [TOPIC_EVENTS, TOPIC_ALERTS],
        stats: eventStats,
        recentEvents: kafkaEvents.slice(-20)
    });
});

app.post('/api/kafka/connect', async (req, res) => {
    const connected = await connectKafka();
    res.json({ connected, message: connected ? 'Kafka conectado' : 'Error conectando a Kafka' });
});

app.post('/api/kafka/produce', async (req, res) => {
    if (!isKafkaConnected) {
        return res.status(503).json({ error: 'Kafka no conectado' });
    }

    // Accept from body or query params
    const count = parseInt(req.body.count || req.query.count) || 100;
    const rate = parseInt(req.body.rate || req.query.rate) || 10;
    const maxCount = Math.min(Math.max(count, 1), 5000);
    const actualRate = Math.max(rate, 1);
    const results = { sent: 0, errors: 0, events: [], requested: count, producing: maxCount };

    try {
        for (let i = 0; i < maxCount; i++) {
            const event = generateRandomEvent();
            const topic = event.event_type === 'ERROR' ? TOPIC_ALERTS : TOPIC_EVENTS;

            try {
                await kafkaProducer.send({
                    topic,
                    messages: [{ key: event.user_id, value: JSON.stringify(event) }]
                });

                results.sent++;
                eventStats.total++;
                eventStats.byType[event.event_type] = (eventStats.byType[event.event_type] || 0) + 1;
                if (event.event_type === 'ERROR') eventStats.alerts++;

                // Store recent events
                kafkaEvents.push(event);
                if (kafkaEvents.length > 100) kafkaEvents.shift();

                if (i < 10) results.events.push(event);

                // Rate limiting
                if (actualRate > 0 && i < maxCount - 1) {
                    await new Promise(resolve => setTimeout(resolve, 1000 / actualRate));
                }
            } catch (err) {
                results.errors++;
            }
        }

        res.json(results);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/kafka/produce-single', async (req, res) => {
    if (!isKafkaConnected) {
        return res.status(503).json({ error: 'Kafka no conectado' });
    }

    try {
        const { type = 'PLAY' } = req.body;
        const event = generateEvent(type);
        const topic = type === 'ERROR' ? TOPIC_ALERTS : TOPIC_EVENTS;

        await kafkaProducer.send({
            topic,
            messages: [{ key: event.user_id, value: JSON.stringify(event) }]
        });

        eventStats.total++;
        eventStats.byType[event.event_type] = (eventStats.byType[event.event_type] || 0) + 1;
        if (type === 'ERROR') eventStats.alerts++;

        kafkaEvents.push(event);
        if (kafkaEvents.length > 100) kafkaEvents.shift();

        res.json({ success: true, event });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/kafka/reset-stats', (req, res) => {
    eventStats = { total: 0, byType: {}, alerts: 0 };
    kafkaEvents = [];
    res.json({ success: true, message: 'Stats reseteados' });
});

// =============================================================================
// API ROUTES - DATA FILES INFO
// =============================================================================
app.get('/api/files', (req, res) => {
    const dataDir = path.join(__dirname, '..', 'datos');
    try {
        const validExtensions = ['.csv', '.json', '.txt'];
        const files = fs.readdirSync(dataDir)
            .filter(file => {
                // Filter out system files and directories
                if (file.startsWith('.')) return false;
                if (file.includes('stackdump')) return false;
                const ext = path.extname(file).toLowerCase();
                return validExtensions.includes(ext);
            })
            .map(file => {
                const filePath = path.join(dataDir, file);
                const stats = fs.statSync(filePath);
                if (!stats.isFile()) return null;

                let lineCount = 0;
                try {
                    const content = fs.readFileSync(filePath, 'utf-8');
                    lineCount = content.split('\n').length;
                } catch {}

                return {
                    name: file,
                    size: stats.size,
                    lines: lineCount,
                    modified: stats.mtime,
                    format: path.extname(file).slice(1).toUpperCase()
                };
            })
            .filter(Boolean)
            .sort((a, b) => b.lines - a.lines); // Sort by lines descending

        res.json(files);
    } catch (error) {
        res.json([]);
    }
});

// =============================================================================
// SERVE FRONTEND
// =============================================================================
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// =============================================================================
// START SERVER
// =============================================================================
async function startServer() {
    console.log('\n' + '='.repeat(60));
    console.log('   NETFLIX ANALYTICS DASHBOARD - BIG DATA');
    console.log('='.repeat(60));

    // Connect to MongoDB
    const mongoConnected = await connectMongoDB();

    // Try to connect to Kafka (non-blocking)
    connectKafka().catch(() => {
        console.log('⚠️  Kafka no disponible - se puede conectar manualmente');
    });

    app.listen(PORT, () => {
        console.log(`
✅ Servidor corriendo en: http://localhost:${PORT}
📊 MongoDB: ${mongoConnected ? 'Conectado' : 'Desconectado'}
📡 Kafka: Conectando...

${'='.repeat(60)}
        `);
    });
}

startServer();
