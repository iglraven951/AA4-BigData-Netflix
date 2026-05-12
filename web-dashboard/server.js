const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');
const path = require('path');

const app = express();
const PORT = 3000;

// MongoDB Atlas connection
const MONGO_URI = 'mongodb+srv://iglraven159:iglraven159%40@netflix.brw7jwq.mongodb.net/?appName=netflix';
const DB_NAME = 'netflix_analytics';

let db = null;
let mongoClient = null;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Connect to MongoDB Atlas
async function connectDB() {
    try {
        mongoClient = new MongoClient(MONGO_URI, {
            serverSelectionTimeoutMS: 5000,
            connectTimeoutMS: 5000
        });
        await mongoClient.connect();
        db = mongoClient.db(DB_NAME);
        console.log('✅ Conectado a MongoDB Atlas - netflix_analytics');
        return true;
    } catch (error) {
        db = null;
        mongoClient = null;
        console.error('❌ Error conectando a MongoDB:', error.message);
        return false;
    }
}

// Check connection status - Real ping to database
async function checkConnection() {
    try {
        if (!mongoClient || !db) {
            return false;
        }
        // Real ping with short timeout
        await db.command({ ping: 1 });
        return true;
    } catch (error) {
        console.log('❌ Conexión perdida:', error.message);
        return false;
    }
}

// Middleware to check connection before each API call
async function requireConnection(req, res, next) {
    const connected = await checkConnection();
    if (!connected) {
        return res.status(503).json({
            error: 'Base de datos desconectada',
            connected: false
        });
    }
    next();
}

// API Routes

// Check connection status (real-time) - No cache
app.get('/api/status', async (req, res) => {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate');

    const connected = await checkConnection();
    res.json({
        connected,
        database: connected ? DB_NAME : null,
        timestamp: new Date().toISOString()
    });
});

// Get all collections info
app.get('/api/collections', requireConnection, async (req, res) => {
    try {
        const collections = await db.listCollections().toArray();
        const collectionStats = [];

        for (const col of collections) {
            const count = await db.collection(col.name).countDocuments();
            collectionStats.push({
                name: col.name,
                count: count
            });
        }

        res.json(collectionStats);
    } catch (error) {
        res.status(500).json({ error: error.message, connected: false });
    }
});

// Get documents from a collection
app.get('/api/collection/:name', requireConnection, async (req, res) => {
    try {
        const collectionName = req.params.name;
        const limit = parseInt(req.query.limit) || 50;
        const documents = await db.collection(collectionName).find({}).limit(limit).toArray();
        res.json(documents);
    } catch (error) {
        res.status(500).json({ error: error.message, connected: false });
    }
});

// Get stats summary
app.get('/api/stats', requireConnection, async (req, res) => {
    try {
        const stats = {
            catalogo: await db.collection('catalogo').countDocuments(),
            usuarios: await db.collection('usuarios').countDocuments(),
            visualizaciones: await db.collection('visualizaciones').countDocuments(),
            valoraciones: await db.collection('valoraciones').countDocuments(),
            catalogo_stats: await db.collection('catalogo_stats').countDocuments(),
            usuarios_metricas: await db.collection('usuarios_metricas').countDocuments(),
            engagement: await db.collection('engagement').countDocuments()
        };

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

        res.json({
            connected: true,
            counts: stats,
            catalogoPorTipo,
            usuariosPorPlan,
            usuariosPorPais
        });
    } catch (error) {
        res.status(500).json({ error: error.message, connected: false });
    }
});

// Get catalogo with filters
app.get('/api/catalogo', requireConnection, async (req, res) => {
    try {
        const { tipo, genero } = req.query;
        const filter = {};
        if (tipo) filter.tipo = tipo;
        if (genero) filter.genero = genero;

        const catalogo = await db.collection('catalogo').find(filter).toArray();
        res.json(catalogo);
    } catch (error) {
        res.status(500).json({ error: error.message, connected: false });
    }
});

// Get usuarios
app.get('/api/usuarios', requireConnection, async (req, res) => {
    try {
        const { pais, plan } = req.query;
        const filter = {};
        if (pais) filter.pais = pais;
        if (plan) filter.plan = plan;

        const usuarios = await db.collection('usuarios').find(filter).toArray();
        res.json(usuarios);
    } catch (error) {
        res.status(500).json({ error: error.message, connected: false });
    }
});

// Get engagement metrics
app.get('/api/engagement', requireConnection, async (req, res) => {
    try {
        const engagement = await db.collection('engagement').find({}).toArray();
        res.json(engagement);
    } catch (error) {
        res.status(500).json({ error: error.message, connected: false });
    }
});

// Add new document to a collection
app.post('/api/collection/:name', requireConnection, async (req, res) => {
    try {
        const collectionName = req.params.name;
        const document = req.body;
        const result = await db.collection(collectionName).insertOne(document);
        res.json({ success: true, insertedId: result.insertedId });
    } catch (error) {
        res.status(500).json({ error: error.message, connected: false });
    }
});

// Serve main page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
async function startServer() {
    const connected = await connectDB();

    app.listen(PORT, () => {
        console.log(`
============================================================
   NETFLIX ANALYTICS DASHBOARD
============================================================
   Servidor corriendo en: http://localhost:${PORT}
   Base de datos: ${connected ? 'MongoDB Atlas (netflix_analytics)' : 'NO CONECTADA'}

   Abre tu navegador en: http://localhost:${PORT}
============================================================
        `);
    });
}

startServer();
