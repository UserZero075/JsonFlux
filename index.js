'use strict';

const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');
const path = require('path');
const jsonDbManager = require('./json-db-manager/json-db-manager.js');

//? ---------------------> CONFIGURACIÓN DE ARGUMENTOS <---------------------------- ?//

const argv = yargs(hideBin(process.argv))
  .option('port', {
    alias: 'p',
    type: 'number',
    description: 'Puerto en el que correrá el servidor',
    default: Number(process.env.PORT) || 4000,
  })
  .option('dbpath', {
    alias: 'a',
    type: 'string',
    description: 'Ruta del archivo o carpeta de la base de datos',
    default: process.env.JSONFLUX_DBPATH || path.join(__dirname, 'DB.json'),
  })
  .option('autosave', {
    alias: 'k',
    type: 'number',
    description: 'Intervalo de autoguardado en milisegundos',
    default: 15000,
  })
  .option('logs', {
    alias: 'l',
    type: 'boolean',
    description: 'Habilitar logs de la base de datos',
    default: true,
  })
  .option('token', {
    alias: 't',
    type: 'string',
    description: 'Token de autenticación secreto opcional',
    default: process.env.JSONFLUX_TOKEN || null,
  })
  .option('cors', {
    type: 'string',
    description: 'Orígenes CORS permitidos',
    default: '*',
  })
  .argv;

//? ---------------------> CONFIGURACIÓN INICIAL <---------------------------- ?//

const app = express();
const server = http.createServer(app);

const io = socketIo(server, {
  cors: {
    origin: argv.cors === '*' ? '*' : argv.cors.split(','),
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
  },
  maxHttpBufferSize: 1e8, // 100 MB de payload máximo para operaciones masivas
});

// Resolver ruta de la base de datos
let finalDbPath = argv.dbpath;
if (!finalDbPath.endsWith('.json')) {
  finalDbPath = path.join(finalDbPath, 'DB.json');
}

let db = jsonDbManager(finalDbPath, {
  autosave: argv.autosave,
  logs: argv.logs,
  syncWrites: false,
});

const setDb = (newDb) => {
  db = newDb;
  return db;
};

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Middleware de CORS para Express
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', argv.cors);
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization, x-api-key');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

//? ---------------------> SEGURIDAD Y AUTENTICACIÓN <---------------------------- ?//

const checkAuth = (tokenProvided) => {
  if (!argv.token) return true; // Modo libre si no hay token configurado
  if (!tokenProvided) return false;
  return String(tokenProvided).trim() === String(argv.token).trim();
};

// Middleware HTTP de Autenticación
const httpAuthMiddleware = (req, res, next) => {
  if (!argv.token) return next();
  const authHeader = req.headers.authorization;
  const token = authHeader && authHeader.startsWith('Bearer ')
    ? authHeader.slice(7)
    : req.headers['x-api-key'] || req.query.token;

  if (checkAuth(token)) {
    return next();
  }
  return res.status(401).json({ error: 'No autorizado: Token de autenticación inválido o ausente.' });
};

// Middleware Socket.IO de Autenticación
io.use((socket, next) => {
  if (!argv.token) return next();
  const token = socket.handshake.auth && socket.handshake.auth.token
    ? socket.handshake.auth.token
    : socket.handshake.headers['x-api-key'] || socket.handshake.query.token;

  if (checkAuth(token)) {
    return next();
  }
  const err = new Error('No autorizado: Token de autenticación inválido.');
  err.data = { status: 401 };
  return next(err);
});

//? ---------------------> PROCESADOR DE SOLICITUDES <---------------------------- ?//

const handleRequest = (operation, rawParams) => {
  let params = rawParams;
  if (typeof params === 'string') {
    try {
      params = JSON.parse(params);
    } catch (e) {
      params = {};
    }
  }
  params = params || {};

  const {
    name,
    model = name,
    data,
    param,
    equal,
    number,
    limit = number,
    skip = 0,
    sort,
    where,
    search,
    value,
    last_search,
    id,
    index = id,
  } = params;

  try {
    switch (operation) {
      case 'addModel': {
        if (!model) return { error: 'El nombre del modelo es obligatorio' };
        const modelData = Array.isArray(data) && data.length === 1 ? data[0] : data;
        return db.addModel(model, modelData);
      }

      case 'getModel': {
        if (!model) return { error: 'El nombre del modelo es obligatorio' };
        const foundModel = db.getModel(model);
        return foundModel ? foundModel : { error: `El modelo ${model} no existe` };
      }

      case 'getModels': {
        return db.getModels();
      }

      case 'create': {
        if (!model) return { error: 'El nombre del modelo es obligatorio' };
        const payload = Array.isArray(data) && data.length === 1 ? data[0] : data;
        const result = db.create(model, payload);
        if (result) {
          // Emitir evento en tiempo real a suscriptores del modelo
          io.to(model).emit('flux:change', {
            model,
            action: 'create',
            data: result,
            timestamp: Date.now(),
          });
          io.emit('flux:event', { model, action: 'create', id: result.index, timestamp: Date.now() });
          return result;
        }
        return { error: `No se pudo crear el registro en ${model}. Verifique el esquema y restricciones.` };
      }

      case 'find': {
        if (!model) return { error: 'El nombre del modelo es obligatorio' };
        let queryOpts = {};

        if (limit !== undefined && limit !== null) {
          queryOpts.limit = Number(limit);
        }
        if (skip) queryOpts.skip = Number(skip);
        if (sort) queryOpts.sort = sort;

        if (where) {
          queryOpts.where = where;
        } else if (param !== undefined && equal !== undefined) {
          queryOpts.where = { [param]: equal };
        } else if (search && typeof search === 'object') {
          if (search.param !== undefined && search.value !== undefined) {
            queryOpts.where = { [search.param]: search.value };
          } else {
            queryOpts.where = search;
          }
        }

        return db.find(model, queryOpts);
      }

      case 'findById': {
        if (!model || !index) return { error: 'Se requiere modelo e index/id' };
        return db.findById(model, index);
      }

      case 'count': {
        if (!model) return { error: 'El nombre del modelo es obligatorio' };
        return { count: db.count(model, where) };
      }

      case 'update': {
        if (!model) return { error: 'El nombre del modelo es obligatorio' };

        let target = last_search;
        if (!target) {
          if (index) {
            target = { index };
          } else if (search) {
            const searchWhere = search.param && search.value ? { [search.param]: search.value } : search;
            target = db.find(model, { where: searchWhere, limit: 1 });
          } else if (param && equal) {
            target = db.find(model, { where: { [param]: equal }, limit: 1 });
          }
        }

        if (!target) {
          return { error: 'No se encontró el registro objetivo para actualizar.' };
        }

        const updateData = Array.isArray(data) && data.length === 1 ? data[0] : data;
        const updated = db.update(model, target, updateData);
        if (updated) {
          io.to(model).emit('flux:change', {
            model,
            action: 'update',
            data: updated,
            timestamp: Date.now(),
          });
          io.emit('flux:event', { model, action: 'update', id: updated.index, timestamp: Date.now() });
          return updated;
        }
        return { error: `Fallo al actualizar el registro en ${model}.` };
      }

      case 'destroy': {
        if (!model) return { error: 'El nombre del modelo es obligatorio' };

        let toDestroy = last_search;
        if (!toDestroy) {
          if (index) {
            toDestroy = { index };
          } else if (search && value) {
            // Resolver búsqueda sin el bug de indexación [0]
            toDestroy = db.find(model, { where: { [search]: value }, limit: 1 });
          } else if (search && typeof search === 'object') {
            toDestroy = db.find(model, { where: search, limit: 1 });
          }
        }

        if (!toDestroy) {
          return { error: 'No se encontró el registro para eliminar.' };
        }

        const targetIndex = toDestroy.index || toDestroy;
        const ok = db.destroy(model, toDestroy);
        if (ok) {
          io.to(model).emit('flux:change', {
            model,
            action: 'destroy',
            id: targetIndex,
            timestamp: Date.now(),
          });
          io.emit('flux:event', { model, action: 'destroy', id: targetIndex, timestamp: Date.now() });
          return { message: `Registro ${targetIndex} eliminado con éxito.` };
        }
        return { error: `Fallo al eliminar el registro en ${model}.` };
      }

      case 'dropModel': {
        if (!model) return { error: 'El nombre del modelo es obligatorio' };
        const dropped = db.drop(model);
        if (dropped) {
          io.to(model).emit('flux:change', {
            model,
            action: 'drop',
            timestamp: Date.now(),
          });
          io.emit('flux:event', { model, action: 'drop', timestamp: Date.now() });
          return { message: `El modelo ${model} ha sido eliminado.` };
        }
        return { error: `No se pudo eliminar el modelo ${model} o no existe.` };
      }

      case 'stats': {
        return {
          status: 'Active',
          ...db.stats(),
          uptimeSecs: Math.floor(process.uptime()),
          port: argv.port,
        };
      }

      default:
        return { error: `Operación '${operation}' no válida` };
    }
  } catch (error) {
    console.error(`Error en operación ${operation}:`, error);
    return { error: `Error en operación ${operation}: ${error.message}` };
  }
};

//? ---------------------> GESTOR DE CONEXIONES SOCKET.IO <---------------------------- ?//

io.on('connection', (socket) => {
  const clientAddr = socket.handshake.address;
  if (argv.logs) console.log(`[Socket.IO] Cliente conectado: ${socket.id} (${clientAddr})`);

  // Canales de suscripción en tiempo real
  socket.on('subscribe', (modelName, callback) => {
    if (typeof modelName === 'string') {
      socket.join(modelName);
      if (argv.logs) console.log(`[Socket.IO] ${socket.id} suscrito a '${modelName}'`);
      if (typeof callback === 'function') callback({ status: 'subscribed', model: modelName });
    }
  });

  socket.on('unsubscribe', (modelName, callback) => {
    if (typeof modelName === 'string') {
      socket.leave(modelName);
      if (argv.logs) console.log(`[Socket.IO] ${socket.id} desuscrito de '${modelName}'`);
      if (typeof callback === 'function') callback({ status: 'unsubscribed', model: modelName });
    }
  });

  // Operaciones RPC soportadas
  const operations = [
    'addModel', 'getModel', 'getModels',
    'create', 'find', 'findById', 'count',
    'update', 'destroy', 'dropModel', 'stats',
  ];

  operations.forEach((operation) => {
    socket.on(operation, (rawParams, callback) => {
      try {
        let params = rawParams;
        if (typeof params === 'string') {
          try { params = JSON.parse(params); } catch (e) { params = {}; }
        }
        params = params || {};

        const reqId = params.reqId || null;
        const result = handleRequest(operation, params);

        // 1. Si el cliente usó ACK callback (estándar moderno de Socket.IO)
        if (typeof callback === 'function') {
          callback(result);
        }

        // 2. Respuesta por evento correlacionada con reqId (para clientes paralelos)
        if (reqId) {
          socket.emit(`response:${reqId}`, result);
        }

        // 3. Emisión de compatibilidad legada
        socket.emit('response', JSON.stringify(result));
      } catch (err) {
        const errPayload = { error: `Error al procesar la solicitud: ${err.message}` };
        if (typeof callback === 'function') callback(errPayload);
        socket.emit('response', JSON.stringify(errPayload));
      }
    });
  });

  socket.on('disconnect', () => {
    if (argv.logs) console.log(`[Socket.IO] Cliente desconectado: ${socket.id}`);
  });
});

//? ---------------------> RUTAS HTTP REST <---------------------------- ?//

// Estado general y Healthcheck
app.get('/', (req, res) => {
  res.status(200).json({
    name: 'JsonFlux',
    status: 'Active',
    version: '2.0.0',
    uptime: Math.floor(process.uptime()),
    authRequired: Boolean(argv.token),
    stats: db.stats(),
  });
});

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok', uptime: process.uptime() });
});

// Rutas modernas REST protegidas
app.get('/api/models', httpAuthMiddleware, (req, res) => {
  res.json(db.getModels());
});

app.get('/api/models/:name', httpAuthMiddleware, (req, res) => {
  const m = db.getModel(req.params.name);
  if (!m) return res.status(404).json({ error: 'Modelo no encontrado' });
  res.json(m);
});

app.get('/api/data/:model', httpAuthMiddleware, (req, res) => {
  const results = db.find(req.params.model, {
    limit: req.query.limit ? Number(req.query.limit) : undefined,
    skip: req.query.skip ? Number(req.query.skip) : undefined,
  });
  res.json(results);
});

// Handler POST genérico para compatibilidad retroactiva
const postHandler = (req, res) => {
  try {
    let params = req.body;
    if (params && params.params) {
      params = typeof params.params === 'object' ? params.params : JSON.parse(params.params);
    }
    const operation = req.path.replace(/^\//, '');
    const result = handleRequest(operation, params);
    if (result && result.error) {
      return res.status(400).json(result);
    }
    res.json(result);
  } catch (error) {
    res.status(400).json({ error: `Error al procesar solicitud HTTP: ${error.message}` });
  }
};

['addModel', 'getModel', 'getModels', 'create', 'find', 'findById', 'count', 'update', 'destroy', 'dropModel', 'stats'].forEach((op) => {
  app.post(`/${op}`, httpAuthMiddleware, postHandler);
});

//? ---------------------> APAGADO LIMPIO (GRACEFUL SHUTDOWN) <---------------------------- ?//

const shutdownGracefully = (signal) => {
  console.log(`\n[JsonFlux] Recibida señal ${signal}. Vaciando datos a disco y cerrando servidor...`);
  try {
    db.saveSync();
  } catch (err) {
    console.error('Error durante guardado final:', err);
  }
  server.close(() => {
    console.log('[JsonFlux] Servidor HTTP/Socket detenido de forma segura.');
    process.exit(0);
  });
  // Forzar salida si no cierra en 3 segundos
  setTimeout(() => process.exit(0), 3000).unref();
};

process.on('SIGINT', () => shutdownGracefully('SIGINT'));
process.on('SIGTERM', () => shutdownGracefully('SIGTERM'));

//? ---------------------> INICIO DEL SERVIDOR <---------------------------- ?//

const startServer = (port = argv.port, customDbPath = null) => {
  if (customDbPath) {
    db = jsonDbManager(customDbPath, { autosave: argv.autosave, logs: false, syncWrites: true });
    finalDbPath = customDbPath;
  }
  return new Promise((resolve) => {
    server.listen(port, () => {
      console.log('===========================================================');
      console.log(`  🚀 Servidor JsonFlux v2.0 iniciado en puerto ${port}`);
      console.log(`  📦 Base de datos: ${finalDbPath}`);
      console.log(`  ⚡ Autoguardado: cada ${argv.autosave} ms`);
      console.log(`  🔒 Autenticación: ${argv.token ? 'ACTIVADA (Token requerido)' : 'DESACTIVADA (Modo desarrollo)'}`);
      console.log(`  📝 Logs: ${argv.logs ? 'Habilitados' : 'Deshabilitados'}`);
      console.log('===========================================================');
      resolve(server);
    });
  });
};

if (require.main === module) {
  startServer();
}

module.exports = { app, server, io, getDb: () => db, setDb, handleRequest, startServer };
