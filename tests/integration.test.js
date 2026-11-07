'use strict';

const test = require('node:test');
const assert = require('node:assert');
const http = require('http');
const express = require('express');
const socketIo = require('socket.io');
const path = require('path');
const fs = require('fs');

const jsonDbManager = require('../json-db-manager/json-db-manager.js');
const { JsonFluxClient } = require('../clients/Nodejs/database_client.js');

const TEST_PORT = 3131;
const TEST_DB = path.join(__dirname, 'test_integ_db.json');
const AUTH_TOKEN = 'test_secret_token_123';

const cleanup = () => {
  for (const f of [TEST_DB, `${TEST_DB}.bak`]) {
    if (fs.existsSync(f)) {
      try { fs.unlinkSync(f); } catch (e) {}
    }
  }
};

let serverInstance = null;
let ioInstance = null;
let dbInstance = null;

const startServer = () => {
  return new Promise((resolve) => {
    cleanup();
    const app = express();
    serverInstance = http.createServer(app);
    ioInstance = socketIo(serverInstance);

    dbInstance = jsonDbManager(TEST_DB, { autosave: 60000, logs: false, syncWrites: true });

    app.use(express.json());

    // Middleware de autenticación
    ioInstance.use((socket, next) => {
      const token = socket.handshake.auth && socket.handshake.auth.token;
      if (token === AUTH_TOKEN) {
        return next();
      }
      return next(new Error('No autorizado'));
    });

    ioInstance.on('connection', (socket) => {
      socket.on('subscribe', (modelName, callback) => {
        socket.join(modelName);
        if (callback) callback({ status: 'subscribed', model: modelName });
      });

      socket.on('addModel', (params, callback) => {
        const { name, data } = params;
        const res = dbInstance.addModel(name, data);
        if (callback) callback(res);
        socket.emit(`response:${params.reqId}`, res);
      });

      socket.on('create', (params, callback) => {
        const { name, data } = params;
        const res = dbInstance.create(name, data);
        if (res) {
          ioInstance.to(name).emit('flux:change', { model: name, action: 'create', data: res });
        }
        if (callback) callback(res);
        socket.emit(`response:${params.reqId}`, res);
      });

      socket.on('find', (params, callback) => {
        const { name, where, limit } = params;
        const res = dbInstance.find(name, { where, limit });
        if (callback) callback(res);
        socket.emit(`response:${params.reqId}`, res);
      });

      socket.on('update', (params, callback) => {
        const { name, search, data } = params;
        const target = dbInstance.find(name, { where: search, limit: 1 });
        const res = target ? dbInstance.update(name, target, data) : null;
        if (res) {
          ioInstance.to(name).emit('flux:change', { model: name, action: 'update', data: res });
        }
        if (callback) callback(res);
        socket.emit(`response:${params.reqId}`, res);
      });
    });

    serverInstance.listen(TEST_PORT, () => resolve());
  });
};

const stopServer = () => {
  return new Promise((resolve) => {
    if (ioInstance) ioInstance.close();
    if (serverInstance) serverInstance.close(() => resolve());
    else resolve();
  });
};

test('JsonFlux Integration Suite (Network, Auth & Real-Time Flux)', async (t) => {
  await startServer();

  await t.test('1. Conexión rechazada si el token es inválido', async () => {
    const badClient = new JsonFluxClient(`http://localhost:${TEST_PORT}`, {
      token: 'token_incorrecto',
      timeout: 2000,
    });
    await assert.rejects(async () => {
      await badClient.connect();
    }, /No autorizado/);
  });

  await t.test('2. Conexión exitosa con token legítimo', async () => {
    const client = new JsonFluxClient(`http://localhost:${TEST_PORT}`, {
      token: AUTH_TOKEN,
      timeout: 5000,
    });
    await client.connect();
    assert.strictEqual(client.connected, true);
    await client.disconnect();
  });

  await t.test('3. Transmisión reactiva en tiempo real (Real-Time Pub/Sub)', async () => {
    const clientA = new JsonFluxClient(`http://localhost:${TEST_PORT}`, { token: AUTH_TOKEN });
    const clientB = new JsonFluxClient(`http://localhost:${TEST_PORT}`, { token: AUTH_TOKEN });

    await Promise.all([clientA.connect(), clientB.connect()]);

    await clientA.addModel('Orders', {
      item: { type: 'String', allowNull: false },
      price: { type: 'Number', allowNull: false },
    });

    // Client B se suscribe a 'Orders'
    const receivedEvents = [];
    const eventPromise = new Promise((resolve) => {
      clientB.subscribe('Orders', (ev) => {
        receivedEvents.push(ev);
        if (receivedEvents.length >= 2) resolve();
      });
    });

    // Pequeño retardo para confirmar registro de room en Socket.IO
    await new Promise((r) => setTimeout(r, 60));

    // Client A crea dos pedidos
    const o1 = await clientA.create('Orders', { item: 'Laptop', price: 1200 });
    const o2 = await clientA.create('Orders', { item: 'Mouse', price: 25 });

    assert.ok(o1);
    assert.ok(o2);

    // Esperar a que Client B reciba ambos eventos en tiempo real
    await eventPromise;

    assert.strictEqual(receivedEvents.length, 2);
    assert.strictEqual(receivedEvents[0].action, 'create');
    assert.strictEqual(receivedEvents[0].data.item, 'Laptop');
    assert.strictEqual(receivedEvents[1].data.item, 'Mouse');

    await Promise.all([clientA.disconnect(), clientB.disconnect()]);
  });

  await t.test('4. Concurrencia sin colisiones de respuesta (Request ID Correlation)', async () => {
    const client = new JsonFluxClient(`http://localhost:${TEST_PORT}`, { token: AUTH_TOKEN });
    await client.connect();

    // Disparar 50 operaciones en paralelo
    const promises = [];
    for (let i = 1; i <= 50; i++) {
      promises.push(client.create('Orders', { item: `BulkItem_${i}`, price: i * 10 }));
    }

    const results = await Promise.all(promises);
    assert.strictEqual(results.length, 50);

    // Verificar que cada respuesta corresponde exactamente a su petición
    for (let i = 0; i < 50; i++) {
      assert.strictEqual(results[i].item, `BulkItem_${i + 1}`);
      assert.strictEqual(results[i].price, (i + 1) * 10);
    }

    // Comprobar total en base de datos
    const all = await client.find('Orders');
    // 2 iniciales + 50 nuevos = 52
    assert.strictEqual(all.length, 52);

    await client.disconnect();
  });

  await stopServer();
  cleanup();
});
