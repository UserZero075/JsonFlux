'use strict';

const io = require('socket.io-client');

/**
 * JsonFluxClient - Cliente oficial de alta concurrencia y tiempo real para JsonFlux en Node.js.
 *
 * Mejoras:
 * - Soporte de correlación de peticiones por reqId y Socket.IO ACK callbacks (cero colisiones en paralelo).
 * - Suscripciones en tiempo real a cambios de modelos (subscribe/unsubscribe con eventos 'flux:change').
 * - Métodos amigables de alto nivel para CRUD, consultas avanzadas y métricas.
 * - Soporte de token de autenticación y reintentos automáticos de reconexión.
 * - 100% retrocompatible con DatabaseClient y el método clásico .DB(operation, params).
 */
class JsonFluxClient {
  constructor(url = 'http://localhost:3030', options = {}) {
    this.url = url;
    this.token = options.token || null;
    this.timeout = options.timeout || 10000;
    this.connected = false;

    // Mapa de promesas pendientes indexadas por reqId para llamadas concurrentes
    this._pendingRequests = new Map();
    this._modelSubscribers = new Map(); // model -> Set(callback)

    const socketOpts = {
      autoConnect: false,
      reconnection: true,
      reconnectionAttempts: 15,
      reconnectionDelay: 1000,
      auth: this.token ? { token: this.token } : {},
      ...options.socketOptions,
    };

    this.socket = io(this.url, socketOpts);

    this._setupInternalListeners();
  }

  _setupInternalListeners() {
    this.socket.on('connect', () => {
      this.connected = true;
      // Re-suscribirse a modelos activos tras reconexión
      for (const model of this._modelSubscribers.keys()) {
        this.socket.emit('subscribe', model);
      }
    });

    this.socket.on('disconnect', () => {
      this.connected = false;
    });

    // Escuchador de cambios en tiempo real
    this.socket.on('flux:change', (changeEvent) => {
      if (!changeEvent || !changeEvent.model) return;
      const subs = this._modelSubscribers.get(changeEvent.model);
      if (subs && subs.size > 0) {
        for (const cb of subs) {
          try {
            cb(changeEvent);
          } catch (err) {
            console.error(`[JsonFluxClient] Error en callback de suscriptor:`, err);
          }
        }
      }
    });
  }

  connect() {
    return new Promise((resolve, reject) => {
      if (this.socket.connected) {
        this.connected = true;
        return resolve(this);
      }

      const onConnect = () => {
        this.socket.off('connect_error', onError);
        this.connected = true;
        resolve(this);
      };

      const onError = (err) => {
        this.socket.off('connect', onConnect);
        reject(err);
      };

      this.socket.once('connect', onConnect);
      this.socket.once('connect_error', onError);
      this.socket.connect();
    });
  }

  disconnect() {
    return new Promise((resolve) => {
      if (!this.socket.connected) {
        this.connected = false;
        return resolve();
      }
      this.socket.once('disconnect', () => {
        this.connected = false;
        resolve();
      });
      this.socket.disconnect();
    });
  }

  /**
   * Ejecutor central seguro con soporte de ACK callback, correlación reqId y fallback de eventos.
   */
  execute(operation, params = {}) {
    return new Promise((resolve, reject) => {
      if (!this.connected) {
        return reject(new Error('No conectado al servidor JsonFlux'));
      }

      const reqId = `req_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
      const payload = { ...params, reqId };

      let settled = false;
      const timer = setTimeout(() => {
        if (!settled) {
          settled = true;
          this.socket.off(`response:${reqId}`, onCorrelatedResponse);
          this._pendingRequests.delete(reqId);
          reject(new Error(`Timeout en operación '${operation}' tras ${this.timeout}ms`));
        }
      }, this.timeout);

      const finish = (result) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.socket.off(`response:${reqId}`, onCorrelatedResponse);
        this._pendingRequests.delete(reqId);

        let data = result;
        if (typeof data === 'string') {
          try { data = JSON.parse(data); } catch (e) {}
        }

        if (data && data.error) {
          return reject(new Error(data.error));
        }
        resolve(data);
      };

      // 1. Escuchar respuesta correlacionada exacta
      const onCorrelatedResponse = (res) => finish(res);
      this.socket.once(`response:${reqId}`, onCorrelatedResponse);
      this._pendingRequests.set(reqId, { finish });

      // 2. Emitir con Socket.IO ACK callback nativo
      this.socket.emit(operation, payload, (ackResponse) => {
        if (ackResponse !== undefined) {
          finish(ackResponse);
        }
      });
    });
  }

  // Compatibilidad con el método clásico .DB(operation, params)
  DB(operation, params = {}) {
    return this.execute(operation, params);
  }

  // ---------------------------------------------------------------------------
  // Suscripciones en Tiempo Real (Real-Time Flux)
  // ---------------------------------------------------------------------------

  subscribe(modelName, callback) {
    if (!this._modelSubscribers.has(modelName)) {
      this._modelSubscribers.set(modelName, new Set());
      if (this.connected) {
        this.socket.emit('subscribe', modelName);
      }
    }
    this._modelSubscribers.get(modelName).add(callback);
    return () => this.unsubscribe(modelName, callback);
  }

  unsubscribe(modelName, callback) {
    if (!this._modelSubscribers.has(modelName)) return;
    const set = this._modelSubscribers.get(modelName);
    if (callback) {
      set.delete(callback);
    } else {
      set.clear();
    }

    if (set.size === 0) {
      this._modelSubscribers.delete(modelName);
      if (this.connected) {
        this.socket.emit('unsubscribe', modelName);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Métodos de Alto Nivel
  // ---------------------------------------------------------------------------

  addModel(name, schema) {
    return this.execute('addModel', { name, data: schema });
  }

  getModel(name) {
    return this.execute('getModel', { name });
  }

  getModels() {
    return this.execute('getModels', {});
  }

  create(model, data) {
    return this.execute('create', { name: model, data });
  }

  find(model, query = {}, options = {}) {
    return this.execute('find', {
      name: model,
      where: query,
      limit: options.limit,
      skip: options.skip,
      sort: options.sort,
    });
  }

  findOne(model, query = {}) {
    return this.execute('find', {
      name: model,
      where: query,
      limit: 1,
    });
  }

  findById(model, id) {
    return this.execute('findById', { name: model, index: String(id) });
  }

  count(model, query = null) {
    return this.execute('count', { name: model, where: query });
  }

  update(model, targetOrFilter, data) {
    const params = { name: model, data };
    if (targetOrFilter && targetOrFilter.index) {
      params.last_search = targetOrFilter;
    } else if (typeof targetOrFilter === 'object') {
      params.search = targetOrFilter;
    } else {
      params.index = String(targetOrFilter);
    }
    return this.execute('update', params);
  }

  destroy(model, targetOrFilter) {
    const params = { name: model };
    if (targetOrFilter && targetOrFilter.index) {
      params.last_search = targetOrFilter;
    } else if (typeof targetOrFilter === 'object') {
      params.search = targetOrFilter;
    } else {
      params.index = String(targetOrFilter);
    }
    return this.execute('destroy', params);
  }

  dropModel(name) {
    return this.execute('dropModel', { name });
  }

  stats() {
    return this.execute('stats', {});
  }
}

const Types = Object.freeze({
  NUMBER: 'Number',
  BOOLEAN: 'Boolean',
  STRING: 'String',
  OBJECT: 'Object',
  ARRAY: 'Array',
  ANY: 'Any',
});

module.exports = {
  JsonFluxClient,
  DatabaseClient: JsonFluxClient, // Alias para compatibilidad total
  Types,
};
