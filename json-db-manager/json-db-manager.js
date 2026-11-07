'use strict';

const fs = require('fs');
const path = require('path');

/**
 * JsonDbManager - Motor de almacenamiento JSON en memoria con persistencia atómica y consultas avanzadas.
 *
 * Características:
 * - Persistencia crash-safe con escrituras atómicas (tmp + rename) y respaldo de seguridad (.bak).
 * - Indexación en memoria O(1) para búsquedas por índice y campos únicos.
 * - Soporte de operadores avanzados de consulta ($eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $regex, $contains).
 * - Esquemas y metadatos persistentes en __METADATA__ (sobreviven reinicios sin perder reglas ni restricciones).
 * - Detección de modificaciones (_isDirty) para evitar escrituras redundantes en disco.
 * - Guardado síncrono garantizado en eventos de salida del proceso (SIGINT, SIGTERM, beforeExit).
 */
const jsonDbManager = (dbPath = path.join(__dirname, 'DB.json'), opts = {}) => {
  const instance = {};

  instance.path = path.resolve(dbPath);
  instance.opts = opts;
  instance.logs = opts.logs !== undefined ? Boolean(opts.logs) : true;
  instance.autosaveMs = opts.autosave ? Number(opts.autosave) : 15000;
  instance.syncWrites = Boolean(opts.syncWrites);

  // Tipos de datos soportados
  instance.Types = Object.freeze({
    NUMBER: 'Number',
    BOOLEAN: 'Boolean',
    STRING: 'String',
    OBJECT: 'Object',
    ARRAY: 'Array',
    ANY: 'Any',
  });

  // Estado interno en memoria
  let _data = {
    __METADATA__: {
      schemas: {},
      counters: {},
      uniques: {},
      version: '2.0.0',
    },
  };
  let _models = {};
  let _isDirty = false;
  let _autosaveTimer = null;
  let _debounceTimer = null;
  let _isSaving = false;

  // Índices en memoria para búsquedas O(1): _indexes[model][field][value] = Set(index)
  const _indexes = new Map();

  instance.log = (msg) => {
    if (instance.logs) {
      console.log(`[jsonDbManager] ${msg}`);
    }
  };

  // ---------------------------------------------------------------------------
  // Utilidades internas y saneamiento
  // ---------------------------------------------------------------------------

  const isPlainObject = (val) => {
    return val !== null && typeof val === 'object' && !Array.isArray(val);
  };

  const sanitizeKey = (key) => {
    if (typeof key !== 'string') return String(key);
    if (key === '__proto__' || key === 'constructor' || key === 'prototype') {
      throw new Error(`Acceso denegado a propiedad restringida: ${key}`);
    }
    return key;
  };

  const clone = (obj) => {
    if (obj === undefined || obj === null) return obj;
    if (typeof structuredClone === 'function') {
      try {
        return structuredClone(obj);
      } catch (e) {
        // Fallback si contiene funciones
      }
    }
    return JSON.parse(JSON.stringify(obj));
  };

  const validateFieldType = (val, expectedType, allowNull = false) => {
    if (val === undefined || val === null) {
      return allowNull;
    }
    if (expectedType === 'Any') return true;

    switch (expectedType) {
      case 'String':
        return typeof val === 'string';
      case 'Number':
        return typeof val === 'number' && !Number.isNaN(val);
      case 'Boolean':
        return typeof val === 'boolean';
      case 'Array':
        return Array.isArray(val);
      case 'Object':
        return isPlainObject(val);
      default:
        return val.constructor && val.constructor.name === expectedType;
    }
  };

  // ---------------------------------------------------------------------------
  // Gestión de Índices en Memoria
  // ---------------------------------------------------------------------------

  const getModelIndexes = (modelName) => {
    if (!_indexes.has(modelName)) {
      _indexes.set(modelName, new Map());
    }
    return _indexes.get(modelName);
  };

  const indexRecord = (modelName, record) => {
    if (!record || !record.index) return;
    const modelIdx = getModelIndexes(modelName);

    // Indexar por 'index' primario siempre
    if (!modelIdx.has('index')) modelIdx.set('index', new Map());
    modelIdx.get('index').set(String(record.index), record);

    // Indexar campos únicos del modelo
    const schema = _models[modelName];
    if (schema) {
      for (const field of Object.keys(schema)) {
        if (schema[field] && schema[field].unique && record[field] !== undefined) {
          if (!modelIdx.has(field)) modelIdx.set(field, new Map());
          modelIdx.get(field).set(record[field], record);
        }
      }
    }
  };

  const deindexRecord = (modelName, record) => {
    if (!record || !record.index) return;
    const modelIdx = getModelIndexes(modelName);

    if (modelIdx.has('index')) {
      modelIdx.get('index').delete(String(record.index));
    }

    const schema = _models[modelName];
    if (schema) {
      for (const field of Object.keys(schema)) {
        if (schema[field] && schema[field].unique && record[field] !== undefined) {
          if (modelIdx.has(field)) {
            modelIdx.get(field).delete(record[field]);
          }
        }
      }
    }
  };

  const rebuildAllIndexes = () => {
    _indexes.clear();
    for (const model of Object.keys(_data)) {
      if (model.startsWith('__')) continue;
      const records = _data[model];
      if (isPlainObject(records)) {
        for (const idx of Object.keys(records)) {
          indexRecord(model, records[idx]);
        }
      }
    }
  };

  // ---------------------------------------------------------------------------
  // Persistencia Atómica y Respaldo
  // ---------------------------------------------------------------------------

  instance.save = () => {
    if (!_isDirty) return;
    _scheduleDebouncedSave();
  };

  const _scheduleDebouncedSave = () => {
    if (_debounceTimer) clearTimeout(_debounceTimer);
    _debounceTimer = setTimeout(() => {
      instance.saveSync();
    }, 150);
  };

  instance.saveSync = () => {
    if (!_isDirty && fs.existsSync(instance.path)) return;
    if (_isSaving) return;
    _isSaving = true;

    try {
      const dir = path.dirname(instance.path);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      // Sincronizar metadatos antes de serializar
      _data.__METADATA__.schemas = _models;

      const serialized = JSON.stringify(_data, null, 2);
      const tmpPath = `${instance.path}.tmp.${Date.now()}`;
      const bakPath = `${instance.path}.bak`;

      // 1. Escribir a archivo temporal
      fs.writeFileSync(tmpPath, serialized, 'utf-8');

      // 2. Crear respaldo antes de reemplazar el archivo principal
      if (fs.existsSync(instance.path)) {
        try {
          fs.copyFileSync(instance.path, bakPath);
        } catch (copyErr) {
          // Ignorar fallo menor en backup si el archivo está bloqueado temporalmente
        }
      }

      // 3. Renombrado atómico (POSIX rename)
      fs.renameSync(tmpPath, instance.path);

      _isDirty = false;
      instance.log('Persistencia completada en disco exitosamente.');
    } catch (err) {
      instance.log(`Error al guardar datos: ${err.message}`);
    } finally {
      _isSaving = false;
    }
  };

  // ---------------------------------------------------------------------------
  // Carga e Inicialización
  // ---------------------------------------------------------------------------

  const initializeDatabase = () => {
    try {
      if (!fs.existsSync(instance.path)) {
        // Comprobar si existe un respaldo previo por caída abrupta
        const bakPath = `${instance.path}.bak`;
        if (fs.existsSync(bakPath)) {
          instance.log('Recuperando base de datos desde respaldo .bak...');
          fs.copyFileSync(bakPath, instance.path);
        } else {
          const dir = path.dirname(instance.path);
          if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
          fs.writeFileSync(instance.path, JSON.stringify(_data, null, 2), 'utf-8');
        }
      }

      const content = fs.readFileSync(instance.path, 'utf-8');
      _data = JSON.parse(content);

      // Normalizar estructura interna
      if (!_data.__METADATA__) {
        _data.__METADATA__ = {
          schemas: {},
          counters: _data.__UNIQUES__ && _data.__UNIQUES__._counters ? _data.__UNIQUES__._counters : {},
          uniques: _data.__UNIQUES__ || {},
          version: '2.0.0',
        };
      }
      if (!_data.__METADATA__.schemas) _data.__METADATA__.schemas = {};
      if (!_data.__METADATA__.counters) _data.__METADATA__.counters = {};
      if (!_data.__METADATA__.uniques) _data.__METADATA__.uniques = {};

      // Cargar modelos persistidos desde __METADATA__.schemas
      _models = clone(_data.__METADATA__.schemas);

      // Compatibilidad con bases de datos legadas que no tenían esquemas explícitos
      for (const key of Object.keys(_data)) {
        if (!key.startsWith('__') && !_models[key]) {
          _models[key] = {};
          const items = Object.values(_data[key]);
          if (items.length > 0 && isPlainObject(items[0])) {
            const sample = items[0];
            for (const f of Object.keys(sample)) {
              if (['index', '_id', 'createdAt', 'updatedAt'].includes(f)) continue;
              _models[key][f] = {
                type: sample[f] ? sample[f].constructor.name : 'String',
                allowNull: true,
              };
            }
          }
        }
      }

      rebuildAllIndexes();
      console.log('Conexión a la base de datos exitosa.');
    } catch (err) {
      throw new Error(`Error al inicializar la base de datos: ${err.message}`);
    }

    // Configurar temporizador de autoguardado si procede
    if (instance.autosaveMs > 0) {
      if (isNaN(instance.autosaveMs)) throw new Error('El autoguardado debe ser un valor de tipo Int');
      _autosaveTimer = setInterval(() => {
        if (_isDirty) instance.saveSync();
      }, instance.autosaveMs);
      if (_autosaveTimer.unref) _autosaveTimer.unref();
    }
  };

  // ---------------------------------------------------------------------------
  // API del Administrador de Base de Datos
  // ---------------------------------------------------------------------------

  instance.addModel = (modelName, modelSchema) => {
    modelName = sanitizeKey(modelName);
    if (!isPlainObject(modelSchema)) {
      throw new Error(`El esquema para el modelo ${modelName} debe ser un objeto.`);
    }

    if (!_data[modelName]) _data[modelName] = {};
    if (!_data.__METADATA__.uniques[modelName]) _data.__METADATA__.uniques[modelName] = {};
    if (!_data.__METADATA__.counters[modelName]) {
      const existingKeys = Object.keys(_data[modelName]).map(Number).filter((n) => !Number.isNaN(n));
      _data.__METADATA__.counters[modelName] = existingKeys.length > 0 ? Math.max(...existingKeys) : 0;
    }

    // Normalizar definición del esquema
    const normalized = {};
    for (const field of Object.keys(modelSchema)) {
      sanitizeKey(field);
      const conf = modelSchema[field];
      if (typeof conf === 'string') {
        normalized[field] = { type: conf, allowNull: true, unique: false };
      } else if (isPlainObject(conf)) {
        normalized[field] = {
          type: conf.type || 'String',
          allowNull: conf.allowNull !== undefined ? Boolean(conf.allowNull) : true,
          unique: Boolean(conf.unique),
          default: conf.default !== undefined ? conf.default : undefined,
        };
      } else {
        normalized[field] = { type: 'String', allowNull: true, unique: false };
      }

      if (normalized[field].unique && !_data.__METADATA__.uniques[modelName][field]) {
        _data.__METADATA__.uniques[modelName][field] = [];
      }
    }

    _models[modelName] = normalized;
    _data.__METADATA__.schemas[modelName] = normalized;
    _isDirty = true;
    if (instance.syncWrites) {
      instance.saveSync();
    } else {
      instance.save();
    }

    return { message: `Modelo ${modelName} registrado con éxito.` };
  };

  instance.getModel = (modelName) => {
    modelName = sanitizeKey(modelName);
    if (!_models[modelName]) return null;
    return clone(_models[modelName]);
  };

  instance.getModels = () => {
    return Object.keys(_models).map((name) => ({
      name,
      fields: Object.keys(_models[name]),
      count: _data[name] ? Object.keys(_data[name]).length : 0,
    }));
  };

  instance.create = (modelName, data) => {
    const t0 = Date.now();
    modelName = sanitizeKey(modelName);

    if (!_models[modelName]) {
      instance.log(`El modelo ${modelName} no existe`);
      return null;
    }
    if (!isPlainObject(data)) {
      instance.log('Los datos a crear deben ser un objeto');
      return null;
    }

    const schema = _models[modelName];
    const record = {};
    const uniquesToAdd = {};

    for (const field of Object.keys(schema)) {
      const rule = schema[field];
      let val = data[field];

      // Aplicar valor por defecto si no viene definido
      if (val === undefined && rule.default !== undefined) {
        val = typeof rule.default === 'function' ? rule.default() : rule.default;
      }

      // Validar nulabilidad
      if (rule.allowNull === false && (val === undefined || val === null)) {
        instance.log(`El parámetro ${field} no puede ser nulo`);
        return null;
      }

      // Validar tipo de dato
      if (!validateFieldType(val, rule.type, rule.allowNull)) {
        const received = val === null ? 'null' : typeof val;
        instance.log(`Se requiere un parámetro de tipo ${rule.type} para '${field}', pero se obtuvo ${received}`);
        return null;
      }

      // Validar unicidad
      if (rule.unique && val !== undefined && val !== null) {
        const modelIdx = getModelIndexes(modelName);
        if (modelIdx.has(field) && modelIdx.get(field).has(val)) {
          instance.log(`El campo ${field} se encontró duplicado con el valor ${val}`);
          return null;
        }
        uniquesToAdd[field] = val;
      }

      record[field] = val;
    }

    // Preservar campos adicionales dinámicos que no estén explícitamente en el esquema
    for (const extraField of Object.keys(data)) {
      if (!schema[extraField] && !['index', 'createdAt', 'updatedAt'].includes(extraField)) {
        record[extraField] = data[extraField];
      }
    }

    // Asignación segura y monótona de índice
    if (!_data.__METADATA__.counters[modelName]) {
      _data.__METADATA__.counters[modelName] = 0;
    }
    _data.__METADATA__.counters[modelName] += 1;
    const nextIndex = String(_data.__METADATA__.counters[modelName]);

    record.index = nextIndex;
    record.createdAt = Date.now();

    // Actualizar almacenamiento e índices en memoria
    if (!_data[modelName]) _data[modelName] = {};
    _data[modelName][nextIndex] = record;
    indexRecord(modelName, record);

    // Actualizar historial de uniques para retrocompatibilidad
    for (const uField of Object.keys(uniquesToAdd)) {
      if (!_data.__METADATA__.uniques[modelName][uField]) {
        _data.__METADATA__.uniques[modelName][uField] = [];
      }
      _data.__METADATA__.uniques[modelName][uField].push(uniquesToAdd[uField]);
    }

    _isDirty = true;
    if (instance.syncWrites) {
      instance.saveSync();
    } else {
      instance.save();
    }

    instance.log(`Modelo ${modelName} creado en ${Date.now() - t0}ms`);
    return clone(record);
  };

  /**
   * Evalúa si un registro coincide con los criterios de filtro avanzados
   */
  const matchFilter = (item, query) => {
    if (!query) return true;
    if (typeof query === 'function') return query(item) === true;
    if (!isPlainObject(query)) return false;

    for (const key of Object.keys(query)) {
      const condition = query[key];
      const itemVal = item[key];

      if (isPlainObject(condition)) {
        // Operadores de consulta
        for (const op of Object.keys(condition)) {
          const target = condition[op];
          switch (op) {
            case '$eq':
              if (itemVal !== target) return false;
              break;
            case '$ne':
              if (itemVal === target) return false;
              break;
            case '$gt':
              if (!(itemVal > target)) return false;
              break;
            case '$gte':
              if (!(itemVal >= target)) return false;
              break;
            case '$lt':
              if (!(itemVal < target)) return false;
              break;
            case '$lte':
              if (!(itemVal <= target)) return false;
              break;
            case '$in':
              if (!Array.isArray(target) || !target.includes(itemVal)) return false;
              break;
            case '$nin':
              if (Array.isArray(target) && target.includes(itemVal)) return false;
              break;
            case '$contains':
              if (typeof itemVal === 'string') {
                if (!itemVal.includes(String(target))) return false;
              } else if (Array.isArray(itemVal)) {
                if (!itemVal.includes(target)) return false;
              } else {
                return false;
              }
              break;
            case '$regex':
              const reg = target instanceof RegExp ? target : new RegExp(target, condition.$options || '');
              if (!reg.test(String(itemVal || ''))) return false;
              break;
            default:
              if (itemVal !== condition) return false;
          }
        }
      } else {
        // Comparación directa de igualdad
        if (itemVal != condition) return false;
      }
    }
    return true;
  };

  instance.find = (modelName, opts = {}) => {
    const t0 = Date.now();
    modelName = sanitizeKey(modelName);

    if (!_models[modelName] && !_data[modelName]) {
      instance.log(`No se encontró el modelo ${modelName}`);
      return opts.limit === 1 ? null : [];
    }

    const records = _data[modelName] || {};
    const modelIdx = getModelIndexes(modelName);
    const limit = opts.limit !== undefined ? Number(opts.limit) : Infinity;
    const skip = opts.skip !== undefined ? Math.max(0, Number(opts.skip)) : 0;
    const isSingle = opts.limit === 1;

    // Optimización O(1): Búsqueda por 'index' exacto
    if (opts.where && isPlainObject(opts.where) && opts.where.index !== undefined && Object.keys(opts.where).length === 1) {
      const direct = modelIdx.has('index') ? modelIdx.get('index').get(String(opts.where.index)) : records[String(opts.where.index)];
      if (direct) {
        return isSingle ? clone(direct) : [clone(direct)];
      }
      return isSingle ? null : [];
    }

    let results = [];
    let skipped = 0;

    for (const id of Object.keys(records)) {
      const item = records[id];
      if (opts.where) {
        const matches = typeof opts.where === 'function' ? opts.where(item) === true : matchFilter(item, opts.where);
        if (matches) {
          if (skipped < skip) {
            skipped++;
            continue;
          }
          results.push(item);
          if (results.length >= limit) break;
        }
      } else {
        if (skipped < skip) {
          skipped++;
          continue;
        }
        results.push(item);
        if (results.length >= limit) break;
      }
    }

    // Ordenamiento si se especifica
    if (opts.sort && isPlainObject(opts.sort)) {
      const sortField = Object.keys(opts.sort)[0];
      const sortDir = opts.sort[sortField] === -1 ? -1 : 1;
      results.sort((a, b) => {
        if (a[sortField] < b[sortField]) return -1 * sortDir;
        if (a[sortField] > b[sortField]) return 1 * sortDir;
        return 0;
      });
    }

    instance.log(`Búsqueda de ${results.length} resultados en ${Date.now() - t0}ms`);

    if (isSingle) {
      return results.length > 0 ? clone(results[0]) : null;
    }
    return clone(results);
  };

  instance.findById = (modelName, id) => {
    return instance.find(modelName, { where: { index: String(id) }, limit: 1 });
  };

  instance.count = (modelName, query = null) => {
    modelName = sanitizeKey(modelName);
    if (!_data[modelName]) return 0;
    if (!query) return Object.keys(_data[modelName]).length;

    let count = 0;
    const records = _data[modelName];
    for (const id of Object.keys(records)) {
      if (matchFilter(records[id], query)) count++;
    }
    return count;
  };

  instance.update = (modelName, modelToUpdate, dataToApply) => {
    const t0 = Date.now();
    modelName = sanitizeKey(modelName);

    if (!_data[modelName]) {
      instance.log(`El modelo ${modelName} no existe`);
      return null;
    }

    // Resolver identificador del registro
    let targetIndex = null;
    if (isPlainObject(modelToUpdate) && modelToUpdate.index) {
      targetIndex = String(modelToUpdate.index);
    } else if (typeof modelToUpdate === 'string' || typeof modelToUpdate === 'number') {
      targetIndex = String(modelToUpdate);
    } else if (Array.isArray(modelToUpdate) && modelToUpdate.length > 0 && modelToUpdate[0].index) {
      targetIndex = String(modelToUpdate[0].index);
    }

    if (!targetIndex || !_data[modelName][targetIndex]) {
      instance.log(`El modelo o índice no existen para actualizar`);
      return null;
    }

    const currentRecord = _data[modelName][targetIndex];
    const schema = _models[modelName] || {};
    const updated = { ...currentRecord };

    // Validar y aplicar cambios
    for (const field of Object.keys(dataToApply)) {
      if (['index', 'createdAt'].includes(field)) continue;

      const val = dataToApply[field];
      const rule = schema[field];

      if (rule) {
        if (!validateFieldType(val, rule.type, rule.allowNull)) {
          const received = val === null ? 'null' : typeof val;
          instance.log(`Tipo incorrecto para '${field}': se esperaba ${rule.type}, recibido ${received}`);
          return null;
        }

        // Si es campo único y su valor cambió, validar unicidad
        if (rule.unique && val !== currentRecord[field] && val !== undefined && val !== null) {
          const modelIdx = getModelIndexes(modelName);
          if (modelIdx.has(field) && modelIdx.get(field).has(val)) {
            instance.log(`El campo único ${field} ya existe con el valor ${val}`);
            return null;
          }
        }
      }

      updated[field] = val;
    }

    updated.updatedAt = Date.now();

    // Actualizar índices en memoria
    deindexRecord(modelName, currentRecord);
    _data[modelName][targetIndex] = updated;
    indexRecord(modelName, updated);

    _isDirty = true;
    if (instance.syncWrites) {
      instance.saveSync();
    } else {
      instance.save();
    }

    instance.log(`Modelo ${modelName} [${targetIndex}] actualizado en ${Date.now() - t0}ms`);
    return clone(updated);
  };

  instance.destroy = (modelName, modelData) => {
    const t0 = Date.now();
    modelName = sanitizeKey(modelName);

    if (!_data[modelName]) {
      instance.log(`El modelo ${modelName} no existe`);
      return false;
    }

    let targetIndex = null;
    if (isPlainObject(modelData) && modelData.index) {
      targetIndex = String(modelData.index);
    } else if (typeof modelData === 'string' || typeof modelData === 'number') {
      targetIndex = String(modelData);
    } else if (Array.isArray(modelData) && modelData.length > 0 && modelData[0].index) {
      targetIndex = String(modelData[0].index);
    }

    if (!targetIndex || !_data[modelName][targetIndex]) {
      instance.log(`El modelo o índice no existen para eliminar`);
      return false;
    }

    const recordToDelete = _data[modelName][targetIndex];
    deindexRecord(modelName, recordToDelete);
    delete _data[modelName][targetIndex];

    _isDirty = true;
    if (instance.syncWrites) {
      instance.saveSync();
    } else {
      instance.save();
    }

    instance.log(`Índice ${targetIndex} del modelo ${modelName} eliminado en ${Date.now() - t0}ms`);
    return true;
  };

  instance.drop = (modelName) => {
    const t0 = Date.now();
    modelName = sanitizeKey(modelName);

    if (_data[modelName]) {
      delete _data[modelName];
      delete _models[modelName];
      if (_data.__METADATA__.schemas[modelName]) delete _data.__METADATA__.schemas[modelName];
      if (_data.__METADATA__.uniques[modelName]) delete _data.__METADATA__.uniques[modelName];
      if (_data.__METADATA__.counters[modelName]) delete _data.__METADATA__.counters[modelName];
      _indexes.delete(modelName);

      _isDirty = true;
      instance.saveSync();
      instance.log(`Modelo ${modelName} eliminado completamente en ${Date.now() - t0}ms`);
      return true;
    }
    return false;
  };

  instance.stats = () => {
    const collections = {};
    let totalRecords = 0;

    for (const key of Object.keys(_data)) {
      if (!key.startsWith('__')) {
        const count = Object.keys(_data[key]).length;
        collections[key] = count;
        totalRecords += count;
      }
    }

    return {
      version: _data.__METADATA__.version || '2.0.0',
      totalCollections: Object.keys(collections).length,
      totalRecords,
      collections,
      isDirty: _isDirty,
      memoryUsage: process.memoryUsage().heapUsed,
    };
  };

  // ---------------------------------------------------------------------------
  // Manejo de apagado limpio (Graceful Shutdown)
  // ---------------------------------------------------------------------------

  const cleanupAndSave = () => {
    if (_isDirty) {
      instance.log('Vaciando datos pendientes a disco antes del cierre...');
      instance.saveSync();
    }
    if (_autosaveTimer) clearInterval(_autosaveTimer);
    if (_debounceTimer) clearTimeout(_debounceTimer);
  };

  process.once('beforeExit', cleanupAndSave);
  process.once('exit', cleanupAndSave);

  // Inicializar estado al instanciar
  initializeDatabase();

  // Exponer propiedades necesarias para compatibilidad con scripts existentes
  instance.data = _data;
  instance.models = _models;

  return instance;
};

module.exports = jsonDbManager;
