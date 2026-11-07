'use strict';

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const jsonDbManager = require('../json-db-manager/json-db-manager.js');

const TEST_DB = path.join(__dirname, 'test_unit_db.json');

const cleanup = () => {
  for (const f of [TEST_DB, `${TEST_DB}.bak`]) {
    if (fs.existsSync(f)) {
      try { fs.unlinkSync(f); } catch (e) {}
    }
  }
};

test('JsonDbManager Core Suite', async (t) => {
  cleanup();

  let db = jsonDbManager(TEST_DB, { logs: false, syncWrites: true });

  await t.test('1. Inicialización y creación de modelos', () => {
    db.addModel('Users', {
      username: { type: 'String', allowNull: false, unique: true },
      age: { type: 'Number', allowNull: false },
      isActive: { type: 'Boolean', default: true },
      tags: { type: 'Array', default: ['user'] },
    });

    const models = db.getModels();
    assert.strictEqual(models.length, 1);
    assert.strictEqual(models[0].name, 'Users');
    assert.deepStrictEqual(models[0].fields, ['username', 'age', 'isActive', 'tags']);
  });

  await t.test('2. Creación con validación de tipos, defaults y campos únicos', () => {
    // Creación válida
    const u1 = db.create('Users', { username: 'alice', age: 28 });
    assert.ok(u1);
    assert.strictEqual(u1.username, 'alice');
    assert.strictEqual(u1.age, 28);
    assert.strictEqual(u1.isActive, true); // Valor por defecto
    assert.deepStrictEqual(u1.tags, ['user']); // Valor por defecto
    assert.strictEqual(u1.index, '1');

    // Violación de campo único (username duplicado)
    const dup = db.create('Users', { username: 'alice', age: 30 });
    assert.strictEqual(dup, null, 'No debe permitir username duplicado');

    // Violación de tipo de dato (age como string)
    const badType = db.create('Users', { username: 'bob', age: 'invalid' });
    assert.strictEqual(badType, null, 'No debe permitir tipo incorrecto');

    // Violación de allowNull: false (age ausente)
    const missingField = db.create('Users', { username: 'charlie' });
    assert.strictEqual(missingField, null, 'No debe permitir campo no nulo omitido');

    // Segundo usuario válido
    const u2 = db.create('Users', { username: 'bob', age: 32 });
    assert.ok(u2);
    assert.strictEqual(u2.index, '2');
  });

  await t.test('3. Búsqueda y operadores avanzados ($eq, $gt, $in, $contains)', () => {
    // Crear más datos de prueba
    db.create('Users', { username: 'charlie', age: 19, tags: ['student', 'user'] });
    db.create('Users', { username: 'diana', age: 45, tags: ['admin', 'pro'] });

    // Búsqueda por igualdad
    const foundAlice = db.find('Users', { where: { username: 'alice' } });
    assert.strictEqual(foundAlice.length, 1);
    assert.strictEqual(foundAlice[0].username, 'alice');

    // Operador $gt: alice (28), bob (32), diana (45) -> 3 usuarios > 25
    const over25 = db.find('Users', { where: { age: { $gt: 25 } } });
    assert.strictEqual(over25.length, 3, 'Debe encontrar 3 usuarios mayores de 25');

    // Operador $in
    const specificUsers = db.find('Users', { where: { username: { $in: ['alice', 'diana'] } } });
    assert.strictEqual(specificUsers.length, 2);

    // Operador $contains en array
    const admins = db.find('Users', { where: { tags: { $contains: 'admin' } } });
    assert.strictEqual(admins.length, 1);
    assert.strictEqual(admins[0].username, 'diana');

    // Búsqueda por ID directo O(1)
    const byId = db.findById('Users', '1');
    assert.strictEqual(byId.username, 'alice');

    // Conteo
    const totalCount = db.count('Users');
    assert.strictEqual(totalCount, 4);

    const adultsCount = db.count('Users', { age: { $gte: 30 } });
    assert.strictEqual(adultsCount, 2);
  });

  await t.test('4. Actualización con validación de unicidad', () => {
    // Actualizar edad de alice (index 1)
    const updated = db.update('Users', { index: '1' }, { age: 29 });
    assert.ok(updated);
    assert.strictEqual(updated.age, 29);
    assert.ok(updated.updatedAt);

    // Intentar cambiar username de bob (index 2) a 'alice' (que ya existe)
    const failUpdate = db.update('Users', { index: '2' }, { username: 'alice' });
    assert.strictEqual(failUpdate, null, 'No debe permitir actualizar a un username que ya existe');

    // Comprobar que bob no cambió
    const bob = db.findById('Users', '2');
    assert.strictEqual(bob.username, 'bob');
  });

  await t.test('5. Eliminación de registros y drop de modelo', () => {
    // Eliminar charlie (index 3)
    const destroyed = db.destroy('Users', { index: '3' });
    assert.strictEqual(destroyed, true);
    assert.strictEqual(db.findById('Users', '3'), null);

    // Contar tras eliminar: quedan 3 (alice, bob, diana)
    assert.strictEqual(db.count('Users'), 3);
  });

  await t.test('6. Supervivencia de esquema y restricciones tras reinicio (Fix Fatal Bug)', () => {
    db.saveSync();

    // Nueva instancia simulando reinicio del servidor
    const db2 = jsonDbManager(TEST_DB, { logs: false, syncWrites: true });

    // Los esquemas y modelos deben conservarse intactos
    const models = db2.getModels();
    assert.strictEqual(models.length, 1);
    assert.strictEqual(models[0].name, 'Users');

    // Debe permitir crear nuevos registros SIN el bug de "parámetro index no puede ser nulo"
    const newRecord = db2.create('Users', { username: 'elena', age: 26 });
    assert.ok(newRecord, 'Debe crear registro sin requerir index en el payload');
    assert.strictEqual(newRecord.username, 'elena');

    // La regla unique: true debe seguir activa tras el reinicio
    const dup = db2.create('Users', { username: 'elena', age: 99 });
    assert.strictEqual(dup, null, 'Debe mantener la restricción unique tras el reinicio');
  });

  cleanup();
});
