'use strict';

const test = require('node:test');
const assert = require('node:assert');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const { startServer } = require('../index.js');

const TEST_PORT = 3434;
const TEST_DB = path.join(__dirname, 'test_py_suite_db.json');
const PYTHON_BIN = '/tmp/jsonflux_py_venv/bin/python';

const cleanup = () => {
  for (const f of [TEST_DB, `${TEST_DB}.bak`]) {
    if (fs.existsSync(f)) {
      try { fs.unlinkSync(f); } catch (e) {}
    }
  }
};

test('JsonFlux Python Client Integration', async () => {
  if (!fs.existsSync(PYTHON_BIN)) {
    console.log('Skipping python client test: venv not available');
    return;
  }

  cleanup();

  // Iniciar servidor JsonFlux en puerto 3434 con base de datos aislada
  const server = await startServer(TEST_PORT, TEST_DB);

  const testScript = path.join(__dirname, 'test_python_client.py');

  const pyExitCode = await new Promise((resolve) => {
    const proc = spawn(PYTHON_BIN, [testScript], {
      env: {
        ...process.env,
        JSONFLUX_PORT: String(TEST_PORT),
      },
    });

    proc.stdout.on('data', (d) => process.stdout.write(`[PY] ${d}`));
    proc.stderr.on('data', (d) => process.stderr.write(`[PY ERR] ${d}`));

    proc.on('close', (code) => resolve(code));
  });

  await new Promise((r) => server.close(() => r()));
  cleanup();

  assert.strictEqual(pyExitCode, 0, 'El script del cliente Python debe salir con código 0');
});
