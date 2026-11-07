# JsonFlux v2.0 ⚡

**Motor de base de datos JSON en tiempo real, ligero, seguro y de alto rendimiento**, con sincronización reactiva bidireccional vía **Socket.IO** y SDKs oficiales para **Node.js** y **Python**.

Diseñado para funcionar en tu propia infraestructura (VPS, servidor dedicado o local), permitiendo que tus aplicaciones, bots de Telegram, dashboards web e IoT almacenen y sincronicen datos con latencias de submilisegundo, sin límites de terceros, sin intermediarios en la nube y con privacidad total.

---

## 🚀 Novedades y Mejoras Clave (v2.0)

1. **Persistencia Atómica Crash-Safe:**
   - Escritura atómica a disco (`tmp + atomic rename`) y copia de respaldo continua (`.bak`).
   - Guardado síncrono garantizado en eventos de salida (`SIGINT`, `SIGTERM`, `beforeExit`) con 0% de pérdida de datos.
   - Detección de modificaciones (`_isDirty`): evita escrituras redundantes cuando no hay cambios.

2. **Indexación en Memoria O(1):**
   - Eliminación de cuellos de botella de serialización (`JSON.parse(JSON.stringify)`) en consultas repetitivas.
   - Búsquedas instantáneas por ID y campos únicos en tiempo $O(1)$.
   - Supervivencia completa de esquemas, reglas `unique` y contadores tras reinicio de servidor.

3. **Arquitectura Real-Time Flux (Pub/Sub):**
   - Transmisión en tiempo real de mutaciones (`create`, `update`, `destroy`, `drop`) a través de canales de suscripción por modelo.
   - Los clientes pueden escuchar cambios en vivo mediante `client.subscribe('Modelo', callback)`.

4. **Correlación de Peticiones Concurrente (Zero Collisions):**
   - Manejo de `reqId` único y callbacks nativos Socket.IO ACK.
   - Miles de operaciones en paralelo (`Promise.all` en Node.js o `asyncio.gather` en Python) sin colisiones de promesas ni respuestas cruzadas.

5. **Operadores de Consulta Avanzados:**
   - Operadores nativos: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$contains` y `$regex`.
   - Paginación integrada con `limit`, `skip` y ordenamiento `sort`.

6. **Seguridad y Control de Acceso:**
   - Autenticación opcional por Token/API Key (`--token <secreto>` o variable `JSONFLUX_TOKEN`).
   - Protección contra Prototype Pollution (`__proto__`, `constructor`, `prototype`).
   - Validación robusta de tipos (`String`, `Number`, `Boolean`, `Object`, `Array`, `Any`).

---

## 📊 Benchmark de Rendimiento y Estrés (Test de Impacto)

Pruebas de estrés ejecutadas bajo carga concurrente real con 4 clientes WebSocket paralelos (`npm run test:stress`):

| Métrica | Resultado Obtenido |
| :--- | :--- |
| **Operaciones Totales** | **10.000 operaciones continuas** |
| **Tiempo de Ejecución** | **2,58 segundos** |
| **Rendimiento Promedio** | **~3.874 operaciones / segundo** |
| **Escrituras Concurrentes** | ~4.353 ops/seg |
| **Lecturas con Filtros Complejos** | ~2.394 ops/seg |
| **Actualizaciones Concurrentes** | ~7.246 ops/seg |
| **Eliminaciones Concurrentes** | ~8.929 ops/seg |
| **Integridad de Datos** | **100% (0 errores, 0 colisiones, 0 pérdidas)** |
| **Fugas de Memoria** | **0 fugas** (heap estable y predecible) |

---

## 🛠️ Instalación y Uso

### 1. Iniciar el Servidor

```bash
# Clonar e instalar dependencias
npm install

# Iniciar servidor en puerto por defecto (4000)
npm start

# Iniciar con parámetros personalizados
node index.js --port 3030 --dbpath ./data/DB.json --autosave 15000 --token mi_token_secreto
```

#### Opciones de Línea de Comandos:
- `-p, --port`: Puerto del servidor (por defecto: `4000` o variable `PORT`).
- `-a, --dbpath`: Ruta del archivo JSON de persistencia (por defecto: `./DB.json`).
- `-k, --autosave`: Intervalo de autoguardado en milisegundos (por defecto: `15000`).
- `-t, --token`: Token secreto de autenticación opcional.
- `-l, --logs`: Habilitar o silenciar logs (por defecto: `true`).

---

### 2. Uso con el Cliente Node.js

```javascript
const { JsonFluxClient, Types } = require('jsonflux/database-client');

async function main() {
  const db = new JsonFluxClient('http://localhost:4000', {
    token: 'mi_token_secreto' // Opcional si el servidor tiene token
  });

  await db.connect();

  // 1. Definir Modelo con restricciones
  await db.addModel('Users', {
    username: { type: Types.STRING, allowNull: false, unique: true },
    age: { type: Types.NUMBER, allowNull: false },
    role: { type: Types.STRING, default: 'user' },
    tags: { type: Types.ARRAY, default: ['developer'] }
  });

  // 2. Suscribirse a eventos en tiempo real
  const unsubscribe = db.subscribe('Users', (event) => {
    console.log(`[REAL-TIME EVENT] Acción: ${event.action}`, event.data);
  });

  // 3. Crear documento
  const user = await db.create('Users', {
    username: 'marlon',
    age: 26,
    role: 'admin'
  });
  console.log('Usuario creado:', user);

  // 4. Búsqueda con operadores avanzados ($gte, $in)
  const results = await db.find('Users', {
    age: { $gte: 18 },
    role: { $in: ['admin', 'moderator'] }
  }, { limit: 10 });
  console.log('Resultados:', results);

  // 5. Actualizar registro
  await db.update('Users', user.index, { age: 27 });

  // 6. Eliminar registro
  await db.destroy('Users', user.index);

  // Desconectar al finalizar
  await db.disconnect();
}

main().catch(console.error);
```

---

### 3. Uso con el Cliente Python

```python
import asyncio
from database_client import JsonFluxClient, Types

async def main():
    # Conexión mediante gestor de contexto asíncrono
    async with JsonFluxClient("http://127.0.0.1:4000", token="mi_token_secreto") as db:
        
        # 1. Registrar modelo
        await db.add_model("Telemetry", {
            "sensor": {"type": Types["STRING"], "unique": True},
            "temperature": {"type": Types["NUMBER"]},
            "status": {"type": Types["STRING"], "default": "nominal"}
        })

        # 2. Suscripción reactiva en tiempo real
        async def on_telemetry(event):
            print(f"[LIVE PY EVENT] {event['action']}: {event.get('data')}")

        await db.subscribe("Telemetry", on_telemetry)

        # 3. Inserciones concurrentes masivas con asyncio.gather
        tasks = [
            db.create("Telemetry", {"sensor": f"SENSOR_{i}", "temperature": 20.0 + i})
            for i in range(1, 11)
        ]
        created_items = await asyncio.gather(*tasks)
        print(f"Creados {len(created_items)} registros en paralelo sin colisiones")

        # 4. Consultas filtradas
        hot_sensors = await db.find("Telemetry", where={"temperature": {"$gt": 25.0}})
        print(f"Sensores con temperatura > 25°C: {len(hot_sensors)}")

        # 5. Conteo y actualización
        count = await db.count("Telemetry")
        print(f"Total sensores registrados: {count}")

asyncio.run(main())
```

---

## 🧪 Pruebas Automatizadas

El proyecto cuenta con suites de pruebas unitarias, de integración en red y de estrés bajo alta concurrencia:

```bash
# Ejecutar todas las pruebas unitarias y de integración
npm test

# Ejecutar el test de estrés de 10.000 operaciones simultáneas
npm run test:stress
```

---

## 📄 Licencia y Créditos

- **Autor:** Marlon Sarduy Machado ([UserZero075](https://github.com/UserZero075)) · DevFast Project.
- **Licencia:** Apache 2.0. Desarrollado para garantizar máxima independencia, privacidad y velocidad en arquitecturas modernas autohospedadas.
