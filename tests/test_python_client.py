# -*- coding: utf-8 -*-
import asyncio
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "clients", "Python")))
from database_client import JsonFluxClient, Types, connect, disconnect, execute


async def main():
    port = os.getenv("JSONFLUX_PORT", "3434")
    print(f"Iniciando pruebas del cliente Python para JsonFlux en puerto {port}...")
    url = f"http://127.0.0.1:{port}"

    # 1. Probar cliente orientado a objetos con Context Manager
    async with JsonFluxClient(url) as db:
        print("1. Conexión OO exitosa")
        try:
            await db.drop_model("Sensor")
        except Exception:
            pass
        await db.add_model("Sensor", {
            "name": {"type": Types["STRING"], "unique": True},
            "reading": {"type": Types["NUMBER"]},
            "status": {"type": Types["STRING"], "default": "active"}
        })
        print("2. add_model Sensor exitoso")

        s1 = await db.create("Sensor", {"name": "Temp_Sensor_1", "reading": 23.5})
        print("3. create exitoso:", s1)
        assert s1["name"] == "Temp_Sensor_1"
        assert s1["reading"] == 23.5

        # 4. Probar concurrencia masiva (asyncio.gather) sin colisiones de futuro
        print("4. Probando 30 peticiones concurrentes con asyncio.gather...")
        tasks = [
            db.create("Sensor", {"name": f"Sensor_Batch_{i}", "reading": i * 1.5})
            for i in range(1, 31)
        ]
        results = await asyncio.gather(*tasks)
        assert len(results) == 30
        print(f"✓ 30 operaciones concurrentes resueltas sin colisión (primer ID: {results[0]['index']}, último ID: {results[-1]['index']})")

        # 5. Probar find y find_by_id
        sensors = await db.find("Sensor", where={"reading": {"$gt": 10}})
        assert len(sensors) >= 20
        print(f"5. find con operador $gt exitoso ({len(sensors)} encontrados)")

        # 6. Probar update
        up = await db.update("Sensor", s1["index"], {"reading": 24.0})
        assert up["reading"] == 24.0
        print("6. update exitoso:", up)

        # 7. Probar destroy
        ok = await db.destroy("Sensor", s1["index"])
        assert ok is True
        print("7. destroy exitoso")

    # 8. Probar compatibilidad retroactiva con funciones globales
    print("8. Probando compatibilidad con connect() / execute() legados...")
    await connect(url)
    res = await execute("find", name="Sensor", limit=5)
    assert isinstance(res, list)
    print(f"✓ execute('find') legado funcionó: {len(res)} registros devueltos")
    await disconnect()
    print("¡Todas las pruebas del cliente Python pasaron con éxito al 100%!")


if __name__ == "__main__":
    asyncio.run(main())
