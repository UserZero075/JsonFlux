# -*- coding: utf-8 -*-
"""
JsonFlux Python Client - Cliente oficial asíncrono y de alta concurrencia para JsonFlux.

Mejoras:
- Soporte de múltiples peticiones concurrentes (asyncio.gather) con correlación reqId sin colisiones.
- Suscripción en tiempo real a mutaciones de modelos (subscribe/unsubscribe vía 'flux:change').
- Soporte para autenticación por token, reconexión automática y gestor de contexto asíncrono.
- Métodos de alto nivel tipo ORM (create, find, update, destroy, etc.).
- 100% retrocompatible con las funciones de módulo connect(), disconnect(), execute().
"""

import asyncio
import json
import uuid
from typing import Any, Callable, Dict, List, Optional, Union
import socketio
from loguru import logger


Types = {
    "NUMBER": "Number",
    "BOOLEAN": "Boolean",
    "STRING": "String",
    "OBJECT": "Object",
    "ARRAY": "Array",
    "ANY": "Any",
}


class JsonFluxClient:
    """Cliente asíncrono para JsonFlux con soporte de concurrencia y eventos en tiempo real."""

    def __init__(
        self,
        url: str = "http://localhost:3030",
        token: Optional[str] = None,
        timeout: float = 10.0,
    ):
        self.url = url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.sio = socketio.AsyncClient(
            reconnection=True,
            reconnection_attempts=15,
            reconnection_delay=1,
        )
        self.connected = False
        self._pending_futures: Dict[str, asyncio.Future] = {}
        self._subscribers: Dict[str, List[Callable[[dict], Any]]] = {}
        self._setup_event_handlers()

    def _setup_event_handlers(self):
        @self.sio.on("connect")
        async def on_connect():
            self.connected = True
            logger.info(f"[JsonFlux] Conectado exitosamente a {self.url}")
            # Re-suscribirse a modelos si hubo reconexión
            for model_name in self._subscribers.keys():
                await self.sio.emit("subscribe", model_name)

        @self.sio.on("disconnect")
        async def on_disconnect():
            self.connected = False
            logger.warning("[JsonFlux] Desconectado del servidor")

        @self.sio.on("flux:change")
        async def on_flux_change(event_data):
            if not isinstance(event_data, dict):
                return
            model = event_data.get("model")
            if model and model in self._subscribers:
                for callback in self._subscribers[model]:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            asyncio.create_task(callback(event_data))
                        else:
                            callback(event_data)
                    except Exception as e:
                        logger.error(f"[JsonFlux] Error en callback de suscriptor para '{model}': {e}")

        # Escuchar respuestas generales (compatibilidad con reqId en el payload)
        @self.sio.on("response")
        async def on_legacy_response(data):
            try:
                parsed = json.loads(data) if isinstance(data, str) else data
                if isinstance(parsed, dict) and "reqId" in parsed:
                    req_id = parsed.get("reqId")
                    if req_id in self._pending_futures:
                        fut = self._pending_futures.pop(req_id)
                        if not fut.done():
                            fut.set_result(parsed)
            except Exception:
                pass

    async def connect(self):
        """Conecta al servidor JsonFlux."""
        if self.connected:
            return self
        auth_payload = {"token": self.token} if self.token else {}
        logger.info(f"[JsonFlux] Conectando a {self.url}...")
        await self.sio.connect(self.url, auth=auth_payload)
        self.connected = True
        return self

    async def disconnect(self):
        """Cierra la conexión con el servidor."""
        if self.connected:
            await self.sio.disconnect()
            self.connected = False
            logger.info("[JsonFlux] Conexión cerrada.")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def execute(self, operation: str, **kwargs) -> Any:
        """
        Ejecuta una operación RPC contra JsonFlux con correlación reqId libre de colisiones.
        """
        if not self.connected:
            raise ConnectionError("No conectado al servidor JsonFlux")

        req_id = f"py_{uuid.uuid4().hex[:12]}"
        payload = dict(kwargs)
        payload["reqId"] = req_id

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending_futures[req_id] = future

        # Escuchador dinámico correlacionado con el reqId
        event_name = f"response:{req_id}"

        @self.sio.on(event_name)
        def on_correlated(data):
            self.sio.handlers.get("/", {}).pop(event_name, None)
            if req_id in self._pending_futures:
                self._pending_futures.pop(req_id, None)
            parsed = json.loads(data) if isinstance(data, str) else data
            if not future.done():
                future.set_result(parsed)

        # Emitir petición
        await self.sio.emit(operation, json.dumps(payload))

        try:
            result = await asyncio.wait_for(future, timeout=self.timeout)
            if isinstance(result, dict) and "error" in result:
                raise RuntimeError(result["error"])
            return result
        except asyncio.TimeoutError:
            self._pending_futures.pop(req_id, None)
            self.sio.handlers.get("/", {}).pop(event_name, None)
            raise TimeoutError(f"Timeout esperando respuesta para '{operation}'")

    # ---------------------------------------------------------------------------
    # Suscripciones en Tiempo Real
    # ---------------------------------------------------------------------------

    async def subscribe(self, model_name: str, callback: Callable[[dict], Any]):
        """Suscribe una función a los eventos de mutación en tiempo real de un modelo."""
        if model_name not in self._subscribers:
            self._subscribers[model_name] = []
            if self.connected:
                await self.sio.emit("subscribe", model_name)
        self._subscribers[model_name].append(callback)

    async def unsubscribe(self, model_name: str, callback: Optional[Callable] = None):
        """Cancela la suscripción a eventos de un modelo."""
        if model_name in self._subscribers:
            if callback:
                if callback in self._subscribers[model_name]:
                    self._subscribers[model_name].remove(callback)
            else:
                self._subscribers[model_name].clear()

            if not self._subscribers[model_name]:
                del self._subscribers[model_name]
                if self.connected:
                    await self.sio.emit("unsubscribe", model_name)

    # ---------------------------------------------------------------------------
    # Métodos CRUD de Alto Nivel
    # ---------------------------------------------------------------------------

    async def add_model(self, name: str, schema: dict) -> dict:
        return await self.execute("addModel", name=name, data=schema)

    async def get_model(self, name: str) -> dict:
        return await self.execute("getModel", name=name)

    async def get_models(self) -> list:
        return await self.execute("getModels")

    async def create(self, model: str, data: dict) -> dict:
        return await self.execute("create", name=model, data=data)

    async def find(
        self,
        model: str,
        where: Optional[dict] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        sort: Optional[dict] = None,
        **kwargs,
    ) -> Union[list, dict, None]:
        params = {"name": model, **kwargs}
        if where:
            params["where"] = where
        if limit is not None:
            params["limit"] = limit
        if skip is not None:
            params["skip"] = skip
        if sort is not None:
            params["sort"] = sort
        return await self.execute("find", **params)

    async def find_one(self, model: str, where: dict) -> Any:
        return await self.find(model, where=where, limit=1)

    async def find_by_id(self, model: str, record_id: Union[str, int]) -> Any:
        return await self.execute("findById", name=model, id=str(record_id))

    async def count(self, model: str, where: Optional[dict] = None) -> int:
        res = await self.execute("count", name=model, where=where)
        return res.get("count", 0) if isinstance(res, dict) else 0

    async def update(self, model: str, target: Union[dict, str, int], data: dict) -> Any:
        params: Dict[str, Any] = {"name": model, "data": data}
        if isinstance(target, dict):
            if "index" in target:
                params["last_search"] = target
            else:
                params["search"] = target
        else:
            params["id"] = str(target)
        return await self.execute("update", **params)

    async def destroy(self, model: str, target: Union[dict, str, int]) -> bool:
        params: Dict[str, Any] = {"name": model}
        if isinstance(target, dict):
            if "index" in target:
                params["last_search"] = target
            else:
                params["search"] = target
        else:
            params["id"] = str(target)
        res = await self.execute("destroy", **params)
        return isinstance(res, dict) and "error" not in res

    async def drop_model(self, model: str) -> dict:
        return await self.execute("dropModel", name=model)

    async def stats(self) -> dict:
        return await self.execute("stats")


# ---------------------------------------------------------------------------
# API Global de Compatibilidad Retroactiva
# ---------------------------------------------------------------------------

_default_client: Optional[JsonFluxClient] = None


async def connect(url: str = "http://127.0.0.1:3030", token: Optional[str] = None):
    global _default_client
    if _default_client is None:
        _default_client = JsonFluxClient(url=url, token=token)
    await _default_client.connect()
    return _default_client


async def disconnect():
    global _default_client
    if _default_client is not None:
        await _default_client.disconnect()
        _default_client = None


async def execute(operation: str, **kwargs):
    global _default_client
    if _default_client is None or not _default_client.connected:
        raise ConnectionError("No conectado al servidor. Llama a connect() primero.")
    return await _default_client.execute(operation, **kwargs)
