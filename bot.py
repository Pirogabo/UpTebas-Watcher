import asyncio
import base64
import copy
import hmac
import html
import json
import os
import random
import re
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse

import aiohttp
import discord
from aiohttp import web
from discord import app_commands
from discord.ext import tasks

# ================= AJUSTES DE ARRANQUE (variables de entorno) =================
TOKEN = os.environ["DISCORD_TOKEN"]
WEBUI_PASSWORD = os.environ.get("WEBUI_PASSWORD", "")  # sin contraseña, la web queda desactivada
WEBUI_PORT = int(os.environ.get("WEBUI_PORT", "8088"))
CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", "/data/config.json"))
MEDIA_DIR = CONFIG_PATH.parent / "media"
EXTS_PERMITIDAS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}

# --- hayahora.futbol (mismos datos que usa su web, sacados del HAR) ---
HAYAHORA_URL = "https://hayahora.futbol/estado/data.json"
HAYAHORA_CACHE_SEG = 180        # no descarga el JSON más de una vez cada 3 min
HAYAHORA_MAX_EDAD_H = 3         # si sus datos llevan más de 3 h sin actualizarse, no me fío
ESTADO_BLOQUEADO = True
ISPS = ["Movistar", "DIGI", "Vodafone", "Orange", "Masmovil"]
# ==============================================================================

CONFIG_POR_DEFECTO = {
    "intervalo": 60,
    "fallos_para_alertar": 2,
    "isp": "",
    "servicios": {},
    "memes_caida": [
        {"msg": "⚠️ El servicio **{s}** ha dejado de responder.", "gif": ""},
    ],
    "memes_subida": [
        {"msg": "✅ El servicio **{s}** vuelve a estar operativo.", "gif": ""},
    ],
}


def cargar_config():
    if CONFIG_PATH.exists():
        try:
            datos = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            for clave, valor in CONFIG_POR_DEFECTO.items():
                datos.setdefault(clave, copy.deepcopy(valor))
            return datos
        except Exception as e:
            print(f"⚠️ No pude leer {CONFIG_PATH} ({e!r}); uso la configuración por defecto")
    return copy.deepcopy(CONFIG_POR_DEFECTO)


def guardar_config():
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CONFIG_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(CONFIG_PATH)


cfg = cargar_config()
estado = {}


def estado_de(nombre):
    return estado.setdefault(nombre, {
        "online": True, "fallos": 0, "ms": 0, "motivo": "",
        "desde": time.time(), "primer_fallo": None, "check": 0,
    })


def elegir_meme(clave, texto_defecto):
    lista = cfg.get(clave) or []
    if not lista:
        return {"msg": texto_defecto, "gif": ""}
    return random.choice(lista)


def fmt_duracion(seg):
    seg = int(seg)
    if seg < 60:
        return f"{seg} s"
    m, s = divmod(seg, 60)
    if m < 60:
        return f"{m} min"
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h} h {m} min"
    d, h = divmod(h, 24)
    return f"{d} d {h} h"


# ================= COMPROBACIÓN DE SERVICIOS =================

async def comprobar_servicio(session, datos):
    inicio = time.monotonic()
    try:
        async with session.get(
            datos["url"],
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            ms = int((time.monotonic() - inicio) * 1000)
            ok = resp.status < 400
            motivo = f"HTTP {resp.status}"
            if ok and datos.get("contiene"):
                ok = datos["contiene"] in await resp.text()
                if not ok:
                    motivo = "no aparece el texto esperado"
            return ok, ms, motivo
    except Exception as e:
        return False, 0, type(e).__name__


# ================= BLOQUEOS DE LALIGA (hayahora.futbol) =================

_dns_cache = {}
_hay = {"intento": 0.0, "tabla": None, "actualizado": None, "error": None}


def parse_ts(ts):
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%SZ").replace(tzinfo=timezone.utc)


async def resolver_ipv4(session, host):
    if re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", host):
        return [host]
    cacheado = _dns_cache.get(host)
    if cacheado and time.time() - cacheado[0] < 300:
        return cacheado[1]
    ips = []
    try:
        async with session.get(
            "https://dns.google/resolve",
            params={"name": host, "type": "A"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            js = await r.json(content_type=None)
        ips = [a["data"] for a in js.get("Answer", []) if a.get("type") == 1]
    except Exception:
        pass
    if not ips:
        try:
            infos = await asyncio.get_running_loop().getaddrinfo(
                host, None, family=socket.AF_INET, type=socket.SOCK_STREAM
            )
            ips = sorted({i[4][0] for i in infos})
        except Exception:
            ips = []
    if ips:
        _dns_cache[host] = (time.time(), ips)
    return ips


def parsear_hayahora(js):
    tabla = {}
    for entrada in js.get("data", []):
        cambios = sorted(entrada.get("stateChanges", []), key=lambda c: c["timestamp"])
        if not cambios:
            continue
        ultimo_bloqueo = None
        for c in cambios:
            if c["state"] == ESTADO_BLOQUEADO:
                ultimo_bloqueo = c["timestamp"]
        tabla.setdefault(entrada["ip"], {})[entrada.get("isp", "?")] = {
            "bloqueada": cambios[-1]["state"] == ESTADO_BLOQUEADO,
            "ultimo_bloqueo": ultimo_bloqueo,
        }
    return tabla


async def cargar_hayahora(session):
    ahora = time.time()
    margen = HAYAHORA_CACHE_SEG if _hay["tabla"] is not None else 60
    if ahora - _hay["intento"] < margen:
        return _hay if _hay["tabla"] is not None else None
    _hay["intento"] = ahora
    try:
        async with session.get(
            HAYAHORA_URL,
            headers={"User-Agent": "bot-uptime-personal"},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            r.raise_for_status()
            js = await r.json(content_type=None)
        _hay["tabla"] = parsear_hayahora(js)
        _hay["actualizado"] = parse_ts(js["lastUpdate"]) if js.get("lastUpdate") else None
        _hay["error"] = None
    except Exception as e:
        _hay["error"] = type(e).__name__
        print(f"⚠️ No pude leer hayahora.futbol: {_hay['error']}")
    return _hay if _hay["tabla"] is not None else None


async def _info_laliga(session, datos):
    modo = datos.get("laliga", "auto")
    if modo == "no":
        return None
    host = urlparse(datos["url"]).hostname
    if not host:
        return None
    ips = await resolver_ipv4(session, host)
    if not ips:
        return None
    hay = await cargar_hayahora(session)
    if hay is None:
        return None

    isp = cfg.get("isp", "").lower()
    filas = []
    for ip in ips:
        por_isp = hay["tabla"].get(ip, {})
        relevantes = {k: v for k, v in por_isp.items() if not isp or k.lower() == isp}
        filas.append({
            "ip": ip,
            "en_lista": bool(por_isp),
            "bloqueada": any(v["bloqueada"] for v in relevantes.values()),
            "isps": sorted(k for k, v in relevantes.items() if v["bloqueada"]),
            "ultimo_bloqueo": max(
                (v["ultimo_bloqueo"] for v in relevantes.values() if v["ultimo_bloqueo"]),
                default=None,
            ),
        })
    en_lista = any(f["en_lista"] for f in filas)
    if modo == "auto" and not en_lista:
        return None

    actualizado = hay["actualizado"]
    desactualizado = (
        actualizado is None
        or (datetime.now(timezone.utc) - actualizado).total_seconds() > HAYAHORA_MAX_EDAD_H * 3600
    )
    return {
        "filas": filas,
        "en_lista": en_lista,
        "desactualizado": desactualizado,
        "bloqueada_ahora": (not desactualizado) and any(f["bloqueada"] for f in filas),
    }


async def info_laliga(session, datos):
    try:
        return await _info_laliga(session, datos)
    except Exception as e:
        print(f"⚠️ Error comprobando bloqueos de {datos.get('url')}: {e!r}")
        return None


def resumen_laliga(info):
    if info is None:
        return None
    if info["desactualizado"]:
        return ("⚠️", "Datos de hayahora.futbol desactualizados, no me fío")
    bloqueadas = [f["ip"] for f in info["filas"] if f["bloqueada"]]
    if bloqueadas:
        return ("🚫", "Bloqueo de LaLiga activo ahora en: " + ", ".join(bloqueadas))
    if info["en_lista"]:
        return ("🟡", "Está en la lista de IPs que bloquea LaLiga, pero ahora mismo accesible")
    return ("⚪", "Ninguna de sus IPs está en la lista de hayahora.futbol")


# ================= EMBEDS =================

def crear_embed(titulo, meme, nombre, color):
    texto = meme.get("msg", "**{s}**").replace("{s}", nombre)
    embed = discord.Embed(title=titulo, description=texto, color=color)
    gif = (meme.get("gif") or "").strip()
    archivo = None
    if gif.startswith("local:"):
        ruta = MEDIA_DIR / Path(gif[6:]).name
        if ruta.is_file():
            archivo = discord.File(ruta, filename=ruta.name)
            embed.set_image(url=f"attachment://{ruta.name}")
    elif gif.startswith(("http://", "https://")):
        embed.set_image(url=gif)
    return embed, archivo


def campo_laliga(embed, info):
    r = resumen_laliga(info)
    if not r:
        return
    texto = f"{r[0]} {r[1]}"
    ultimos = [f["ultimo_bloqueo"] for f in info["filas"] if f["ultimo_bloqueo"]]
    if ultimos:
        ts = int(parse_ts(max(ultimos)).timestamp())
        texto += f"\nÚltimo bloqueo detectado: <t:{ts}:f>"
    embed.add_field(name="LaLiga", value=texto, inline=False)


def embed_servicio(nombre, ok, ms, motivo, info):
    embed = discord.Embed(
        title=f"{'🟢' if ok else '🔴'} {nombre}",
        description=f"**Online** ({ms} ms)" if ok else f"**Caído** ({motivo})",
        color=discord.Color.green() if ok else discord.Color.red(),
    )
    st = estado.get(nombre)
    if st and not st["online"]:
        embed.add_field(name="Avisado como caído", value=f"hace {fmt_duracion(time.time() - st['desde'])}", inline=False)
    campo_laliga(embed, info)
    return embed


# ================= BOT =================

class MiBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        if not CONFIG_PATH.exists():
            guardar_config()
        MEDIA_DIR.mkdir(parents=True, exist_ok=True)
        await self.tree.sync()
        await self.arrancar_web()
        self.vigilar_servicios.change_interval(seconds=cfg["intervalo"])
        self.vigilar_servicios.start()

    async def arrancar_web(self):
        if not WEBUI_PASSWORD:
            print("⚠️ WEBUI_PASSWORD no está definida: la web de administración está DESACTIVADA")
            return
        runner = web.AppRunner(crear_app())
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", WEBUI_PORT).start()
        print(f"🌐 Web de administración en el puerto {WEBUI_PORT}")

    async def enviar_aviso(self, nombre, datos, embed, archivo=None):
        canal = self.get_channel(datos["canal_id"])
        if canal is None:
            print(f"⚠️ No encuentro el canal de {nombre} (ID {datos['canal_id']}). ¿ID mal copiado o el bot no lo ve?")
            return
        if archivo is not None:
            await canal.send(embed=embed, file=archivo)
        else:
            await canal.send(embed=embed)

    async def revisar(self, session, nombre, datos):
        st = estado_de(nombre)
        ok, ms, motivo = await comprobar_servicio(session, datos)
        st["ms"], st["motivo"], st["check"] = ms, motivo, time.time()

        if ok:
            st["fallos"] = 0
            st["primer_fallo"] = None
            if not st["online"]:
                meme = elegir_meme("memes_subida", "✅ El servicio **{s}** vuelve a estar operativo.")
                embed, archivo = crear_embed(f"🎉 {nombre} REVIVIÓ", meme, nombre, discord.Color.green())
                embed.add_field(name="Estuvo caído", value=fmt_duracion(time.time() - st["desde"]))
                await self.enviar_aviso(nombre, datos, embed, archivo)
                st["online"] = True
                st["desde"] = time.time()
            return

        if st["fallos"] == 0:
            st["primer_fallo"] = time.time()
        st["fallos"] += 1
        print(f"⚠️ {nombre}: fallo {st['fallos']}/{cfg['fallos_para_alertar']} ({motivo})")

        if st["online"] and st["fallos"] >= cfg["fallos_para_alertar"]:
            info = await info_laliga(session, datos)
            archivo = None
            if info and info["bloqueada_ahora"]:
                embed = discord.Embed(
                    title=f"🚫 {nombre} inaccesible (bloqueo de LaLiga)",
                    description=f"**{nombre}** no responde, pero hay IPs de Cloudflare bloqueadas ahora mismo. No parece cosa de tu servidor.",
                    color=discord.Color.orange(),
                )
            else:
                meme = elegir_meme("memes_caida", "⚠️ El servicio **{s}** ha dejado de responder.")
                embed, archivo = crear_embed(f"🚨 {nombre} CAYÓ", meme, nombre, discord.Color.red())
            embed.add_field(name="Motivo", value=motivo, inline=False)
            campo_laliga(embed, info)
            await self.enviar_aviso(nombre, datos, embed, archivo)
            st["online"] = False
            st["desde"] = st["primer_fallo"] or time.time()

    async def revisar_seguro(self, session, nombre, datos):
        try:
            await self.revisar(session, nombre, datos)
        except Exception as e:
            print(f"⚠️ Error con {nombre}: {e!r}")

    @tasks.loop(seconds=60)
    async def vigilar_servicios(self):
        for nombre in list(estado):
            if nombre not in cfg["servicios"]:
                del estado[nombre]
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*(
                self.revisar_seguro(session, nombre, dict(datos))
                for nombre, datos in list(cfg["servicios"].items())
            ))

    @vigilar_servicios.before_loop
    async def antes_de_vigilar(self):
        await self.wait_until_ready()


bot = MiBot()


@bot.event
async def on_ready():
    print(f"🤖 Bot conectado como {bot.user}")


# ================= COMANDOS DE BARRA (/) =================

async def autocompletar_servicio(interaction: discord.Interaction, current: str):
    return [
        app_commands.Choice(name=n, value=n)
        for n in cfg["servicios"]
        if current.lower() in n.lower()
    ][:25]


@bot.tree.command(name="servicio", description="Mira si un servicio está up o down")
@app_commands.describe(nombre="Servicio a comprobar")
@app_commands.autocomplete(nombre=autocompletar_servicio)
async def comando_servicio(interaction: discord.Interaction, nombre: str):
    await interaction.response.defer()
    datos = cfg["servicios"].get(nombre)
    if datos is None:
        await interaction.followup.send(f"No conozco ningún servicio llamado **{nombre}**.")
        return
    async with aiohttp.ClientSession() as session:
        (ok, ms, motivo), info = await asyncio.gather(
            comprobar_servicio(session, datos), info_laliga(session, datos)
        )
    await interaction.followup.send(embed=embed_servicio(nombre, ok, ms, motivo, info))


@bot.tree.command(name="estado", description="Muestra el estado de todos los servicios configurados")
async def comando_estado(interaction: discord.Interaction):
    await interaction.response.defer()
    servicios = list(cfg["servicios"].items())[:25]
    if not servicios:
        await interaction.followup.send("No hay servicios configurados.")
        return
    async with aiohttp.ClientSession() as session:
        resultados = await asyncio.gather(*(comprobar_servicio(session, d) for _, d in servicios))
        infos = await asyncio.gather(*(info_laliga(session, d) for _, d in servicios))

    embed = discord.Embed(title="📡 Estado de los servicios", color=discord.Color.blurple())
    for (nombre, _), (ok, ms, motivo), info in zip(servicios, resultados, infos):
        texto = f"🟢 **Online** ({ms} ms)" if ok else f"🔴 **Caído** ({motivo})"
        r = resumen_laliga(info)
        if r and r[0] in ("🚫", "⚠️"):
            texto += f"\n{r[0]} {r[1]}"
        embed.add_field(name=nombre, value=texto, inline=False)
    await interaction.followup.send(embed=embed)


# ================= WEB DE ADMINISTRACIÓN =================

CSS = """
:root{color-scheme:dark}
*{box-sizing:border-box}
body{font-family:system-ui,sans-serif;background:#111318;color:#e6e6e6;margin:0;padding:24px;max-width:1240px;margin-inline:auto}
h1{font-size:1.45rem;margin-bottom:8px}
h2{font-size:1.05rem;margin-top:22px;margin-bottom:8px;color:#9aa4b2}
h3{font-size:.95rem;margin:10px 0 4px;color:#cbd5e1}
.layout{display:grid;grid-template-columns:1fr 440px;gap:24px;align-items:start}
@media(max-width:920px){.layout{grid-template-columns:1fr}}
.card{background:#1a1d24;border:1px solid #2a2f3a;border-radius:10px;padding:14px;margin:10px 0;display:grid;gap:10px}
.meme-item{background:#14171d;border:1px solid #252a34;border-radius:8px;padding:10px;display:grid;gap:8px}
.meme-head{display:flex;gap:10px;align-items:center}
.meme-thumb{width:48px;height:48px;object-fit:cover;border-radius:6px;border:1px solid #2a2f3a;flex-shrink:0}
label{display:grid;gap:4px;font-size:.83rem;color:#9aa4b2;width:100%}
input,select{background:#0e1015;color:#e6e6e6;border:1px solid #2a2f3a;border-radius:6px;padding:8px;font-size:.92rem;width:100%}
input[type="file"]{padding:5px;font-size:.82rem}
button{background:#3b6df0;color:#fff;border:0;border-radius:6px;padding:8px 13px;cursor:pointer;font-size:.9rem}
button.peligro{background:#9b2c2c}
button.secundario{background:#2b3240}
.botones{display:flex;gap:8px;flex-wrap:wrap}
.estado{font-size:.98rem}
small{color:#9aa4b2;line-height:1.35}
code{background:#0e1015;padding:2px 5px;border-radius:4px;color:#93c5fd}
.aviso{background:#2a2410;border:1px solid #6b5a1a;border-radius:8px;padding:10px;margin:12px 0}
"""


def _opciones(pares, actual):
    return "".join(
        f'<option value="{html.escape(v)}"{" selected" if v == actual else ""}>{html.escape(t)}</option>'
        for v, t in pares
    )


def _campos_servicio(nombre, datos):
    e = html.escape
    laliga = _opciones(
        [("auto", "Auto (detectar si es Cloudflare)"), ("si", "Sí, siempre"), ("no", "No")],
        datos.get("laliga", "auto"),
    )
    return f"""
      <input type="hidden" name="original" value="{e(nombre)}">
      <label>Nombre<input name="nombre" value="{e(nombre)}" required maxlength="50"></label>
      <label>URL que se comprueba<input name="url" value="{e(datos.get('url', ''))}" required placeholder="http://192.168.1.52:8096/health"></label>
      <label>ID del canal de Discord<input name="canal_id" value="{e(str(datos.get('canal_id', '')))}" required inputmode="numeric"></label>
      <label>Texto que debe aparecer en la respuesta (opcional)<input name="contiene" value="{e(datos.get('contiene', ''))}"></label>
      <label>Bloqueos de LaLiga<select name="laliga">{laliga}</select></label>"""


def _tarjeta(nombre, datos, resumen):
    e = html.escape
    st = estado.get(nombre)
    if st is None:
        badge = "⚪ sin datos todavía"
    elif st["online"]:
        badge = f"🟢 online ({st['ms']} ms)"
        if st["fallos"]:
            badge = f"🟠 {st['fallos']} fallo(s) seguido(s), aún sin avisar ({e(st['motivo'])})"
    else:
        badge = f"🔴 caído desde hace {fmt_duracion(time.time() - st['desde'])} ({e(st['motivo'])})"
    laliga = f"{resumen[0]} {resumen[1]}" if resumen else "—"
    return f"""
    <form method="post" action="/guardar" class="card">
      <div class="estado">{badge}<br><small>LaLiga: {e(laliga)}</small></div>
      {_campos_servicio(nombre, datos)}
      <div class="botones">
        <button>Guardar</button>
        <button class="peligro" formaction="/borrar" formnovalidate onclick="return confirm('¿Borrar este servicio?')">Borrar</button>
      </div>
    </form>"""


def _bloque_memes(clave, titulo):
    e = html.escape
    lista = cfg.get(clave) or []
    items = []
    for idx, m in enumerate(lista):
        msg = m.get("msg", "")
        gif = m.get("gif") or ""
        if gif.startswith("local:"):
            thumb = f'<img src="/media/{e(gif[6:])}" class="meme-thumb" alt="img">'
            url_val = ""
            nota_local = f"<small>📎 Archivo subido: <code>{e(gif[6:])}</code></small>"
        elif gif.startswith(("http://", "https://")):
            thumb = f'<img src="{e(gif)}" class="meme-thumb" alt="gif">'
            url_val = gif
            nota_local = ""
        else:
            thumb = ""
            url_val = ""
            nota_local = ""

        items.append(f"""
        <form method="post" action="/meme/guardar" enctype="multipart/form-data" class="meme-item">
          <input type="hidden" name="tipo" value="{e(clave)}">
          <input type="hidden" name="idx" value="{idx}">
          <div class="meme-head">
            {thumb}
            <label>Frase<input name="msg" value="{e(msg)}" required maxlength="300"></label>
          </div>
          {nota_local}
          <label>Subir nueva imagen/GIF desde tu dispositivo (opcional)
            <input type="file" name="archivo" accept=".png,.jpg,.jpeg,.gif,.webp">
          </label>
          <label>O pegar enlace directo http/https (opcional)
            <input name="gif" value="{e(url_val)}" placeholder="https://...">
          </label>
          <div class="botones">
            <button class="secundario">Actualizar</button>
            <button class="peligro" formaction="/meme/borrar" formnovalidate onclick="return confirm('¿Eliminar esta frase?')">Borrar</button>
          </div>
        </form>""")
    vacio = "<small>No hay frases personalizadas (se usará una frase neutra por defecto).</small>" if not items else ""
    return f"""
    <h3>{titulo}</h3>
    {vacio}
    {"".join(items)}
    <form method="post" action="/meme/guardar" enctype="multipart/form-data" class="meme-item">
      <input type="hidden" name="tipo" value="{e(clave)}">
      <input type="hidden" name="idx" value="">
      <label>Nueva frase (usa <code>{{s}}</code> donde quieras el nombre del servicio)
        <input name="msg" required maxlength="300" placeholder="Ej: 💀 {{s}} ha dejado de responder.">
      </label>
      <label>Subir imagen/GIF desde tu PC o móvil (opcional)
        <input type="file" name="archivo" accept=".png,.jpg,.jpeg,.gif,.webp">
      </label>
      <label>O pegar enlace directo (opcional)
        <input name="gif" placeholder="https://...">
      </label>
      <div class="botones"><button>Añadir frase</button></div>
    </form>"""


def render_pagina(mensaje, resumenes):
    e = html.escape
    aviso = f'<div class="aviso">{e(mensaje)}</div>' if mensaje else ""
    isp = _opciones([("", "Cualquiera")] + [(i, i) for i in ISPS], cfg.get("isp", ""))
    tarjetas = "".join(_tarjeta(n, d, resumenes.get(n)) for n, d in cfg["servicios"].items())
    if not tarjetas:
        tarjetas = '<div class="card"><small>No hay servicios configurados todavía. Añade el primero aquí abajo.</small></div>'
    nuevo = _campos_servicio("", {"laliga": "auto"})
    memes_caida_html = _bloque_memes("memes_caida", "🚨 Al caer un servicio")
    memes_subida_html = _bloque_memes("memes_subida", "🎉 Al recuperarse un servicio")

    return f"""<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vigilante de servicios</title><style>{CSS}</style></head><body>
<h1>📡 Vigilante de servicios</h1>{aviso}
<div class="layout">
  <main>
    <h2>Servicios monitorizados</h2>
    {tarjetas}
    <h2>Añadir servicio</h2>
    <form method="post" action="/guardar" class="card">{nuevo}<div class="botones"><button>Añadir servicio</button></div></form>
  </main>
  <aside>
    <h2>⚙️ Ajustes generales</h2>
    <form method="post" action="/ajustes" class="card">
      <label>Mirar cada (segundos, mínimo 15)<input name="intervalo" type="number" min="15" max="3600" value="{int(cfg['intervalo'])}"></label>
      <label>Fallos seguidos antes de avisar<input name="fallos" type="number" min="1" max="10" value="{int(cfg['fallos_para_alertar'])}"></label>
      <label>Tu operador (filtra los bloqueos de LaLiga)<select name="isp">{isp}</select></label>
      <small>Con 60 s y 2 fallos, una caída de menos de 1 minuto no genera aviso.</small>
      <div class="botones"><button>Guardar ajustes</button></div>
    </form>

    <h2>💬 Frases y GIFs de avisos</h2>
    <div class="card">
      <small>Puedes subir imágenes/GIFs directamente desde tu dispositivo (se guardan en <code>/data/media</code>) o pegar un enlace. Usa <code>{{s}}</code> para el nombre del servicio.</small>
      {memes_caida_html}
      {memes_subida_html}
    </div>
  </aside>
</div>
</body></html>"""


@web.middleware
async def auth_mw(request, handler):
    ok = False
    cabecera = request.headers.get("Authorization", "")
    if cabecera.startswith("Basic "):
        try:
            usuario_y_clave = base64.b64decode(cabecera[6:]).decode()
            clave = usuario_y_clave.partition(":")[2]
            ok = hmac.compare_digest(clave.encode(), WEBUI_PASSWORD.encode())
        except Exception:
            ok = False
    if not ok:
        await asyncio.sleep(1)
        return web.Response(status=401, text="Hace falta contraseña", headers={"WWW-Authenticate": 'Basic realm="bot"'})
    if request.method == "POST":
        origen = request.headers.get("Origin")
        if origen and urlparse(origen).netloc != request.host:
            return web.Response(status=403, text="Origen no permitido")
    return await handler(request)


def _volver(mensaje=""):
    raise web.HTTPSeeOther(location="/?msg=" + quote(mensaje) if mensaje else "/")


async def ui_inicio(request):
    async with aiohttp.ClientSession() as session:
        nombres = list(cfg["servicios"])
        infos = await asyncio.gather(*(info_laliga(session, cfg["servicios"][n]) for n in nombres))
    resumenes = {n: resumen_laliga(i) for n, i in zip(nombres, infos)}
    return web.Response(text=render_pagina(request.query.get("msg", ""), resumenes), content_type="text/html")


async def ui_media(request):
    nombre = Path(request.match_info["nombre"]).name
    ruta = MEDIA_DIR / nombre
    if not ruta.is_file():
        return web.Response(status=404)
    return web.FileResponse(ruta)


async def ui_guardar(request):
    f = await request.post()
    original = f.get("original", "").strip()
    nombre = f.get("nombre", "").strip()
    url = f.get("url", "").strip()
    canal = f.get("canal_id", "").strip()
    contiene = f.get("contiene", "").strip()
    laliga = f.get("laliga", "auto")

    if not nombre or len(nombre) > 50:
        _volver("El nombre tiene que tener entre 1 y 50 caracteres.")
    if nombre != original and nombre in cfg["servicios"]:
        _volver(f"Ya existe un servicio llamado {nombre}.")
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        _volver("La URL tiene que empezar por http:// o https://")
    if not canal.isdigit() or not 15 <= len(canal) <= 20:
        _volver("El ID del canal no parece válido (clic derecho en el canal → Copiar ID).")
    if laliga not in ("auto", "si", "no"):
        laliga = "auto"

    if original and original != nombre:
        cfg["servicios"].pop(original, None)
        estado.pop(original, None)
    cfg["servicios"][nombre] = {"url": url, "canal_id": int(canal), "contiene": contiene, "laliga": laliga}
    guardar_config()

    aviso = f"Guardado: {nombre}."
    if bot.get_channel(int(canal)) is None:
        aviso += " OJO: el bot no ve ese canal (¿ID mal copiado o sin permisos?)."
    _volver(aviso)


async def ui_borrar(request):
    f = await request.post()
    nombre = f.get("original", "").strip()
    if cfg["servicios"].pop(nombre, None) is not None:
        estado.pop(nombre, None)
        guardar_config()
        _volver(f"Borrado: {nombre}.")
    _volver("Ese servicio ya no existe.")


async def ui_ajustes(request):
    f = await request.post()
    try:
        intervalo = int(f.get("intervalo", ""))
        fallos = int(f.get("fallos", ""))
    except ValueError:
        _volver("Intervalo y fallos tienen que ser números.")
    if not 15 <= intervalo <= 3600 or not 1 <= fallos <= 10:
        _volver("Intervalo entre 15 y 3600 s, y fallos entre 1 y 10.")
    isp = f.get("isp", "")
    if isp not in ISPS:
        isp = ""
    cfg["intervalo"], cfg["fallos_para_alertar"], cfg["isp"] = intervalo, fallos, isp
    guardar_config()
    bot.vigilar_servicios.change_interval(seconds=intervalo)
    _volver("Ajustes guardados.")


async def ui_meme_guardar(request):
    f = await request.post()
    tipo = f.get("tipo", "")
    if tipo not in ("memes_caida", "memes_subida"):
        _volver("Tipo de mensaje no válido.")
    msg = f.get("msg", "").strip()
    gif_url = f.get("gif", "").strip()
    idx_raw = f.get("idx", "").strip()
    archivo = f.get("archivo")

    if not msg or len(msg) > 300:
        _volver("La frase tiene que tener entre 1 y 300 caracteres.")

    lista = cfg.setdefault(tipo, [])
    gif_final = ""
    if idx_raw != "":
        try:
            gif_final = lista[int(idx_raw)].get("gif", "")
        except (ValueError, IndexError):
            _volver("No se encontró la frase a editar.")

    if isinstance(archivo, web.FileField) and archivo.filename:
        ext = Path(archivo.filename).suffix.lower()
        if ext not in EXTS_PERMITIDAS:
            _volver("Formato no válido. Usa PNG, JPG, GIF o WEBP.")
        MEDIA_DIR.mkdir(parents=True, exist_ok=True)
        nombre_fichero = f"{int(time.time() * 1000)}_{random.randint(100, 999)}{ext}"
        (MEDIA_DIR / nombre_fichero).write_bytes(archivo.file.read())
        gif_final = f"local:{nombre_fichero}"
    elif gif_url:
        p = urlparse(gif_url)
        if p.scheme not in ("http", "https") or not p.hostname:
            _volver("La URL del GIF o imagen debe empezar por http:// o https://")
        gif_final = gif_url
    elif not gif_final.startswith("local:"):
        gif_final = ""

    item = {"msg": msg, "gif": gif_final}
    if idx_raw == "":
        lista.append(item)
    else:
        lista[int(idx_raw)] = item
    
    guardar_config()
    _volver("Frase guardada correctamente.")


async def ui_meme_borrar(request):
    f = await request.post()
    tipo = f.get("tipo", "")
    idx_raw = f.get("idx", "").strip()
    if tipo in ("memes_caida", "memes_subida"):
        try:
            eliminado = cfg.get(tipo, []).pop(int(idx_raw))
            gif = eliminado.get("gif", "")
            if gif.startswith("local:"):
                ruta = MEDIA_DIR / Path(gif[6:]).name
                if ruta.is_file():
                    ruta.unlink()
            guardar_config()
            _volver("Frase eliminada.")
        except (ValueError, IndexError):
            pass
    _volver("No se pudo borrar la frase.")


def crear_app():
    app = web.Application(middlewares=[auth_mw], client_max_size=10 * 1024 * 1024)
    app.add_routes([
        web.get("/", ui_inicio),
        web.get("/media/{nombre}", ui_media),
        web.post("/guardar", ui_guardar),
        web.post("/borrar", ui_borrar),
        web.post("/ajustes", ui_ajustes),
        web.post("/meme/guardar", ui_meme_guardar),
        web.post("/meme/borrar", ui_meme_borrar),
    ])
    return app


if __name__ == "__main__":
    bot.run(TOKEN)
