"""
Sistema automàtic de processament de podcasts.
Llegeix RSS → descarrega → comprimeix → transcriu (Groq) → extreu entitats (Claude) → SQLite → JSON.
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import subprocess
import tempfile
from datetime import datetime
from typing import Literal

import anthropic
import feedparser
import requests
from groq import Groq
from pydantic import BaseModel
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
)

# ─── Configuració ────────────────────────────────────────────────────────────

RSS_URL = os.environ.get("RSS_URL", "")
DB_PATH = os.environ.get("DB_PATH", "dades/podcast.db")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "whisper-large-v3")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
MAX_EPISODIS = int(os.environ.get("MAX_EPISODIS", "10"))
SCORE_MINIM = float(os.environ.get("SCORE_MINIM", "0.6"))

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG = "https://image.tmdb.org/t/p/w342"
OL_BASE = "https://openlibrary.org"
OL_COVER = "https://covers.openlibrary.org/b/id"
RAWG_API_KEY = os.environ.get("RAWG_API_KEY", "")
RAWG_BASE = "https://api.rawg.io/api"

log = logging.getLogger("podcast")

# ─── Pydantic models ────────────────────────────────────────────────────────


CATEGORIES_VALIDES = {"llibre", "serie", "pelicula", "videojoc", "podcast"}


class RecomanacioRaw(BaseModel):
    obra: str
    categoria: str
    marca_temps: str
    confianca: float
    context: str
    justificacio: str


class RecomanacionsResponseRaw(BaseModel):
    recomanacions: list[RecomanacioRaw]


class Enrichment(BaseModel):
    imatge_url: str | None = None
    any_publicacio: str | None = None
    autor: str | None = None
    descripcio: str | None = None
    puntuacio: float | None = None
    api_id: str | None = None


# ─── Base de dades ───────────────────────────────────────────────────────────


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS podcast (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            titol TEXT,
            autor TEXT,
            descripcio TEXT,
            imatge_url TEXT
        );
        CREATE TABLE IF NOT EXISTS episodis (
            id TEXT PRIMARY KEY,
            titol TEXT NOT NULL,
            data_pub TEXT,
            numero TEXT,
            temporada TEXT,
            imatge_url TEXT,
            durada TEXT,
            resum TEXT,
            audio_url TEXT,
            transcripcio TEXT,
            extraccio_feta INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS mencions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            episodi_id TEXT NOT NULL,
            obra TEXT NOT NULL,
            categoria TEXT NOT NULL CHECK(categoria IN ('llibre', 'serie', 'pelicula', 'videojoc', 'podcast')),
            marca_temps TEXT,
            context TEXT,
            justificacio TEXT,
            imatge_url TEXT,
            any_publicacio TEXT,
            autor TEXT,
            descripcio TEXT,
            puntuacio REAL,
            api_id TEXT,
            enrichment_feta INTEGER DEFAULT 0,
            FOREIGN KEY (episodi_id) REFERENCES episodis(id)
        );
    """)
    conn.commit()


def episodi_existeix(conn: sqlite3.Connection, ep_id: str) -> bool:
    row = conn.execute("SELECT 1 FROM episodis WHERE id = ?", (ep_id,)).fetchone()
    return row is not None


def guardar_podcast(conn: sqlite3.Connection, feed) -> None:
    info = feed.feed
    conn.execute(
        "INSERT OR REPLACE INTO podcast (id, titol, autor, descripcio, imatge_url) "
        "VALUES (1, ?, ?, ?, ?)",
        (
            info.get("title"),
            info.get("itunes_author") or info.get("author"),
            info.get("subtitle") or info.get("summary"),
            (info.get("itunes_image") or {}).get("href")
            or (info.get("image") or {}).get("href"),
        ),
    )
    conn.commit()


def guardar_episodi(
    conn: sqlite3.Connection,
    ep_id: str,
    titol: str,
    data_pub: str,
    srt: str,
    *,
    numero: str | None = None,
    temporada: str | None = None,
    imatge_url: str | None = None,
    durada: str | None = None,
    resum: str | None = None,
    audio_url: str | None = None,
) -> None:
    try:
        conn.execute(
            "INSERT INTO episodis "
            "(id, titol, data_pub, numero, temporada, imatge_url, durada, resum, audio_url, transcripcio, extraccio_feta) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
            (ep_id, titol, data_pub, numero, temporada, imatge_url, durada, resum, audio_url, srt),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def guardar_mencions(
    conn: sqlite3.Connection, ep_id: str, mencions: list[dict]
) -> None:
    try:
        for m in mencions:
            conn.execute(
                "INSERT INTO mencions (episodi_id, obra, categoria, marca_temps, context, justificacio) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (ep_id, m["obra"], m["categoria"], m["marca_temps"], m.get("context"), m.get("justificacio")),
            )
        conn.execute(
            "UPDATE episodis SET extraccio_feta = 1 WHERE id = ?", (ep_id,)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def _mencio_a_dict(row: tuple) -> dict:
    obra, categoria, marca_temps, context, justificacio, imatge_url, any_pub, autor, descripcio, puntuacio, api_id = row
    d: dict = {"obra": obra, "categoria": categoria, "marca_temps": marca_temps}
    if context:
        d["context"] = context
    if justificacio:
        d["justificacio"] = justificacio
    if imatge_url:
        d["imatge_url"] = imatge_url
    if any_pub:
        d["any"] = any_pub
    if autor:
        d["autor"] = autor
    if descripcio:
        d["descripcio"] = descripcio
    if puntuacio is not None:
        d["puntuacio"] = puntuacio
    if api_id:
        d["api_id"] = api_id
    return d


def exportar_json(conn: sqlite3.Connection) -> None:
    # Podcast-level info
    pod_row = conn.execute(
        "SELECT titol, autor, descripcio, imatge_url FROM podcast WHERE id = 1"
    ).fetchone()
    podcast_info = None
    if pod_row:
        podcast_info = {
            k: v for k, v in
            zip(("titol", "autor", "descripcio", "imatge_url"), pod_row)
            if v is not None
        }

    episodis = conn.execute(
        "SELECT id, titol, data_pub, numero, temporada, imatge_url, durada, resum, audio_url "
        "FROM episodis ORDER BY data_pub DESC"
    ).fetchall()
    resultat = []
    for ep_id, titol, data_pub, numero, temporada, img, durada, resum, audio_url in episodis:
        mencions = conn.execute(
            "SELECT obra, categoria, marca_temps, context, justificacio, imatge_url, "
            "any_publicacio, autor, descripcio, puntuacio, api_id "
            "FROM mencions WHERE episodi_id = ? AND api_id IS NOT NULL",
            (ep_id,),
        ).fetchall()
        ep: dict = {"id": ep_id, "titol": titol, "data_pub": data_pub}
        if numero:
            ep["numero"] = numero
        if temporada:
            ep["temporada"] = temporada
        if img:
            ep["imatge_url"] = img
        if durada:
            ep["durada"] = durada
        if resum:
            ep["resum"] = resum
        if audio_url:
            ep["audio_url"] = audio_url
        ep["mencions"] = [_mencio_a_dict(m) for m in mencions]
        resultat.append(ep)

    output: dict = {}
    if podcast_info:
        output["podcast"] = podcast_info
    output["episodis"] = resultat

    json_path = os.path.join(os.path.dirname(DB_PATH) or ".", "podcast.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    log.info("JSON exportat: %s", json_path)


def publicar_json() -> None:
    dades_dir = os.path.dirname(DB_PATH) or "."
    try:
        github_token = os.environ.get("GITHUB_TOKEN")
        if github_token:
            subprocess.run(["git", "config", "--global", "user.name", "Podcast Bot"], check=True, capture_output=True)
            subprocess.run(["git", "config", "--global", "user.email", "bot@podcast.local"], check=True, capture_output=True)
            remote_url = subprocess.run(
                ["git", "remote", "get-url", "origin"], cwd=dades_dir, capture_output=True, text=True
            ).stdout.strip()
            if remote_url.startswith("https://"):
                # Inject token into existing HTTPS remote URL
                authed_url = remote_url.replace("https://", f"https://oauth2:{github_token}@")
                subprocess.run(["git", "remote", "set-url", "origin", authed_url], cwd=dades_dir, check=True, capture_output=True)
        subprocess.run(["git", "add", "."], cwd=dades_dir, check=True, capture_output=True)
        diff = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=dades_dir,
            capture_output=True,
        )
        if diff.returncode == 0:
            log.info("Cap canvi per publicar")
            return
        subprocess.run(
            ["git", "commit", "-m", f"Actualització {datetime.now().isoformat(timespec='minutes')}"],
            cwd=dades_dir,
            check=True,
            capture_output=True,
        )
        subprocess.run(["git", "push"], cwd=dades_dir, check=True, capture_output=True)
        log.info("JSON publicat a git")
    except subprocess.CalledProcessError as e:
        log.error("Error publicant: %s", e.stderr.decode() if e.stderr else e)
    except FileNotFoundError:
        log.warning("git no disponible, salt publicació")


# ─── Enriquiment ─────────────────────────────────────────────────────────────


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential_jitter(initial=1, max=30),
    stop=stop_after_attempt(3),
)
def cercar_tmdb_pelicula(titol: str) -> Enrichment:
    resp = requests.get(
        f"{TMDB_BASE}/search/movie",
        params={"query": titol, "language": "ca"},
        headers={"Authorization": f"Bearer {TMDB_API_KEY}"},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        return Enrichment()
    r = results[0]
    poster = f"{TMDB_IMG}{r['poster_path']}" if r.get("poster_path") else None
    return Enrichment(
        imatge_url=poster,
        any_publicacio=(r.get("release_date") or "")[:4] or None,
        descripcio=r.get("overview") or None,
        puntuacio=r.get("vote_average"),
        api_id=f"tmdb:movie:{r['id']}",
    )


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential_jitter(initial=1, max=30),
    stop=stop_after_attempt(3),
)
def cercar_tmdb_serie(titol: str) -> Enrichment:
    resp = requests.get(
        f"{TMDB_BASE}/search/tv",
        params={"query": titol, "language": "ca"},
        headers={"Authorization": f"Bearer {TMDB_API_KEY}"},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        return Enrichment()
    r = results[0]
    poster = f"{TMDB_IMG}{r['poster_path']}" if r.get("poster_path") else None
    return Enrichment(
        imatge_url=poster,
        any_publicacio=(r.get("first_air_date") or "")[:4] or None,
        descripcio=r.get("overview") or None,
        puntuacio=r.get("vote_average"),
        api_id=f"tmdb:tv:{r['id']}",
    )


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential_jitter(initial=1, max=30),
    stop=stop_after_attempt(3),
)
def cercar_openlibrary(titol: str) -> Enrichment:
    resp = requests.get(
        f"{OL_BASE}/search.json",
        params={"title": titol, "limit": 1},
        timeout=10,
    )
    resp.raise_for_status()
    docs = resp.json().get("docs", [])
    if not docs:
        return Enrichment()
    d = docs[0]
    cover_id = d.get("cover_i")
    cover_url = f"{OL_COVER}/{cover_id}-M.jpg" if cover_id else None
    key = d.get("key", "")
    return Enrichment(
        imatge_url=cover_url,
        any_publicacio=str(d["first_publish_year"]) if d.get("first_publish_year") else None,
        autor=d["author_name"][0] if d.get("author_name") else None,
        api_id=f"ol:{key}" if key else None,
    )


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential_jitter(initial=1, max=30),
    stop=stop_after_attempt(3),
)
def cercar_rawg_joc(titol: str) -> Enrichment:
    resp = requests.get(
        f"{RAWG_BASE}/games",
        params={"key": RAWG_API_KEY, "search": titol, "page_size": 1},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        return Enrichment()
    r = results[0]
    return Enrichment(
        imatge_url=r.get("background_image"),
        any_publicacio=(r.get("released") or "")[:4] or None,
        puntuacio=r.get("rating"),
        api_id=f"rawg:game:{r['id']}",
    )


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential_jitter(initial=1, max=30),
    stop=stop_after_attempt(3),
)
def cercar_podcast_itunes(titol: str) -> Enrichment:
    resp = requests.get(
        "https://itunes.apple.com/search",
        params={"term": titol, "entity": "podcast", "limit": 1},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        return Enrichment()
    r = results[0]
    return Enrichment(
        imatge_url=r.get("artworkUrl600"),
        autor=r.get("artistName"),
        api_id=f"itunes:podcast:{r.get('trackId')}",
    )


def enriquir_mencio(obra: str, categoria: str) -> Enrichment:
    if categoria == "pelicula":
        if not TMDB_API_KEY:
            return Enrichment()
        return cercar_tmdb_pelicula(obra)
    elif categoria == "serie":
        if not TMDB_API_KEY:
            return Enrichment()
        return cercar_tmdb_serie(obra)
    elif categoria == "videojoc":
        if not RAWG_API_KEY:
            return Enrichment()
        return cercar_rawg_joc(obra)
    elif categoria == "podcast":
        return cercar_podcast_itunes(obra)
    else:
        return cercar_openlibrary(obra)


def buscar_enrichment_existent(
    conn: sqlite3.Connection, obra: str, categoria: str
) -> Enrichment | None:
    row = conn.execute(
        "SELECT imatge_url, any_publicacio, autor, descripcio, puntuacio, api_id "
        "FROM mencions WHERE obra = ? AND categoria = ? AND enrichment_feta = 1 "
        "AND api_id IS NOT NULL LIMIT 1",
        (obra, categoria),
    ).fetchone()
    if not row:
        return None
    return Enrichment(
        imatge_url=row[0], any_publicacio=row[1], autor=row[2],
        descripcio=row[3], puntuacio=row[4], api_id=row[5],
    )


def guardar_enrichment(
    conn: sqlite3.Connection, mencio_id: int, enrichment: Enrichment
) -> None:
    conn.execute(
        "UPDATE mencions SET imatge_url=?, any_publicacio=?, autor=?, "
        "descripcio=?, puntuacio=?, api_id=?, enrichment_feta=1 WHERE id=?",
        (
            enrichment.imatge_url, enrichment.any_publicacio, enrichment.autor,
            enrichment.descripcio, enrichment.puntuacio, enrichment.api_id,
            mencio_id,
        ),
    )
    conn.commit()


# ─── Àudio ───────────────────────────────────────────────────────────────────

MAX_GROQ_SIZE = 25 * 1024 * 1024  # 25 MB


@retry(
    retry=retry_if_exception_type((IOError, requests.exceptions.RequestException)),
    wait=wait_exponential_jitter(initial=1, max=60),
    stop=stop_after_attempt(3),
)
def descarregar_audio(url: str, dest: str) -> None:
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)


def comprimir_audio(src: str, dest: str) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        src,
        "-ac",
        "1",
        "-ar",
        "16000",
        "-b:a",
        "24k",
        dest,
    ]
    subprocess.run(cmd, check=True, capture_output=True)


# ─── IA ──────────────────────────────────────────────────────────────────────


def transcriure_audio(path: str, client_groq: Groq) -> str:
    size = os.path.getsize(path)
    if size > MAX_GROQ_SIZE:
        raise ValueError(
            f"Fitxer massa gran per Groq: {size / 1024 / 1024:.1f} MB (màx 25 MB)"
        )
    with open(path, "rb") as f:
        transcription = client_groq.audio.transcriptions.create(
            file=(os.path.basename(path), f),
            model=GROQ_MODEL,
            response_format="verbose_json",
            timestamp_granularities=["segment"],
        )
    return json.dumps(
        [{"start": s["start"], "end": s["end"], "text": s["text"].strip()} for s in transcription.segments],
        ensure_ascii=False,
    )


@retry(
    retry=retry_if_exception_type(anthropic.APIStatusError),
    wait=wait_exponential_jitter(initial=1, max=60),
    stop=stop_after_attempt(3),
)
def extreure_entitats(transcripcio_json: str, client_anthropic: anthropic.Anthropic) -> list[dict]:
    system_prompt = (
        "Ets un detector de RECOMANACIONS culturals en podcasts en català. "
        "Rebràs segments JSON amb camps \"start\" (segons), \"end\" (segons) i \"text\".\n\n"

        "OBJECTIU: Identifica obres culturals que els parlants RECOMANEN o valoren positivament. "
        "NO busques simples mencions — busques recomanacions, és a dir: entusiasme, "
        "valoració positiva, suggeriment explícit o implícit que l'audiència hauria de consumir l'obra.\n\n"

        "QUÈ ÉS UNA RECOMANACIÓ (exemples):\n"
        "- \"Us recomano molt La píndola vermella\" → recomanació directa\n"
        "- \"Culinary Class Wars és superxulo, ens encanta\" → valoració entusiasta\n"
        "- \"Warehouse 13 és molt interessant, doneu-li una oportunitat\" → suggeriment\n"
        "- \"He llegit aquest llibre i m'ha fascinat\" → valoració personal positiva\n"
        "- \"Estem veient aquesta sèrie i és genial\" → entusiasme\n\n"

        "QUÈ NO ÉS UNA RECOMANACIÓ (exemples):\n"
        "- \"Hem viscut de referents de Star Wars\" → referència cultural, no recomanació\n"
        "- \"Aquestes classicorres que tinc: Blade Runner, 2001...\" → llistat/inventari/col·lecció\n"
        "- \"Tinc guardades totes les pelis de ciència-ficció\" → col·lecció personal, no recomanació\n"
        "- \"Com a la pel·lícula Inception\" → comparació/al·lusió\n"
        "- \"Recordo quan vaig veure Matrix\" → record/anècdota sense valoració\n"
        "- Qualsevol menció del propi podcast (\"Mossega la Poma\")\n\n"

        "TRANSCRIPCIÓ WHISPER — ERRORS FREQÜENTS:\n"
        "La transcripció pot contenir errors de Whisper. Si un títol sona similar a una obra real "
        "però està mal transcrit, corregeix-lo al camp \"obra\" amb el títol oficial correcte. "
        "Exemples d'errors típics: \"Missez Poagot\" → no és cap obra real, descarta; "
        "\"Guer of Stars\" → podria ser \"War of Stars\" però si no encaixa amb cap obra real, descarta.\n\n"

        "REGLES IMPORTANTS:\n"
        "- \"obra\" ha de ser un TÍTOL CONCRET d'una obra específica, MAI un nom d'autor sol "
        "(NO: \"Asimov\", \"Tolkien books\", \"Matt Dinniman works\"; "
        "SÍ: \"Fundació\", \"El Senyor dels Anells\", \"Dungeon Crawler Carl\").\n"
        "- Si el parlant recomana un autor sense esmentar cap títol concret, NO l'incloguis.\n"
        "- NO dupliquis: cada obra ha d'aparèixer UNA SOLA vegada. Si es menciona en diversos "
        "segments, usa el primer segment on apareix la recomanació.\n"
        "- NO afegeixis prefixos d'autor al títol (NO: \"Craig Alanson - Convergence\"; SÍ: \"Convergence\").\n\n"

        "CAMPS REQUERITS per cada recomanació:\n"
        "- \"obra\": títol oficial exacte de l'obra (corregit si cal, sense prefix d'autor)\n"
        "- \"categoria\": un de \"llibre\", \"serie\", \"pelicula\", \"videojoc\", \"podcast\"\n"
        "- \"marca_temps\": HH:MM:SS derivat del camp \"start\" (ex: 3661.5 → 01:01:01)\n"
        "- \"confianca\": float 0.0–1.0 — confiança que és una recomanació REAL d'una obra REAL "
        "(1.0 = recomanació explícita d'obra coneguda; 0.7 = valoració positiva clara; "
        "0.4 = menció positiva ambigua; 0.2 = simple menció sense recomanació)\n"
        "- \"context\": cita breu literal del segment on apareix\n"
        "- \"justificacio\": 1 frase explicant PER QUÈ consideres que és una recomanació "
        "(ex: \"El parlant expressa entusiasme dient que és superxulo\")\n\n"

        "EXCLOU (NO són obres culturals): el propi podcast, persones/personalitats, "
        "canals de YouTube, vídeos de YouTube, aplicacions, software (Lightroom, Aperture, etc.), "
        "models d'IA (Llama, Gemini, etc.), hardware, plataformes, xarxes socials, webs, "
        "empreses, marques comercials i cançons.\n"
        "La \"categoria\" NOMÉS pot ser llibre/serie/pelicula/videojoc/podcast. "
        "Si una obra no encaixa clarament en cap d'aquestes, NO l'incloguis.\n\n"

        "Si no trobes cap recomanació, retorna una llista buida. "
        "És millor retornar una llista buida que incloure mencions que no són recomanacions."
    )

    response = client_anthropic.messages.parse(
        model=ANTHROPIC_MODEL,
        max_tokens=4096,
        system=system_prompt,
        messages=[{"role": "user", "content": transcripcio_json}],
        output_format=RecomanacionsResponseRaw,
    )

    result = response.parsed_output
    valid = [m for m in result.recomanacions if m.categoria in CATEGORIES_VALIDES]
    if len(valid) < len(result.recomanacions):
        dropped = [m for m in result.recomanacions if m.categoria not in CATEGORIES_VALIDES]
        for m in dropped:
            log.warning("  Descartada (categoria '%s'): %s", m.categoria, m.obra)

    # Post-filter: context keywords + confidence threshold
    _CONTEXT_REJECTS = re.compile(r"\b(cançó|cançons|canción|song|store)\b", re.IGNORECASE)
    filtered = []
    for m in valid:
        if _CONTEXT_REJECTS.search(m.context):
            log.warning("  Descartada (context): %s — %s", m.obra, m.context)
        elif m.confianca < SCORE_MINIM:
            log.warning("  Descartada (confiança %.1f): %s — %s", m.confianca, m.obra, m.context)
        else:
            filtered.append(m)
            log.info("  Recomanació [%.1f]: %s [%s] — %s", m.confianca, m.obra, m.categoria, m.justificacio)

    return [
        {"obra": m.obra, "categoria": m.categoria, "marca_temps": m.marca_temps,
         "context": m.context, "justificacio": m.justificacio}
        for m in filtered
    ]


# ─── Utilitats ───────────────────────────────────────────────────────────────


def obtenir_url_audio(entry) -> str | None:
    for link in entry.get("links", []):
        if link.get("type", "").startswith("audio/"):
            return link["href"]
    for enc in entry.get("enclosures", []):
        if enc.get("type", "").startswith("audio/"):
            return enc["url"]
    return None


def formatar_data(entry) -> str:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6]).isoformat()
    return entry.get("published", "")


# ─── Etapes del pipeline ────────────────────────────────────────────────────


def etapa_transcriure(conn: sqlite3.Connection, client_groq: Groq) -> None:
    if not RSS_URL:
        log.error("RSS_URL no definida")
        raise SystemExit(1)

    log.info("Llegint feed: %s", RSS_URL)
    feed = feedparser.parse(RSS_URL)

    guardar_podcast(conn, feed)

    entries = feed.entries[:MAX_EPISODIS]
    log.info("Trobats %d episodis (màx %d)", len(entries), MAX_EPISODIS)

    for entry in entries:
        ep_id = entry.get("id", entry.get("link", ""))
        titol = entry.get("title", "Sense títol")

        if episodi_existeix(conn, ep_id):
            log.debug("SKIP (ja existeix): %s", titol)
            continue

        audio_url = obtenir_url_audio(entry)
        if not audio_url:
            log.warning("SKIP (sense àudio): %s", titol)
            continue

        log.info("Processant: %s", titol)

        tmp_orig = None
        tmp_comp = None
        try:
            tmp_orig = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False).name
            log.info("  Descarregant àudio...")
            descarregar_audio(audio_url, tmp_orig)

            tmp_comp = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False).name
            log.info("  Comprimint àudio...")
            comprimir_audio(tmp_orig, tmp_comp)

            log.info("  Transcrivint (Groq)...")
            srt_text = transcriure_audio(tmp_comp, client_groq)

            data_pub = formatar_data(entry)
            guardar_episodi(
                conn, ep_id, titol, data_pub, srt_text,
                numero=entry.get("itunes_episode"),
                temporada=entry.get("itunes_season"),
                imatge_url=(entry.get("itunes_image") or {}).get("href"),
                durada=entry.get("itunes_duration"),
                resum=entry.get("summary") or None,
                audio_url=audio_url,
            )
            log.info("  Guardat OK")

        except Exception as e:
            log.error("ERROR processant '%s': %s", titol, e)

        finally:
            for f in (tmp_orig, tmp_comp):
                if f and os.path.exists(f):
                    os.remove(f)


def etapa_extreure(conn: sqlite3.Connection, client_anthropic: anthropic.Anthropic) -> None:
    rows = conn.execute(
        "SELECT id, titol, transcripcio FROM episodis "
        "WHERE extraccio_feta = 0 AND transcripcio IS NOT NULL"
    ).fetchall()

    if not rows:
        log.info("Cap episodi pendent d'extracció")
        return

    log.info("Extraient entitats de %d episodis", len(rows))
    for ep_id, titol, srt_text in rows:
        try:
            log.info("  Extraient: %s", titol)
            mencions = extreure_entitats(srt_text, client_anthropic)
            guardar_mencions(conn, ep_id, mencions)
            log.info("  %d mencions trobades", len(mencions))
        except Exception as e:
            log.error("ERROR extraient '%s': %s", titol, e)


def etapa_enriquir(conn: sqlite3.Connection) -> None:
    if not TMDB_API_KEY:
        log.warning("TMDB_API_KEY no definida — sèries/pelis no s'enriquiran")
    if not RAWG_API_KEY:
        log.warning("RAWG_API_KEY no definida — videojocs no s'enriquiran")

    rows = conn.execute(
        "SELECT id, obra, categoria FROM mencions WHERE enrichment_feta = 0"
    ).fetchall()

    if not rows:
        log.info("Cap menció pendent d'enriquiment")
        return

    log.info("Enriquint %d mencions", len(rows))
    for mencio_id, obra, categoria in rows:
        try:
            existent = buscar_enrichment_existent(conn, obra, categoria)
            if existent:
                guardar_enrichment(conn, mencio_id, existent)
                log.debug("  Reutilitzat: %s", obra)
                continue

            enrichment = enriquir_mencio(obra, categoria)
            guardar_enrichment(conn, mencio_id, enrichment)
            if enrichment.api_id:
                log.info("  Enriquit: %s → %s", obra, enrichment.api_id)
            else:
                log.debug("  Sense resultats: %s", obra)
        except Exception as e:
            log.error("ERROR enriquint '%s': %s", obra, e)


# ─── CLI ─────────────────────────────────────────────────────────────────────


def etapa_re_extreure(conn: sqlite3.Connection, client_anthropic: anthropic.Anthropic) -> None:
    """Esborra totes les mencions, reseteja extraccio_feta, i re-executa extracció."""
    n_mencions = conn.execute("SELECT COUNT(*) FROM mencions").fetchone()[0]
    conn.execute("DELETE FROM mencions")
    conn.execute("UPDATE episodis SET extraccio_feta = 0 WHERE transcripcio IS NOT NULL")
    conn.commit()
    log.info("Esborrades %d mencions, resetejat extraccio_feta", n_mencions)
    etapa_extreure(conn, client_anthropic)


def main():
    parser = argparse.ArgumentParser(description="Pipeline de processament de podcasts")
    parser.add_argument(
        "etapa",
        choices=["transcriure", "extreure", "enriquir", "tot", "exportar", "re-extreure"],
        help="Etapa a executar",
    )
    args = parser.parse_args()

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    client_groq = Groq() if args.etapa in ("transcriure", "tot") else None
    client_anthropic = (
        anthropic.Anthropic() if args.etapa in ("extreure", "re-extreure", "tot") else None
    )

    try:
        if args.etapa == "transcriure":
            etapa_transcriure(conn, client_groq)
        elif args.etapa == "extreure":
            etapa_extreure(conn, client_anthropic)
        elif args.etapa == "re-extreure":
            etapa_re_extreure(conn, client_anthropic)
        elif args.etapa == "enriquir":
            etapa_enriquir(conn)
        elif args.etapa == "exportar":
            exportar_json(conn)
            publicar_json()
        elif args.etapa == "tot":
            etapa_transcriure(conn, client_groq)
            etapa_extreure(conn, client_anthropic)
            etapa_enriquir(conn)
            exportar_json(conn)
            publicar_json()
    finally:
        conn.close()

    log.info("Fet.")


if __name__ == "__main__":
    main()
