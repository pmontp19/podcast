# Podcast Pipeline

Pipeline automàtic de processament de podcasts: RSS → descàrrega → compressió → transcripció → extracció d'entitats culturals → SQLite → JSON.

## Pipeline

```
RSS Feed
  ↓
Descàrrega MP3
  ↓
Compressió (ffmpeg → mono 16kHz 24kbps)
  ↓
Transcripció (Groq Whisper → SRT)
  ↓
Extracció entitats (Claude Haiku → llibres, sèries, pel·lícules)
  ↓
SQLite + JSON → Git push
```

## Ús

```bash
# Totes les etapes
python processar_podcast.py tot

# Només transcriure nous episodis
python processar_podcast.py transcriure

# Només extreure entitats (episodis ja transcrits)
python processar_podcast.py extreure

# Només exportar JSON i publicar
python processar_podcast.py exportar
```

## Variables d'entorn

| Variable | Obligatòria | Default | Descripció |
|---|---|---|---|
| `GROQ_API_KEY` | Sí | — | Clau API de Groq |
| `ANTHROPIC_API_KEY` | Sí | — | Clau API d'Anthropic |
| `RSS_URL` | Sí* | — | URL del feed RSS (*per transcriure/tot) |
| `DB_PATH` | No | `dades/podcast.db` | Ruta a la base de dades |
| `GROQ_MODEL` | No | `whisper-large-v3` | Model de transcripció |
| `ANTHROPIC_MODEL` | No | `claude-haiku-4-5` | Model d'extracció |
| `MAX_EPISODIS` | No | `10` | Episodis màxims per execució |
| `LOG_LEVEL` | No | `INFO` | Nivell de logging |

## Docker

```bash
# Construir
docker compose build

# Executar pipeline complet
docker compose run --rm podcast tot

# Només extreure
docker compose run --rm podcast extreure
```

## Cron (VPS)

```cron
0 6 * * * cd /path/to/podcast && docker compose run --rm podcast tot >> /var/log/podcast.log 2>&1
```

## Publicació JSON

L'etapa `exportar` (i `tot`) fa `git add + commit + push` dins `dades/`. Cal que `dades/` sigui un repo git amb remote configurat i autenticació (token o SSH).

## Estructura de la BD

- `episodis`: id, titol, data_pub, transcripcio (SRT), extraccio_feta (0/1)
- `mencions`: episodi_id, obra, categoria (llibre/serie/pelicula), marca_temps
