# Setup Podcast Backend a VPS

## 1. Preparació inicial

```bash
# Clonar el repositori (o usar el setup script)
git clone https://github.com/pmontp19/podcast.git /home/podcast
cd /home/podcast/backend

# O usar el script automatitzat:
chmod +x setup-vps.sh
./setup-vps.sh
```

## 2. Configurar GITHUB_TOKEN

```bash
# Crear token a GitHub:
# 1. Anar a Settings → Developer settings → Personal access tokens → Tokens (classic)
# 2. Generar nou token amb permisos: repo, write:repo_hook
# 3. Copiar el token

# Configurar a la VPS:
export GITHUB_TOKEN="ghp_xxxxxxxxxxxxx"

# O afegir-ho a .env (no commitear!)
echo "GITHUB_TOKEN=ghp_xxxxxxxxxxxxx" >> /home/podcast/.env
```

## 3. Executar el backend

```bash
# Versió única (processa RSS i fa push)
python processar_podcast.py tot

# Versió com a cron job (cada X hores)
# Afegir a crontab:
0 */6 * * * cd /home/podcast && python backend/processar_podcast.py tot >> /var/log/podcast.log 2>&1
```

## 4. Verificar que funciona

Després d'executar `processar_podcast.py tot`:
- ✅ JSON es genera a `frontend/src/data/podcast.json`
- ✅ Git fa commit automàticament
- ✅ Push al repositori (desencadena CF Pages build)

## Troubleshooting

### Error: "git not available"
```bash
sudo apt-get install git
```

### Error: "GITHUB_TOKEN no configurat"
```bash
# Verificar que existeix:
echo $GITHUB_TOKEN

# O afegir a /etc/environment:
export GITHUB_TOKEN="ghp_xxxxxxxxxxxxx"
```

### Error: "Permission denied" al fer push
```bash
# Assegurar que el token té permisos de repo
# i que no ha expirat
```

## Cloudflare Pages Integration

Quan es fa push al repositori:
1. GitHub detecta canvis
2. Cloudflare Pages automàticament detecta els canvis
3. Compila el frontend Astro
4. Deploy automàtic ✨

No necessites configurar res més, és totalment automàtic!
