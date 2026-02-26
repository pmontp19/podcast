#!/bin/bash
# Setup script for VPS - Configure podcast backend with auto-git-push

set -e

echo "🚀 Setup Podcast Backend per VPS"

# Variables
REPO_URL="https://github.com/pmontp19/podcast.git"
REPO_PATH="/home/podcast"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"

# Verificar si es el primer setup
if [ ! -d "$REPO_PATH/.git" ]; then
    echo "📦 Clonant repositori..."
    git clone "$REPO_URL" "$REPO_PATH" || git clone "https://oauth2:${GITHUB_TOKEN}@github.com/pmontp19/podcast.git" "$REPO_PATH"
    cd "$REPO_PATH"
else
    echo "✅ Repositori ja existeix"
    cd "$REPO_PATH"
    git pull origin main
fi

# Configurar git globally si GITHUB_TOKEN existeix
if [ -n "$GITHUB_TOKEN" ]; then
    echo "🔐 Configurant GITHUB_TOKEN..."
    git config --global credential.helper store
    echo "https://oauth2:${GITHUB_TOKEN}@github.com" > ~/.git-credentials
    chmod 600 ~/.git-credentials
fi

# Configurar git user
git config --global user.name "Podcast Bot"
git config --global user.email "bot@podcast.local"

echo "✨ Setup completat!"
echo ""
echo "Asseguresa que tinguis aquestes variables d'entorn configurades:"
echo "  GITHUB_TOKEN=tu_token_aqui"
echo "  GROQ_API_KEY=..."
echo "  ANTHROPIC_API_KEY=..."
echo "  RSS_URL=..."
echo ""
echo "Per executar el backend:"
echo "  cd $REPO_PATH/backend"
echo "  python processar_podcast.py tot"
