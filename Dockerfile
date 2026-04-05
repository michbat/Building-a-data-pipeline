# Image Python 3.13 slim (légère)
FROM python:3.13.10-slim

# Copier le binaire uv depuis l'image officielle (multi-stage build)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Définir le répertoire de travail
WORKDIR /code    

# Ajouter l'environnement virtuel au PATH pour utiliser les packages installés
ENV PATH="/code/.venv/bin:$PATH"

# Copier d'abord les fichiers de dépendances (optimisation du cache Docker)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Installer les dépendances depuis le fichier de verrouillage (builds reproductibles)
RUN uv sync --locked

# Copier le code de l'application
COPY ingest_data.py .

# Définir le point d'entrée
# ENTRYPOINT ["uv", "run", "python", "pipeline.py"]
ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]
