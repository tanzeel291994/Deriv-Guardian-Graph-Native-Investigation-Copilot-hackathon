# ──────────────────────────────────────────────────────────────────────────────
# Deriv Guardian — Single-container deploy (Frontend + API)
# ──────────────────────────────────────────────────────────────────────────────

# Stage 1: Build the React frontend
FROM node:20-slim AS frontend-build
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Python API + serve built frontend
FROM python:3.12-slim
WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml ./
# Generate a requirements.txt-style install from pyproject.toml
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    "pandas>=2.2" \
    "numpy>=2.1" \
    "openai>=1.0.0" \
    "python-dotenv>=1.0.0" \
    "fastapi>=0.115" \
    "uvicorn>=0.34"

# Copy application code
COPY pipeline/ ./pipeline/

# Copy pre-processed data (small: ~3.4MB total)
COPY data/transformed/ ./data/transformed/
COPY data/kumo_export/ ./data/kumo_export/
COPY data/predictions/ ./data/predictions/

# Copy built frontend from stage 1
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

# Railway sets PORT env var; default to 8000
ENV PORT=8000
EXPOSE 8000

# Run the API server
CMD ["sh", "-c", "uvicorn pipeline.api:app --host 0.0.0.0 --port ${PORT}"]

