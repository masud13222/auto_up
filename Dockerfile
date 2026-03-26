# ---- Base Image ----
FROM python:3.13-slim

# Environment
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DJANGO_SETTINGS_MODULE=config.settings

# Step 1: aria2 + ffmpeg (JPEG screenshots) + Chrome prerequisites; Pillow compresses frames in-app
RUN apt-get update && apt-get install -y --no-install-recommends \
    aria2 ffmpeg wget gnupg ca-certificates \
    postgresql-client \
    libx11-xcb1 libdbus-glib-1-2 \
    && rm -rf /var/lib/apt/lists/*

# Step 2: Google Chrome (WITHOUT --no-install-recommends so all runtime deps install)
RUN wget -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir pydoll-python gunicorn


# Copy project
COPY . .

# Collect static files
RUN python manage.py collectstatic --noinput 2>/dev/null || true

# Create directories
RUN mkdir -p /app/downloads /app/media

# Expose port
EXPOSE 5000

# Start: run migrations, then launch gunicorn
CMD sh -c "\
    python manage.py migrate --noinput && \
    gunicorn config.wsgi:application \
        --bind 0.0.0.0:5000 \
        --workers 2 \
        --timeout 300 \
        --graceful-timeout 30 \
        --keep-alive 5 \
        --env GUNICORN_WORKER_PROCESS=1"
