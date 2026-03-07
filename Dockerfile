# ---- Base Image ----
FROM python:3.13-slim

# Environment
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DJANGO_SETTINGS_MODULE=config.settings

# Install system dependencies (aria2, ffmpeg)
RUN apt-get update && \
    apt-get install -y --no-install-recommends aria2 ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copy project
COPY . .

# Collect static files
RUN python manage.py collectstatic --noinput 2>/dev/null || true

# Create directories
RUN mkdir -p /app/downloads /app/media

# Expose port
EXPOSE 5000

# Start: run migrations, then launch gunicorn + qcluster
CMD sh -c "\
    python manage.py migrate --noinput && \
    python manage.py qcluster & \
    gunicorn config.wsgi:application --bind 0.0.0.0:8000 --workers 2 --timeout 300"
