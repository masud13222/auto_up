# syntax=docker/dockerfile:1.7

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DJANGO_SETTINGS_MODULE=config.settings

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    aria2 ffmpeg wget gnupg ca-certificates \
    postgresql-client \
    libx11-xcb1 libdbus-glib-1-2 \
    && wget -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt \
    && pip install gunicorn

COPY . .

RUN python manage.py collectstatic --noinput

RUN mkdir -p /app/downloads /app/media

EXPOSE 5000

CMD sh -c "\
    python manage.py migrate --noinput && \
    gunicorn config.wsgi:application \
        --bind 0.0.0.0:5000 \
        --workers 2 \
        --timeout 300 \
        --graceful-timeout 30 \
        --keep-alive 5 \
        --env GUNICORN_WORKER_PROCESS=1"
