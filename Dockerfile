<<<<<<< HEAD
# Build arguments
ARG PYTHON_VERSION=3.10-slim

# Python version: 3.10
FROM python:${PYTHON_VERSION}

# Configure environment
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install gcc, Google Chrome, CLI tools, git, R and others libs Firefox
RUN apt-get update && \
    apt-get install --no-install-recommends -y wget gnupg && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list && \
    apt-get update && \
    apt-get install --no-install-recommends -y \
    build-essential \
    curl \
    freetds-dev \
    ftp \
    gcc \
    google-chrome-stable \
    libcrypto++-dev \
    libssl-dev \
    p7zip-full \
    python3-dev \
    traceroute \
    wget \
    tesseract-ocr \
    python3-opencv \
    git \
    bzip2 \
    libxtst6 \
    libgtk-3-0 \
    libx11-xcb-dev \
    libdbus-glib-1-2 \
    libxt6 \
    libpci-dev \
    && \
    apt-get install -y r-base && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setup virtual environment and prefect
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN pip install --no-cache-dir --upgrade "pip>=21.2.4" "poetry==1.8.5"
WORKDIR /app
COPY . .
RUN poetry install && \
dbt deps && \
mkdir -p /opt/prefect/app/bases && \
mkdir -p /root/.basedosdados/templates && \
mkdir -p /root/.basedosdados/credentials/
=======
# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Ensure installed tools can be executed out of the box
ENV UV_TOOL_BIN_DIR=/usr/local/bin

# Install gcc, Google Chrome, CLI tools, git, R and others libs Firefox
RUN apt-get update && \
    apt-get install --no-install-recommends -y curl gnupg && \
    curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] https://dl.google.com/linux/chrome/deb/ stable main" | tee /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install --no-install-recommends -y \
    build-essential \
    freetds-dev \
    ftp \
    gcc \
    google-chrome-stable \
    libcrypto++-dev \
    libssl-dev \
    p7zip-full \
    python3-dev \
    traceroute \
    wget \
    tesseract-ocr \
    python3-opencv \
    git \
    bzip2 \
    libxtst6 \
    libgtk-3-0 \
    libx11-xcb-dev \
    libdbus-glib-1-2 \
    libxt6 \
    libpci-dev \
    && \
    apt-get install -y r-base && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

RUN uv run dbt deps && \
mkdir -p /opt/prefect/app/bases && \
mkdir -p /root/.basedosdados/templates && \
mkdir -p /root/.basedosdados/credentials/
>>>>>>> main
