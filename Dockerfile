# Build arguments
ARG PYTHON_VERSION=3.9-slim

# Python version: 3.9
FROM python:${PYTHON_VERSION}

# Configure environment
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Setting environment with prefect version
ARG PREFECT_VERSION=0.15.9
ENV PREFECT_VERSION $PREFECT_VERSION

# Install gcc, Google Chrome, CLI tools and R
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
        && \
    apt-get install -y r-base && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setup virtual environment and prefect
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python3 -m pip install --no-cache-dir -U "pip>=21.2.4" "prefect==$PREFECT_VERSION"

# Install requirements
WORKDIR /app
COPY . .
RUN python3 -m pip install --prefer-binary --no-cache-dir -U . && \
    mkdir -p /opt/prefect/app/bases && \
    mkdir -p /root/.basedosdados/templates && \
    mkdir -p /root/.basedosdados/credentials/
