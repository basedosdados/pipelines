    # Build arguments
    ARG PYTHON_VERSION=3.9-slim

    # Python version: 3.9
    FROM python:${PYTHON_VERSION}

    # Setting environment with prefect version
    ARG PREFECT_VERSION=0.15.9
    ENV PREFECT_VERSION $PREFECT_VERSION

    # Install gcc
    RUN apt-get update && \
        apt-get install -y gcc python3-dev freetds-dev libssl-dev libcrypto++-dev

    # Setup virtual environment and prefect
    ENV VIRTUAL_ENV=/opt/venv
    RUN python3 -m venv $VIRTUAL_ENV
    ENV PATH="$VIRTUAL_ENV/bin:$PATH"
    RUN python3 -m pip install --no-cache-dir -U "pip>=21.2.4" "prefect==$PREFECT_VERSION"

    # Add CLI tools
    RUN apt-get update && \
        apt-get install --no-install-recommends -y curl wget ftp p7zip-full traceroute && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists/*

    # Install R
    RUN apt-get update && apt-get install -y r-base

    # Install requirements
    WORKDIR /app
    COPY . .
    RUN python3 -m pip install --prefer-binary --no-cache-dir -U . && \
        mkdir -p /opt/prefect/app/bases && \
        mkdir -p /root/.basedosdados/templates && \
        mkdir -p /root/.basedosdados/credentials/
