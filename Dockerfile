FROM prefecthq/prefect:3-python3.12

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.11.21 /uv /usr/local/bin/uv

# Install system dependencies (Chrome, SQL Server client, OCR, etc.)
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
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies directly into the system Python
COPY pyproject.toml uv.lock README.md ./
RUN uv export --locked --no-dev --no-hashes --no-emit-project -o /tmp/requirements.txt && \
    uv pip install --system --no-cache -r /tmp/requirements.txt

# dbt deps — copia só os arquivos de configuração do dbt, não o código dos flows
COPY packages.yml dbt_project.yml profiles.yml README.md ./
RUN dbt deps

# dbt Fusion engine (Rust) — coexiste com o dbt-core instalado via pip.
# O instalador coloca os binários em /root/.local/bin. Como /usr/local/bin
# (pip dbt-core) vem antes no PATH, `dbt` resolve para o core e `dbtf` para o
# Fusion. Durante a fase de migração, a escolha do engine em runtime é feita
# pela env DBT_ENGINE (ver pipelines/utils/execute_dbt_model/engine.py).
# Defina DBT_FUSION_VERSION para fixar a versão; vazio instala a mais recente.
ARG DBT_FUSION_VERSION=""
ENV PATH="/usr/local/bin:/root/.local/bin:${PATH}"
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh \
      | sh -s -- $( [ -n "$DBT_FUSION_VERSION" ] && echo "--version $DBT_FUSION_VERSION" || echo "--update" ) && \
    dbtf --version && \
    dbtf deps

# Diretórios necessários para basedosdados e prefect
RUN mkdir -p /opt/prefect/app/bases && \
    mkdir -p /root/.basedosdados/templates && \
    mkdir -p /root/.basedosdados/credentials/

# Entrypoint: decodifica credenciais antes de iniciar o flow
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["prefect", "worker", "start"]
