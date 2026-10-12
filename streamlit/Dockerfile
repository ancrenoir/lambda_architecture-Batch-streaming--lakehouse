# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim-bookworm AS build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

COPY streamlit/requirements.txt ./requirements.txt
RUN mkdir -p /wheels && \
    pip wheel --wheel-dir=/wheels -r requirements.txt


FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

ARG STREAMLIT_UID=1000
ARG STREAMLIT_GID=1000
ARG STREAMLIT_USER=streamlit
ARG STREAMLIT_GROUP=streamlit
ARG PORT=8501

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app \
    PORT=${PORT} \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /wheels /wheels
COPY streamlit/requirements.txt ./requirements.txt
RUN pip install --no-index --find-links=/wheels -r requirements.txt && \
    rm -rf /wheels

RUN groupadd --gid ${STREAMLIT_GID} ${STREAMLIT_GROUP} && \
    useradd --uid ${STREAMLIT_UID} --gid ${STREAMLIT_GID} \
        --no-create-home --shell /usr/sbin/nologin ${STREAMLIT_USER} && \
    chown -R ${STREAMLIT_USER}:${STREAMLIT_GROUP} /app

COPY --chown=${STREAMLIT_USER}:${STREAMLIT_GROUP} streamlit ./streamlit
COPY --chown=${STREAMLIT_USER}:${STREAMLIT_GROUP} scripts ./scripts
COPY --chown=${STREAMLIT_USER}:${STREAMLIT_GROUP} schemas ./schemas

USER ${STREAMLIT_USER}

EXPOSE ${PORT}

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=5 \
    CMD curl -fsS "http://localhost:${PORT}/_stcore/health" || exit 1

CMD ["sh", "-c", "streamlit run /app/streamlit/Home.py --server.address=0.0.0.0 --server.port=$PORT"]
