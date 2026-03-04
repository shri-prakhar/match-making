FROM python:3.13-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false

WORKDIR /build

COPY pyproject.toml poetry.lock ./
RUN poetry install --only main --no-root --no-interaction

FROM python:3.13-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 git && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local /usr/local

WORKDIR /opt/dagster/app

COPY talent_matching/ talent_matching/
COPY migrations/ migrations/
COPY alembic.ini .
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

EXPOSE 4266
