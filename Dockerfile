FROM python:3.13-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --frozen

RUN mkdir -p /app/.dagster

COPY dagster_project/ dagster_project/
COPY dbt_project/ dbt_project/
COPY data/ data/

EXPOSE 3000

CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
