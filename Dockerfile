FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY labs ./labs
COPY tests ./tests
RUN pip install --no-cache-dir -e ".[dev]" \
    # warm the Ivy cache so the Delta jars are baked into the image
    && python -c "from labs.spark import get_spark; get_spark().stop()"

CMD ["spark-labs"]
