# syntax=docker/dockerfile:1
FROM python:3.14.7-slim@sha256:51dafde81dbdb6ebde285137a295cf18a47ca95234fe388a343719cb97305b3d AS base

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg libogg0 libvorbis0a libvorbisfile3 libopus0 \
    && rm -rf /var/lib/apt/lists/*

FROM base AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /source
COPY pyproject.toml README.md LICENSE ./
COPY zotify ./zotify
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

FROM base
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --no-deps /wheels/*.whl \
    && pip check \
    && rm -rf /wheels
WORKDIR /music
ENV PYTHONDONTWRITEBYTECODE=1
EXPOSE 4381
ENTRYPOINT ["zotify"]
CMD ["--help"]
