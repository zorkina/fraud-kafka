FROM python:3.11-slim-bookworm
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd --create-home --uid 1000 appuser
COPY --chown=appuser:appuser app app
COPY --chown=appuser:appuser scripts scripts
COPY --chown=appuser:appuser models models
COPY --chown=appuser:appuser tests tests
RUN mkdir data && chown appuser:appuser data
USER appuser
CMD ["python", "-m", "app.consumer"]
