FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install -e .
COPY configs ./configs
COPY scripts ./scripts
RUN chmod +x scripts/entrypoint.sh
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health').status==200 else 1)"
ENTRYPOINT ["scripts/entrypoint.sh"]
CMD ["uvicorn", "sentinalops.serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
