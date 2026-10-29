FROM python:3.11-slim
WORKDIR /app
COPY docker/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && python -c "import duckdb; duckdb.sql('INSTALL httpfs')"
COPY api/ /app/
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
