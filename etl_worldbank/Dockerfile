FROM python:3.11-slim

WORKDIR /app

# Evita .pyc e garante logs em tempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Dependências primeiro (cache de camada)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código-fonte
COPY src/ ./src/

CMD ["python", "src/main.py"]
