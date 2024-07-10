# Use uma imagem base do Python
FROM python:3.9-slim

# Define o diretório de trabalho
WORKDIR /app

# Copia o requirements.txt e instala as dependências
COPY batch-service/requirements.txt .
COPY batch-service/main.py .
RUN pip install --no-cache-dir -r requirements.txt

# Comando de execução do script Python
CMD ["python", "main.py"]
