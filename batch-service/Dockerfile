# Use uma imagem base do Python
FROM python:3.9-slim

# Define o diretório de trabalho
WORKDIR /app

# Copia o requirements.txt e instala as dependências
COPY . .
RUN ls -lh
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código do app para o contêiner
COPY . .

EXPOSE 8080

# Comando de execução do script Python
CMD ["python", "main.py"]
