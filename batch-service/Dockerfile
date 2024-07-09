# Use uma imagem base do Python
FROM python:3.9-slim

# Defina o diretório de trabalho no contêiner
WORKDIR /app

# Copie os arquivos de requisitos e instale as dependências
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copie o código da aplicação para o contêiner
COPY . .

# Defina o comando de execução da aplicação
CMD ["python", "main.py"]
