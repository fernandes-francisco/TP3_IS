FROM node:18-alpine

WORKDIR /app

# Instala apenas dependencias de prod
COPY package*.json ./
RUN npm install --omit=dev

# Copia o restante do codigo
COPY . .

EXPOSE 8000 5001

CMD ["node", "src/index.js"]
