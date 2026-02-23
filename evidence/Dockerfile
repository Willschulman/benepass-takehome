FROM node:20-slim

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

EXPOSE 3000

CMD sh -c "npm run sources && npx evidence dev --host 0.0.0.0"
