FROM node:lts-alpine as build

WORKDIR /app

COPY tsconfig.json ./
COPY package*.json ./
COPY ./src ./src
COPY .env ./

RUN npm install
RUN npm run build

CMD ["node", "./build/index.js"]
