FROM node:22-slim AS build
WORKDIR /app
COPY dashboard/package.json dashboard/package-lock.json* ./
RUN npm install
COPY dashboard .
# Baked in at build time: this is client-side JS running in the user's
# browser, so it must reach the API at the HOST-exposed port, not the
# Docker-network-internal "api:8000" hostname.
ARG VITE_API_BASE=http://localhost:8080
ENV VITE_API_BASE=${VITE_API_BASE}
RUN npm run build

FROM nginx:1.30-alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
