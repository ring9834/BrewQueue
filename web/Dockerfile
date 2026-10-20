# --- build ---
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json .npmrc ./
# Scripts are skipped here because `nuxt prepare` needs the source, which isn't copied yet.
RUN npm ci --ignore-scripts
COPY . .
RUN npm run build

# --- run (Nitro's self-contained Node server) ---
FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000
COPY --from=build /app/.output ./.output
USER node
EXPOSE 3000
CMD ["node", ".output/server/index.mjs"]
