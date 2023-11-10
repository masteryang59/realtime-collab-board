# One Dockerfile, two images:
#   docker build --target realtime -t collab-realtime .   (a Socket.io node)
#   docker build --target web      -t collab-web .        (the Next.js UI)

FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

# Realtime node: plain Node, no build step.
FROM node:22-alpine AS realtime
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY server ./server
COPY shared ./shared
USER node
EXPOSE 4001
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||4001)+'/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "server/index.js"]

FROM deps AS build
WORKDIR /app
COPY . .
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

# Web: Next.js standalone output.
FROM node:22-alpine AS web
WORKDIR /app
ENV NODE_ENV=production NEXT_TELEMETRY_DISABLED=1 PORT=3000 HOSTNAME=0.0.0.0
COPY --from=build /app/.next/standalone ./
COPY --from=build /app/.next/static ./.next/static
USER node
EXPOSE 3000
CMD ["node", "server.js"]

ENV VALIDATE_SCHEMA=true

ENV TRANSCODE_VIDEO=true

ENV CHECK_HEALTH=true
ENV ROLLBACK_CHANGES=true
ENV RESIZE_IMAGE=true