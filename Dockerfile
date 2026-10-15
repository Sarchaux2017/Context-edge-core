# ---------------------------------------------------------------------------
# Vaultcore :: vaultcore-infrastructure
# Stage 1 - build: install all deps, typecheck, bundle with esbuild
# ---------------------------------------------------------------------------
FROM node:20-alpine AS build
WORKDIR /app

COPY package.json package-lock.json* tsconfig.json ./
RUN if [ -f package-lock.json ]; then npm ci; else npm install; fi

COPY gatewayRouter.js localEngine.js ./
RUN npm run typecheck && npm run build

# ---------------------------------------------------------------------------
# Stage 2 - prod-deps: production-only node_modules, caches cleared
# ---------------------------------------------------------------------------
FROM node:20-alpine AS prod-deps
WORKDIR /app
ENV NODE_ENV=production

COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev; fi \
    && npm cache clean --force \
    && rm -rf /root/.npm /tmp/*

# ---------------------------------------------------------------------------
# Stage 3 - runtime: minimal, non-root, tini as PID 1
# ---------------------------------------------------------------------------
FROM node:20-alpine AS runtime
ENV NODE_ENV=production \
    PORT=8080 \
    HOST=0.0.0.0

RUN apk add --no-cache tini

WORKDIR /app

# package.json is required at runtime so Node treats dist/*.js as ESM ("type": "module").
COPY --chown=node:node package.json ./
COPY --chown=node:node --from=prod-deps /app/node_modules ./node_modules
COPY --chown=node:node --from=build /app/dist ./dist

USER node
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||8080)+'/healthz',{signal:AbortSignal.timeout(4000)}).then(async r=>{const b=await r.json();process.exit(r.ok&&(b.status==='ok'||b.status==='degraded')?0:1)}).catch(()=>process.exit(1))"

ENTRYPOINT ["/sbin/tini", "--", "node", "dist/gatewayRouter.js"]
