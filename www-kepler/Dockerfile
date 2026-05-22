# Multi-stage build: Vite (React + kepler.gl) → nginx serving dist/.
# Expected build context: ./www-kepler (this directory).
# docker-compose sets:
#   context: ./www-kepler
#   dockerfile: Dockerfile

FROM node:22-alpine AS build
WORKDIR /app
# Forwarded from docker-compose build args. Re-exported with the VITE_
# prefix so `vite build` exposes it via import.meta.env in App.jsx.
ARG MAPBOX_ACCESS_TOKEN=
ENV VITE_MAPBOX_ACCESS_TOKEN=$MAPBOX_ACCESS_TOKEN
COPY package.json package-lock.json* ./
# Use install (not ci) for the first build — there's no committed lockfile
# yet. Once the build settles, commit the generated lockfile and switch to
# `npm ci`.
# --legacy-peer-deps because react-palm peer-depends on enzyme-adapter-utils
# which still pins React ^16. enzyme is test-only and not bundled, so we
# accept the peer-dep mismatch to keep a single hoisted react-palm copy
# (required for Vite resolve.dedupe to collapse all kepler-internal imports
# to one module instance — otherwise react-palm's task registry fragments
# across 4 copies and dispatch fails at runtime).
RUN npm install --no-audit --no-fund --legacy-peer-deps
COPY . ./
RUN npm run build

FROM nginx:alpine
# Vite builds with base '/kepler/', so the emitted index.html references
# /kepler/assets/* etc. Mounting dist/ at /usr/share/nginx/html/kepler/
# matches the public URL prefix.
COPY --from=build /app/dist /usr/share/nginx/html/kepler/
# Replace the stock default.conf (which listens on :80) with our /kepler/
# server block listening on :8088. Host networking means port collisions
# with the parent host nginx (:8080) and stock :80 must be avoided.
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 8088
