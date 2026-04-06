FROM nginx:1.27-alpine

# Serve the static web app + gold-only data.
# Everything is self-contained under /usr/share/nginx/html.

WORKDIR /usr/share/nginx/html

# App files
COPY index.html ./
COPY styles.css ./
COPY app.js ./
COPY vendor-jszip.min.js ./
COPY vendor-xlsx.full.min.js ./
COPY brazil-flag.svg ./
COPY brazil-states.geojson ./
COPY "linkedin white.svg" ./
COPY linkedin-blue.svg ./

# Data used by the app
COPY data/ ./data/
