FROM nginx:1.27-alpine
# Serve the static web app + gold-only data.
# Everything is self-contained under /usr/share/nginx/html.

COPY index.html /usr/share/nginx/html/
COPY styles.css /usr/share/nginx/html/
COPY app.js /usr/share/nginx/html/
COPY vendor-jszip.min.js /usr/share/nginx/html/
COPY vendor-xlsx.full.min.js /usr/share/nginx/html/
COPY brazil-flag.svg /usr/share/nginx/html/

# Data + geojson used by the app
COPY data/ /usr/share/nginx/html/data/
