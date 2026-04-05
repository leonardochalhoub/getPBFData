/* global Plotly */

const DATA_URL = "./gold_pbf_estados_df_geo.json";

// Backwards-compatible:
// - GitHub Pages: publish JSON/GeoJSON at web root (directory paths were 404'ing in prod)
// - older layout: GeoJSON lived at repo root (same path)
const GEOJSON_URL = ["./brazil-states.geojson"];

// If the app is served from a subpath (e.g. GitHub Pages /<repo>/app/web/),
// allow a base override via: ?base=/getPBFData/app/web/
const BASE = (() => {
  try {
    const u = new URL(window.location.href);
    const b = u.searchParams.get("base");
    return b ? b.replace(/\/+$/, "") : "";
  } catch (_) {
    return "";
  }
})();
const withBase = (p) => (BASE ? `${BASE}/${String(p).replace(/^\.?\//, "")}` : p);

function formatNumber(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "null";
  if (Math.abs(v) >= 1e9) return v.toExponential(3);
  return new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 2 }).format(v);
}

function metricLabel(metric) {
  switch (metric) {
    case "valor_2021":
      // Short menu label communicating that values were inflation-adjusted to the 2021 price level
      // (can mean “inflated” or “deflated” depending on the year).
      return "Valor (R$ bi, corr. inflação p/ 2021)";
    case "valor_nominal":
      return "Valor (R$ bi, nominal)";
    // Keep per-capita/per-benef labels short for axis titles, but still signal the 2021 price level.
    // Remove only the “corr.” wording, as requested.
    case "pbfPerBenef":
      return "PBF/beneficiário (R$ inf. p/ 2021)";
    case "pbfPerCapita":
      return "PBF per capita (R$ inf. p/ 2021)";
    default:
      return metric;
  }
}

function metricColorbarTitle(metric) {
  switch (metric) {
    case "valor_2021":
    case "valor_nominal":
      return "R$ bi";
    case "pbfPerBenef":
    case "pbfPerCapita":
      return "R$";
    default:
      return metric;
  }
}

function sumByYear(rows, metric) {
  const sums = new Map();
  for (const r of rows) {
    const y = r.Ano;
    const v = r[metric];

    // Keep all years visible even if values are null.
    // But don't silently turn "all nulls in a year" into a 0 bar (that hides the data issue).
    if (!sums.has(y)) sums.set(y, { sum: 0, n: 0 });

    if (v !== null && v !== undefined && !Number.isNaN(v)) {
      const cur = sums.get(y);
      cur.sum += v;
      cur.n += 1;
    }
  }

  // Convert to Map<year, value|null> where null means "no non-null values"
  const out = new Map();
  for (const [y, obj] of sums.entries()) {
    out.set(y, obj.n === 0 ? null : obj.sum);
  }
  return out;
}

async function loadJson(urlOrUrls) {
  const urls = Array.isArray(urlOrUrls) ? urlOrUrls : [urlOrUrls];

  let lastErr = null;
  for (const url of urls) {
    try {
      const resp = await fetch(url, { cache: "no-store" });
      if (!resp.ok) throw new Error(`Failed to load ${url}: ${resp.status}`);
      return await resp.json();
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error(`Failed to load: ${urls.join(", ")}`);
}

function normalizeGeoJsonIds(geojson) {
  for (const f of geojson.features) {
    if (f.id) continue;
    const p = f.properties || {};
    f.id = p.sigla || p.SIGLA || p.uf || p.UF || p.abbrev || null;
  }
  return geojson;
}

function getReverseScale(colorscale) {
  // Ensure low=bright and high=dark for all scales.
  // Plotly palettes differ: some are dark→bright by default and must be reversed.
  return [
    "Viridis",
    "Cividis",
    "Blues",
    "Greens",
    "Greys",
    "YlGnBu",
    "YlOrRd",
  ].includes(colorscale);
}

function renderBar({ rows, years, metric }) {
  const isDark = document.body.classList.contains("dark");

  // Dark-mode tuned palette for bars (high contrast, colorblind-friendly-ish)
  const barColor = isDark ? "#60a5fa" : "#2b6cb0";
  const textColor = isDark ? "rgba(232, 238, 249, 0.92)" : "#111";
  const gridColor = isDark ? "rgba(255,255,255,0.12)" : "rgba(0,0,0,0.08)";
  const bg = isDark ? "#0f172a" : "#ffffff";

  const sums = sumByYear(rows, metric);
  const x = years;
  const y = years.map((yy) => (sums.get(yy) === undefined ? null : sums.get(yy)));

  // Text labels above each bar
  const text = y.map((v) => (v === null || v === undefined || Number.isNaN(v) ? "" : formatNumber(v)));

  Plotly.newPlot(
    "barWrap",
    [
      {
        type: "bar",
        x,
        y,
        marker: { color: barColor },
        text,
        textposition: "outside",
        cliponaxis: false,
        hovertemplate: "Ano=%{x}<br>Valor=%{y}<extra></extra>",
      },
    ],
    {
      paper_bgcolor: bg,
      plot_bgcolor: bg,
      margin: { t: 10, r: 10, b: 70, l: 70 },
      yaxis: {
        title: metricLabel(metric),
        automargin: true,
        gridcolor: gridColor,
        zerolinecolor: gridColor,
        tickfont: { color: textColor },
        titlefont: { color: textColor },
      },
      xaxis: {
        title: "Ano",
        type: "category",
        tickmode: "array",
        tickvals: x,
        ticktext: x.map(String),
        tickangle: -45,
        automargin: true,
        gridcolor: "rgba(0,0,0,0)",

        tickfont: { color: textColor },
        titlefont: { color: textColor },
      },
      font: { color: textColor },
      annotations: [],
    },
    { displayModeBar: false, responsive: true }
  );
}

function renderMap({ rows, metric, colorscale, yearValue, years, geojson }) {
  const isDark = document.body.classList.contains("dark");

  const textColor = isDark ? "rgba(232, 238, 249, 0.92)" : "#111";
  const bg = isDark ? "#0f172a" : "#ffffff";
  const borderColor = isDark ? "rgba(255,255,255,0.45)" : "#ffffff";

  const isAgg = yearValue === "AGG";
  const year = isAgg ? null : parseInt(yearValue, 10);

  const reverseScale = getReverseScale(colorscale);

  const rowsYear = isAgg ? rows : rows.filter((r) => r.Ano === year);

  // Aggregate per UF when isAgg=true; otherwise pass through the year slice.
  const byUf = new Map();
  for (const r of rowsYear) {
    const uf = r.uf;
    if (!byUf.has(uf)) {
      byUf.set(uf, {
        uf,
        metricSum: 0,
        metricN: 0,
        popLast: null,
        popLastYear: -Infinity,
      });
    }
    const acc = byUf.get(uf);

    const v = r[metric];
    if (v !== null && v !== undefined && !Number.isNaN(v)) {
      acc.metricSum += v;
      acc.metricN += 1;
    }

    // For hover: show population from the most recent year present.
    if (r.populacao !== null && r.populacao !== undefined && !Number.isNaN(r.populacao)) {
      if (r.Ano >= acc.popLastYear) {
        acc.popLastYear = r.Ano;
        acc.popLast = r.populacao;
      }
    }
  }

  const rowsUf = Array.from(byUf.values()).sort((a, b) => a.uf.localeCompare(b.uf));
  const locations = rowsUf.map((r) => r.uf);
  const z = rowsUf.map((r) => (r.metricN === 0 ? null : r.metricSum));
  const hover = rowsUf.map((r) => {
    const label = metricLabel(metric);
    const val = r.metricN === 0 ? null : r.metricSum;
    const pop = r.popLast;
    const popSuffix = r.popLastYear > -Infinity ? ` (ano ${r.popLastYear})` : "";
    const title = isAgg ? `${label} (acumulado)` : label;
    return `${r.uf}<br>${title}: ${formatNumber(val)}<br>Pop${popSuffix}: ${formatNumber(pop)}`;
  });

  // Shorten + split the AGG title into 2 lines and give it more top margin.
  // Plotly annotations don't auto-wrap reliably, so we insert a <br>.
  const mapTitle = isAgg
    ? `Distribuição por UF<br>Acumulado ${years[0]}–${years[years.length - 1]}`
    : `Distribuição por UF — ${year}`;

  const titleFontSize = isAgg ? 16 : 20;
  const titleY = isAgg ? 1.06 : 1.02;

  Plotly.newPlot(
    "mapWrap",
    [
      {
        type: "choropleth",
        geojson,
        featureidkey: "id",
        locations,
        z,
        text: hover,
        hovertemplate: "%{text}<extra></extra>",
        colorscale,
        reversescale: reverseScale,
        colorbar: {
          title: { text: metricColorbarTitle(metric), font: { color: textColor } },
          tickfont: { color: textColor },
          outlinecolor: isDark ? "rgba(255,255,255,0.18)" : "rgba(0,0,0,0.15)",
        },
        marker: { line: { color: borderColor, width: 0.6 } },
      },
    ],
    {
      paper_bgcolor: bg,
      plot_bgcolor: bg,
      // More top margin so the 2-line AGG title doesn't get clipped.
      margin: { t: isAgg ? 96 : 55, r: 10, b: 10, l: 10 },
      geo: {
        fitbounds: "locations",
        visible: false,
        projection: { type: "mercator", scale: 1.55 },
        center: { lat: -14, lon: -52 },
        bgcolor: bg,
      },
      annotations: [
        {
          xref: "paper",
          yref: "paper",
          x: 0.5,
          y: titleY,
          xanchor: "center",
          yanchor: "bottom",
          showarrow: false,
          text: mapTitle,
          align: "center",
          bgcolor: "rgba(0,0,0,0)",
          borderwidth: 0,
          borderpad: 0,
          font: { size: titleFontSize, color: textColor },
        },
      ],
      font: { color: textColor },
    },
    { displayModeBar: false, responsive: true }
  );
}

function initBuildDate() {
  const el = document.getElementById("buildDate");
  if (el) el.textContent = new Date().toLocaleDateString("pt-BR");
}

function setLoading(isLoading) {
  const el = document.getElementById("loading");
  if (!el) return;
  el.style.display = isLoading ? "block" : "none";
}

function setError(msg) {
  const el = document.getElementById("error");
  if (!el) return;
  el.textContent = msg || "";
  el.style.display = msg ? "block" : "none";
}

function safeFileToken(s) {
  return String(s).replace(/[^a-zA-Z0-9._-]+/g, "_");
}

function computeMapRows({ rowsYear, years, metric, yearValue }) {
  // Aggregate per UF (sum), plus last known population for hover/export
  const byUf = new Map();

  for (const r of rowsYear) {
    const uf = r.uf;
    if (!byUf.has(uf)) {
      byUf.set(uf, {
        uf,
        metricSum: 0,
        metricN: 0,
        popLast: null,
        popLastYear: -Infinity,
      });
    }
    const acc = byUf.get(uf);

    const v = r[metric];
    if (v !== null && v !== undefined && !Number.isNaN(v)) {
      acc.metricSum += v;
      acc.metricN += 1;
    }

    if (r.populacao !== null && r.populacao !== undefined && !Number.isNaN(r.populacao)) {
      if (r.Ano >= acc.popLastYear) {
        acc.popLastYear = r.Ano;
        acc.popLast = r.populacao;
      }
    }
  }

  const label = metricLabel(metric);

  return Array.from(byUf.values())
    .sort((a, b) => a.uf.localeCompare(b.uf))
    .map((r) => ({
      UF: r.uf,
      Ano: yearValue === "AGG" ? `AGG_${years[0]}_${years[years.length - 1]}` : Number(yearValue),
      Metrica: metric,
      MetricaLabel: label,
      Valor: r.metricN === 0 ? null : r.metricSum,
      Populacao: r.popLast,
      PopulacaoAno: r.popLastYear > -Infinity ? r.popLastYear : null,
    }));
}

function sheetNameFor(metric, kind) {
  // Excel sheet name limit: 31 chars
  const base = {
    valor_2021: "Valor_2021",
    valor_nominal: "Valor_Nominal",
    pbfPerBenef: "PBF_por_Benef",
    pbfPerCapita: "PBF_per_Capita",
  }[metric] || metric;

  const suffix = kind === "bar" ? "Barras" : "Mapa";
  return `${base}_${suffix}`.slice(0, 31);
}

function buildExcelWorkbookAll({ rows, years }) {
  const wb = XLSX.utils.book_new();

  const metrics = ["valor_2021", "valor_nominal", "pbfPerBenef", "pbfPerCapita"];

  for (const metric of metrics) {
    // Bars sheet: Brazil totals per year
    const sums = sumByYear(rows, metric);
    const barRows = years.map((y) => ({
      Ano: y,
      Metrica: metric,
      MetricaLabel: metricLabel(metric),
      TotalBrasil: sums.get(y) === undefined ? null : sums.get(y),
    }));
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(barRows), sheetNameFor(metric, "bar"));

    // Map sheet: All years + AGG
    const mapAllRows = [];

    for (const y of years) {
      const rowsYear = rows.filter((r) => r.Ano === y);
      mapAllRows.push(...computeMapRows({ rowsYear, years, metric, yearValue: String(y) }));
    }

    // Aggregated across all years
    mapAllRows.push(...computeMapRows({ rowsYear: rows, years, metric, yearValue: "AGG" }));

    XLSX.utils.book_append_sheet(
      wb,
      XLSX.utils.json_to_sheet(mapAllRows),
      sheetNameFor(metric, "map")
    );
  }

  return wb;
}

function downloadExcelComplete({ rows, years }) {
  const wb = buildExcelWorkbookAll({ rows, years });

  const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "");
  const filename = `bolsa_familia_completo_${stamp}.xlsx`;

  XLSX.writeFile(wb, filename, { compression: true });
}

function dataUrlToU8(dataUrl) {
  const base64 = dataUrl.split(",")[1];
  const binStr = atob(base64);
  const len = binStr.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i += 1) bytes[i] = binStr.charCodeAt(i);
  return bytes;
}

function triggerDownloadBlob(blob, filename) {
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 10_000);
}

async function toPngBytes(divId, { width, height, scale = 2 }) {
  const dataUrl = await Plotly.toImage(divId, {
    format: "png",
    width,
    height,
    scale,
  });
  return dataUrlToU8(dataUrl);
}

async function downloadAllMapsZip({
  rows,
  years,
  metric,
  colorscale,
  geojson,
  // Bigger PNGs (Brazil was too small)
  mapSize = { width: 1800, height: 1300 },
  barSize = { width: 1600, height: 900 },
}) {
  // Create hidden offscreen containers so the user doesn't see plots changing.
  const scratch = document.createElement("div");
  scratch.style.position = "fixed";
  scratch.style.left = "-10000px";
  scratch.style.top = "0";
  scratch.style.width = "10px";
  scratch.style.height = "10px";
  scratch.style.overflow = "hidden";
  scratch.style.pointerEvents = "none";
  scratch.style.opacity = "0";
  document.body.appendChild(scratch);

  const barDiv = document.createElement("div");
  barDiv.id = "barExport";
  const mapDiv = document.createElement("div");
  mapDiv.id = "mapExport";
  scratch.appendChild(barDiv);
  scratch.appendChild(mapDiv);

  try {
    // Prepare ZIP
    const zip = new JSZip();
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "");
    const folder = zip.folder(`bolsa_familia_${safeFileToken(colorscale)}_${stamp}`);

    const metrics = ["valor_2021", "valor_nominal", "pbfPerBenef", "pbfPerCapita"];

    async function toPngBytesFromDiv(divEl, { width, height, scale = 2 }) {
      const dataUrl = await Plotly.toImage(divEl, { format: "png", width, height, scale });
      return dataUrlToU8(dataUrl);
    }

    // Render + capture without touching the visible charts
    async function renderBarAndCapture(metricKey) {
      const isDark = document.body.classList.contains("dark");

      const barColor = isDark ? "#60a5fa" : "#2b6cb0";
      const textColor = isDark ? "rgba(232, 238, 249, 0.92)" : "#111";
      const gridColor = isDark ? "rgba(255,255,255,0.12)" : "rgba(0,0,0,0.08)";
      const bg = isDark ? "#0f172a" : "#ffffff";

      const sums = sumByYear(rows, metricKey);
      const x = years;
      const y = years.map((yy) => (sums.get(yy) === undefined ? null : sums.get(yy)));
      const text = y.map((v) => (v === null || v === undefined || Number.isNaN(v) ? "" : formatNumber(v)));

      await Plotly.newPlot(
        barDiv,
        [
          {
            type: "bar",
            x,
            y,
            marker: { color: barColor },
            text,
            textposition: "outside",
            cliponaxis: false,
          },
        ],
        {
          paper_bgcolor: bg,
          plot_bgcolor: bg,
          margin: { t: 10, r: 10, b: 70, l: 70 },
          yaxis: {
            title: metricLabel(metricKey),
            automargin: true,
            gridcolor: gridColor,
            zerolinecolor: gridColor,
            tickfont: { color: textColor },
            titlefont: { color: textColor },
          },
          xaxis: {
            title: "Ano",
            type: "category",
            tickmode: "array",
            tickvals: x,
            ticktext: x.map(String),
            tickangle: -45,
            automargin: true,
            gridcolor: "rgba(0,0,0,0)",
            tickfont: { color: textColor },
            titlefont: { color: textColor },
          },
          font: { color: textColor },
        },
        { displayModeBar: false, responsive: true }
      );

      return toPngBytesFromDiv(barDiv, barSize);
    }

    async function renderMapAndCapture(metricKey, yearValue) {
      const isDark = document.body.classList.contains("dark");

      const textColor = isDark ? "rgba(232, 238, 249, 0.92)" : "#111";
      const bg = isDark ? "#0f172a" : "#ffffff";
      const borderColor = isDark ? "rgba(255,255,255,0.45)" : "#ffffff";

      const isAgg = yearValue === "AGG";
      const year = isAgg ? null : parseInt(yearValue, 10);
      const reverseScale = getReverseScale(colorscale);

      const rowsYear = isAgg ? rows : rows.filter((r) => r.Ano === year);

      const byUf = new Map();
      for (const r of rowsYear) {
        const uf = r.uf;
        if (!byUf.has(uf)) byUf.set(uf, { uf, metricSum: 0, metricN: 0 });
        const acc = byUf.get(uf);
        const v = r[metricKey];
        if (v !== null && v !== undefined && !Number.isNaN(v)) {
          acc.metricSum += v;
          acc.metricN += 1;
        }
      }

      const rowsUf = Array.from(byUf.values()).sort((a, b) => a.uf.localeCompare(b.uf));
      const locations = rowsUf.map((r) => r.uf);
      const z = rowsUf.map((r) => (r.metricN === 0 ? null : r.metricSum));

      const mapTitle = isAgg
        ? `Distribuição por UF — acumulado ${years[0]}–${years[years.length - 1]}`
        : `Distribuição por UF — ${year}`;

      await Plotly.newPlot(
        mapDiv,
        [
          {
            type: "choropleth",
            geojson,
            featureidkey: "id",
            locations,
            z,
            colorscale,
            reversescale: reverseScale,
            colorbar: {
              title: { text: metricColorbarTitle(metricKey), font: { color: textColor } },
              tickfont: { color: textColor },
              outlinecolor: isDark ? "rgba(255,255,255,0.18)" : "rgba(0,0,0,0.15)",
            },
            marker: { line: { color: borderColor, width: 0.6 } },
          },
        ],
        {
          paper_bgcolor: bg,
          plot_bgcolor: bg,
          margin: { t: 55, r: 10, b: 10, l: 10 },
          geo: {
            fitbounds: "locations",
            visible: false,
            projection: { type: "mercator", scale: 1.55 },
            center: { lat: -14, lon: -52 },
            bgcolor: bg,
          },
          annotations: [
            {
              xref: "paper",
              yref: "paper",
              x: 0.5,
              y: 1.02,
              xanchor: "center",
              yanchor: "bottom",
              showarrow: false,
              text: mapTitle,
              font: { size: 20, color: textColor },
            },
          ],
          font: { color: textColor },
        },
        { displayModeBar: false, responsive: true }
      );

      return toPngBytesFromDiv(mapDiv, mapSize);
    }

    for (const metricKey of metrics) {
      const metricFolder = folder.folder(safeFileToken(metricKey));

      // Bar chart
      {
        const bytes = await renderBarAndCapture(metricKey);
        metricFolder.file(`barras_total_brasil_${safeFileToken(metricKey)}.png`, bytes);
      }

      // Map AGG
      {
        const bytes = await renderMapAndCapture(metricKey, "AGG");
        metricFolder.file(`mapa_AGG_${years[0]}-${years[years.length - 1]}_${safeFileToken(metricKey)}.png`, bytes);
      }

      // Maps each year
      for (const y of years) {
        const bytes = await renderMapAndCapture(metricKey, String(y));
        metricFolder.file(`mapa_${y}_${safeFileToken(metricKey)}.png`, bytes);
      }
    }

    const blob = await zip.generateAsync({ type: "blob", compression: "DEFLATE" });
    const zipName = `bolsa_familia_mapas_${safeFileToken(colorscale)}_${stamp}.zip`;
    triggerDownloadBlob(blob, zipName);
  } finally {
    scratch.remove();
  }
}

function applyTheme(theme) {
  const isDark = theme === "dark";
  document.body.classList.toggle("dark", isDark);
  try {
    localStorage.setItem("theme", isDark ? "dark" : "light");
  } catch (_) {
    // ignore
  }

  const toggle = document.getElementById("toggleDark");
  if (toggle && toggle.type === "checkbox") {
    toggle.checked = isDark;
  }
}

function initTheme() {
  let saved = null;
  try {
    saved = localStorage.getItem("theme");
  } catch (_) {
    saved = null;
  }

  if (saved === "dark" || saved === "light") {
    applyTheme(saved);
    return;
  }

  const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  applyTheme(prefersDark ? "dark" : "light");
}

async function main() {
  initBuildDate();
  initTheme();
  setLoading(true);
  setError("");

  const metricSel = document.getElementById("metric");
  const colorscaleSel = document.getElementById("colorscale");
  const yearSel = document.getElementById("year");

  try {
    const [rows, geojsonRaw] = await Promise.all([
      loadJson(withBase(DATA_URL)),
      loadJson(GEOJSON_URL.map(withBase)),
    ]);
    const geojson = normalizeGeoJsonIds(geojsonRaw);

    const years = Array.from(new Set(rows.map((r) => r.Ano))).sort((a, b) => a - b);

    // Build year dropdown dynamically + add an aggregate option (all years in the dataset).
    yearSel.innerHTML =
      `<option value="AGG">Acumulado (${years[0]}–${years[years.length - 1]})</option>` +
      years.map((y) => `<option value="${y}">${y}</option>`).join("");
    yearSel.value = years[years.length - 1];

    const downloadBtn = document.getElementById("downloadXlsx");
    const downloadAllPngBtn = document.getElementById("downloadAllPng");
    const toggleDarkBtn = document.getElementById("toggleDark");

    function render() {
      const metric = metricSel.value;
      const colorscale = colorscaleSel.value;
      const yearValue = yearSel.value;

      renderBar({ rows, years, metric });
      renderMap({ rows, metric, colorscale, yearValue, years, geojson });
    }

    function onDownload() {
      downloadExcelComplete({ rows, years });
    }

    async function onDownloadAllPng() {
      const metric = metricSel.value;
      const colorscale = colorscaleSel.value;

      // Basic guardrails (this can take a while)
      setLoading(true);
      setError("");
      try {
        await downloadAllMapsZip({ rows, years, metric, colorscale, geojson });
      } catch (e) {
        setError(e && e.stack ? e.stack : String(e));
      } finally {
        setLoading(false);
      }
    }

    metricSel.addEventListener("change", render);
    colorscaleSel.addEventListener("change", render);
    yearSel.addEventListener("change", render);
    if (downloadBtn) downloadBtn.addEventListener("click", onDownload);
    if (downloadAllPngBtn) downloadAllPngBtn.addEventListener("click", onDownloadAllPng);
    if (toggleDarkBtn) {
      // Checkbox toggle
      toggleDarkBtn.addEventListener("change", () => {
        applyTheme(toggleDarkBtn.checked ? "dark" : "light");
        render(); // re-render charts so Plotly picks up dark palette
      });
    }

    render();
  } catch (e) {
    setError(e && e.stack ? e.stack : String(e));
  } finally {
    setLoading(false);
  }
}

main();
