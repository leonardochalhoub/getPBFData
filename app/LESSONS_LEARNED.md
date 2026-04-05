# Lessons Learned (Task: IBGE population completeness 2013–2026 + linear estimation)

## What went wrong
1. **Over-reliance on heuristics for column detection**
   - I initially tried to “guess” `year` and `population` columns in `arquivos_aux/populacao.xlsx`.
   - The file is *wide* (years are columns), not *long* (Ano/populacao rows), so that approach was fundamentally wrong.
   - The heuristic mistakenly selected `uf` and `nome_estado`, leading to exceptions and wasted cycles.

2. **Not adapting quickly after discovering the real schema**
   - Once we saw the real columns (`uf`, `nome_estado`, `2013`…`2023`), the correct next step should have been:
     - stop treating it as a (Ano, populacao) long table,
     - explicitly “melt/unpivot” years if we were to use that file at all,
     - or move directly to the IBGE API approach (as requested).

3. **Tool execution interruptions (“got stuck”)**
   - Multiple `execute_command` steps did not return results due to interruptions.
   - This created uncertainty about whether the IBGE API calls completed and prevented a clean end-to-end verification.

4. **Duplication / formatting issues in tool calls**
   - Some responses accidentally contained duplicated tool blocks or extraneous text, which can confuse the workflow and increase the chance of interruption.

## What we learned
1. **Inspect the data shape before writing logic**
   - For spreadsheets, first print columns + dtypes and identify whether data is wide vs. long.
   - Only then decide on parsing strategy (unpivot vs direct read).

2. **Prefer explicitness over heuristics**
   - When the schema is known (or can be read), explicitly reference columns/fields.
   - Avoid “guess the column” logic unless there is no alternative.

3. **When data must cover a full year range, build a complete grid first**
   - Correct approach for “must have every UF x Year”:
     - build a UF list,
     - build a Year list,
     - cross join to create a full panel,
     - left join raw data,
     - fill gaps (interpolate inside, extrapolate edges).

4. **Separate concerns:**
   - **Fetch official data** (IBGE API) and keep it raw/traceable.
   - **Fill missing values** as a separate deterministic step (linear interpolation/extrapolation).
   - **Consume in gold** by joining on (Ano, uf).

## Confirmed state of the codebase
- The gold job `app/src/gold/pbf_estados_df_geo.py` already contains:
  - IBGE API download for UF population: `_download_ibge_uf_population_estimates`
  - JSON → Spark DF conversion: `_ibge_population_json_to_df`
  - Full UF x year grid + linear interpolation/extrapolation fill: `_read_populacao_estados`
- This is aligned with the requirement: **use official IBGE data and linearly estimate missing years**.

## Concrete recommendations to prevent “stuck” sessions
1. **Use smaller, verifiable commands**
   - Start with a simple request (e.g., fetch one UF or one year range) and print a short summary.
2. **Avoid chaining `curl | python` unless necessary**
   - Prefer a single Python script with `requests` so errors are clearer.
3. **Add a local “verification command”**
   - Example checks:
     - count rows == 27 UFs * (end-start+1)
     - no null populations after fill
     - years cover exactly 2013..2026
4. **Never leave the user without an answer due to a long/fragile verification step**
   - First: answer the question directly using what’s already known from the code/data model.
   - Then: run verification as a follow-up (and keep it short, robust, and restartable).
   - If verification fails/interrupts, still provide the partial conclusion + what remains to be verified.
5. **Avoid multi-line heredocs in chat-driven terminals when they’re error-prone**
   - If a multi-line snippet breaks, the shell starts executing Python lines as bash (causing many `syntax error near unexpected token` errors).
   - Prefer one of:
     - a single `python -c "..."` (short),
     - or write a small script file (e.g., `app/scripts/compare_population.py`) and run it,
     - or ensure the heredoc delimiter (`PY`) is the only token on its line with no stray prompts or copy/paste artifacts.
6. **Don’t paste Python statements directly into bash**
   - Errors like `from: can't read /var/mail/...` and `bash: syntax error near unexpected token '('` happen when Python code is executed by the shell.
   - Always run Python snippets via `python -c '...'`, `python - <<'PY' ... PY`, or a `.py` script (`python path/to/script.py`).
   - For project modules, prefer `PYTHONPATH=. python -m app.src....` or set `PYTHONPATH=.` when running scripts directly so imports like `from app.src...` resolve.
7. **For web exports, always generate data via a script (never manual interactive snippets)**
   - Exporting Delta → CSV/JSON is multi-step and easy to break when pasted line-by-line into bash.
   - Standardize on a single command such as:
     - `PYTHONPATH=. python app/scripts/export_gold_for_web.py`
   - The script should:
     - read gold Delta,
     - filter out “Agregado …” rows,
     - select only needed columns,
     - write a deterministic output file into `exports/web/...`.
8. **Don’t paste HTML-escaped shell commands into bash**
   - If `&&` turns into `&&` (HTML-escaped), bash will error with e.g.: `bash: syntax error near unexpected token \`;&'`.
   - Workarounds:
     - Prefer `;` instead of `&&` when you’re copy/pasting from chat (works for servers *and* file ops):  
       `cd exports/web; python -m http.server 8000 --bind 127.0.0.1`  
       `cp -f exports/web/index.html app/web/index.html; cp -f exports/web/app.js app/web/app.js; cp -f exports/web/styles.css app/web/styles.css`
     - Or re-type operators manually (ensure it is literally `&&`, not `&&`).
9. **Avoid heredocs if the channel can HTML-escape them**
   - If `<<'PY'` gets converted to `<<'PY'` (HTML escape), the shell will interpret it literally and the heredoc won’t start.
   - In those environments, prefer:
     - a `.py` script checked into the repo and executed with `PYTHONPATH=. python path/to/script.py`, or
     - a short `python -c "..."` one-liner.
10. **For web maps, avoid relying on “magic” location modes**
   - Plotly’s `locationmode: "BR-states"` is not reliable.
   - Always use a GeoJSON file and explicitly set `featureidkey` to a property that matches your dataset keys (here: UF sigla).
   - Keep the GeoJSON local (served from the same webroot) to avoid CORS / blocked outbound fetches.
11. **If a choropleth looks “blank”, check if your metric is all-null for that year**
   - Plotly will effectively render “nothing” if `z` is entirely null (or if all values are filtered out).
   - In this project, `valor_2021`, `pbfPerBenef`, `pbfPerCapita` are null for 2022–2026 because the deflator table is only built to 2021, so the deflator join fails for later years.
   - Use `valor_nominal` to visualize 2022–2026 unless/until deflators are extended.
12. **Gold can have “missing” derived metrics even when silver has the raw data**
   - Silver has nominal values for 2022–2026, but gold’s *real* (2021-adjusted) metrics depend on an IPCA deflator time series.
   - If the deflator table stops at 2021, then 2022–2026 `valor_2021`, `pbfPerBenef`, `pbfPerCapita` become null even though `valor_nominal` exists.
   - Fix is to extend the deflator series beyond 2021 (or change the base year) and then rebuild/re-export gold.

13. **Serving the web export locally (what worked / now stable / confirmed)**
   - “Link doesn’t work” troubleshooting: `http://127.0.0.1:8000/` only works *after* the server is running. If you see `Connection refused`, start the server again (it was stopped / terminal closed).
   - Recommended steps:
     - `cd "/home/leochalhoub/getPBFData"`
     - `cd app/web && python -m http.server 8000 --bind 127.0.0.1`
   - Then open in the browser: `http://127.0.0.1:8000/`
   - Correct command to start listening (run from the repo root):
     - `cd exports/web && python -m http.server 8000 --bind 127.0.0.1`
   - Then open (manually in your browser):
     - `http://127.0.0.1:8000/` (or `http://localhost:8000/`)
   - Note: in this environment, auto-opening the browser from the terminal may fail:
     - `xdg-open http://127.0.0.1:8000/` fails when `xdg-open` is not installed (`Command 'xdg-open' not found`).
     - Python `webbrowser.open(...)` may also fail with `gio: ... Operation not supported`.
   - If you see `bash: syntax error near unexpected token \`;&'`, it almost always means the command got HTML-escaped (`&&`). Re-type the command manually with real `&&`.
   - Status: after fixing the GeoJSON matching (`featureidkey`) and limiting the export to 2013–2025, the choropleth renders for all metric options.
   - Workflow quirk: sometimes I need to explicitly ask “start listening” twice before the server command actually gets executed / stays running in the terminal.
   - What actually worked reliably (confirmed working):
     - First ensure you are at the repo root: `cd "/home/leochalhoub/getPBFData"`
     - Then start the server: `cd exports/web && python -m http.server 8000 --bind 127.0.0.1`
   - Visualization: for colorblind-friendly bright→dark mapping (small=bright, large=dark), use a sequential palette like Plotly `Viridis`/`Cividis` and ensure the direction is correct (set `reversescale` as needed). In practice it’s easy to get this backwards, so make it a UI option and/or verify by checking min/max UFs. Plotly palette defaults differ: some palettes (e.g. Viridis/Cividis) may need `reversescale=true` to get low=bright/high=dark, while others (e.g. Plasma) may already be correct.

## Suggested verification snippet (manual run)
```bash
python -c "
import requests, json
START, END = 2013, 2026
url = f'https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/{START}-{END}/variaveis/9324?localidades=N3[all]'
r=requests.get(url,timeout=60); r.raise_for_status()
print('bytes', len(r.content))
print('ok')
"
```

Then validate the Spark side by running the gold job and checking the resulting delta table partitions.
