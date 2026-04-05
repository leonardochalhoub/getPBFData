# getPBFData (Python + Spark + Delta Lake + Web)

Este repositório automatiza a **coleta** e o **processamento** dos dados públicos de pagamentos do:

- **Bolsa Família (PBF)**
- **Auxílio Brasil**
- **Novo Bolsa Família (NBF)**

a partir do **Portal da Transparência (CGU)**, construindo um *lakehouse* local em camadas (**bronze → silver → gold**) e exportando um *snapshot* “pronto para navegador” para um **web app** (HTML/CSS/JavaScript + Plotly).

## Por que isso existe (e por que não dá para usar Excel)

O Portal disponibiliza os dados como **ZIPs mensais** contendo CSVs com **milhões de linhas**. O histórico desde 2013 pode somar **dezenas/centenas de GB**.

Isso torna inviável (ou extremamente frágil) trabalhar manualmente com ferramentas como Excel/Google Sheets:
- limite de linhas
- consumo de memória
- lentidão/instabilidade
- dificuldade de repetir o processo (reprodutibilidade)

A proposta aqui é tratar como **Big Data**: baixar, ingerir e refinar em um formato analítico (Delta Lake), com um pipeline reexecutável e verificável.

## Fontes (Portal da Transparência)

- Bolsa Família (pagamentos):  
  https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/
- Auxílio Brasil:  
  https://portaldatransparencia.gov.br/download-de-dados/auxilio-brasil/
- Novo Bolsa Família:  
  https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/

---

## Como os dados fluem (visão geral)

1. **Download** de ZIPs mensais (dados brutos)
2. **Bronze (Delta)**: ingestão “raw/normalizada”
3. **Silver (Delta)**: agregações e tabelas intermediárias (UF/município/ano/mês etc.)
4. **Gold (Delta)**: dataset final (métricas prontas para consumo)
5. **Export Web (JSON)**: gera um arquivo compacto para o navegador
6. **Web App**: consome o JSON via HTTP (app estático)

> Importante: o web app **não lê Delta diretamente**. Ele consome um JSON exportado do Gold.

---

## Versão online (GitHub Pages)

O web app está rodando em:
- https://leonardochalhoub.github.io/getPBFData/

> Observação: a URL raiz redireciona para o app em `./app/web/`.

## Estrutura do projeto

- `app/scripts/` — scripts utilitários (download dos ZIPs, export web, verificações)
- `app/src/bronze/` — ingestão de ZIP/CSV no **Bronze** (Delta)
- `app/src/silver/` — transformações para **Silver** (tabelas agregadas)
- `app/src/gold/` — dataset final **Gold** (pronto para consumo)
- `lakehouse/` — saída local do lakehouse (Delta tables: bronze/silver/gold)
- `data/` — *inputs* locais (ex.: `data/source_zips/`)
- `exports/web/` — export para o front-end (`gold_pbf_estados_df_geo.json`)
- `app/web/` — web app local (Plotly + mapa por UF)

No front-end, o arquivo usado em runtime é:
- `app/web/data/gold_pbf_estados_df_geo.json`

Ele é uma **cópia/snapshot** do export gerado em `exports/web/`.

---

# Como rodar (Passo 1..5)

Abaixo um fluxo típico do zero, seguindo a lógica “Passo 1..4” do README legado (da época do R), atualizado para Python + Delta.

## Pré-requisitos (alto nível)

- Python 3.10+ (recomendado)
- Java 11+ (necessário para Spark)
- Dependências Python: `pyspark`, `delta-spark`, `requests`, `tqdm`, etc.

Os comandos abaixo assumem que você já tem um ambiente Python configurado e que vai rodar com `PYTHONPATH=.` quando necessário.

---

## Passo 1 — Baixar os ZIPs (dados brutos)

Baixa ZIPs mensais e salva em `data/source_zips/`.  
O downloader pula arquivos já existentes e valida se o conteúdo baixado é realmente um ZIP (o Portal às vezes retorna HTML com HTTP 200).

```bash
python app/scripts/download_source_zips.py \
  --out-dir data/source_zips \
  --pbf-years 2013-2021 \
  --aux-years 2021-2023 \
  --nbf-years 2023-2026 \
  --months 1-12 \
  --workers 4
```

Dica: para baixar só um programa, use `--only pbf` / `--only aux` / `--only nbf`.

---

## Passo 2 — Bronze (Delta Lake)

A camada **Bronze** guarda os dados “raw/normalizados” em uma tabela Delta.

CLI em `app/src/shared/cli.py`:

```bash
PYTHONPATH=. python -m app.src.shared.cli ingest-bronze \
  --source-zips-dir data/source_zips \
  --lakehouse-root lakehouse \
  --batch-size 2
```

Parâmetros úteis:
- `--only-origin PBF,AUX,NBF` para limitar origens
- `--min-competencia YYYYMM` para começar de um mês específico (ex.: `202301`)
- `--shuffle-partitions 64` / `--master local[*]` para tuning local

---

## Passo 3 — Silver (agregações e tabelas intermediárias)

Nesta etapa são construídas tabelas com agregações (por ano/mês e por UF/município), enriquecimentos e padronizações.

Módulos principais:
- `app/src/silver/total_ano_mes_estados.py`
- `app/src/silver/total_ano_mes_municipios.py`
- `app/src/silver/populacao_uf_ano.py`
- utilitários em `app/src/silver/common.py`

> Observação: nesta base, Silver/Gold podem estar implementados como módulos importáveis e/ou scripts.  
> Se você já tem um “entrypoint” específico para gerar Silver/Gold, use-o aqui.

Exemplo ilustrativo (ajuste conforme o entrypoint real do seu projeto):

```bash
PYTHONPATH=. python -c "from app.src.silver.total_ano_mes_estados import main; main()"
```

---

## Passo 4 — Gold (dataset final)

O dataset Gold é a base final para consumo analítico e para export ao web.

Módulo principal:
- `app/src/gold/pbf_estados_df_geo.py`

Exemplo ilustrativo (ajuste conforme o entrypoint real do seu projeto):

```bash
PYTHONPATH=. python -c "from app.src.gold.pbf_estados_df_geo import main; main()"
```

---

## Passo 5 — Exportar Gold para Web (JSON compacto)

Este script lê a tabela Delta Gold e gera um JSON “flat” em `exports/web/`:

```bash
PYTHONPATH=. python app/scripts/export_gold_for_web.py
```

Saída:
- `exports/web/gold_pbf_estados_df_geo.json`

Para o web app consumir, copie (ou faça symlink) para `app/web/data/`:

```bash
cp exports/web/gold_pbf_estados_df_geo.json app/web/data/gold_pbf_estados_df_geo.json
```

---

# Rodar o Web App local (HTML/CSS/JS)

O app fica em `app/web/` e usa Plotly para:
- barras: total Brasil por ano
- mapa: distribuição por UF
- alternância Light/Dark
- downloads (Excel e imagens)

Inicie um servidor local:

```bash
python -m http.server 8000 --directory app/web
```

Acesse:
- http://127.0.0.1:8000/

---

# Rodar via container (Web-only)

## Usar imagem publicada no Docker Hub (recomendado)

Imagem:
- `leochalhoub/getpbfdata-web:latest`

Página no Docker Hub:
- https://hub.docker.com/r/leochalhoub/getpbfdata-web/

Rodar:
```bash
docker run --rm -p 8080:80 leochalhoub/getpbfdata-web:latest
```

Abra:
- http://127.0.0.1:8080/

> A imagem é publicada via GitHub Actions. Para habilitar o push automático, configure os secrets no GitHub:
> - `DOCKERHUB_USERNAME` = `leochalhoub`
> - `DOCKERHUB_TOKEN` = Docker Hub access token (Settings → Security → New Access Token)

## Build local (alternativa)

O web app é **estático** e pode ser servido via **nginx**. Já existe um Dockerfile em `app/web/Dockerfile` que empacota:

- `index.html`, `styles.css`, `app.js`, vendors
- `app/web/data/*` (JSON exportado do gold + geojson)
- `brazil-flag.svg`

## Build

Na raiz do repo:

```bash
docker build -t getpbfdata-web -f app/web/Dockerfile app/web
```

## Run

```bash
docker run --rm -p 8080:80 getpbfdata-web
```

Abra:
- http://127.0.0.1:8080/

> Nota: este container **não roda o pipeline Spark/Delta**. Ele só serve o snapshot JSON já exportado.
> Para atualizar os dados, rode o pipeline e reexporte o JSON, depois refaça o build da imagem (ou monte `app/web/data` como volume).

# Rodar via container (Pipeline + Web) — previsto

O caminho “completo” (mais pesado, porém reprodutível) é ter um container/compose que:
- rode o pipeline (bronze/silver/gold) apontando para volumes com `lakehouse/` e `data/`
- gere `exports/web/gold_pbf_estados_df_geo.json`
- copie para `app/web/data/` e sirva o web

> Se você estiver vendo este README no futuro, procure por um `docker-compose.yml` ou uma imagem publicada (ex.: GHCR) que encapsule esses passos.

---

# Observações e validação

- O pipeline usa Spark local com Delta. Em máquinas com pouca RAM, pode ser necessário ajustar memória/partições.
- Alguns meses/arquivos podem não existir no Portal (o downloader marca como `[MISS]`).
- Scripts auxiliares em `app/scripts/`:
  - `verify_idempotency.py`
  - `verify_gold_population_percapita.py`
  - `gold_metric_nonnull_by_year.py`
  - etc.

---

# Autor

Leonardo Chalhoub  
E-mail: <leochalhoub@hotmail.com>  
GitHub: https://github.com/leonardochalhoub/getPBFData
