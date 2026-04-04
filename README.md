# getPBFData (Python + Delta Lake + Web)

Este repositório disponibiliza um pipeline em **Python** para baixar, ingestir e refinar dados públicos relativos aos pagamentos do **Programa Bolsa Família (PBF)**, **Auxílio Brasil** e **Novo Bolsa Família (NBF)**, com foco em **agregações** (por UF/município/ano/mês) e exportação de um *dataset* “gold” para um **web app local (HTML/CSS/JavaScript + Plotly)**.

Os dados são públicos (Portal da Transparência/CGU), porém o formato/volume torna difícil trabalhar manualmente:

- os arquivos são disponibilizados como **ZIPs mensais** contendo CSVs com milhões de linhas
- o histórico completo desde 2013 pode somar **dezenas/centenas de GB**
- o pipeline abaixo automatiza download e processamento em camadas (bronze/silver/gold) para facilitar análises

Fontes (Portal da Transparência):

- Bolsa Família (pagamentos):  
  https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/
- Auxílio Brasil:  
  https://portaldatransparencia.gov.br/download-de-dados/auxilio-brasil/
- Novo Bolsa Família:  
  https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/

## Estrutura do projeto

- `app/scripts/` — scripts utilitários (download dos ZIPs, export web, verificações)
- `app/src/bronze/` — ingestão de ZIP/CSV no **Bronze** (Delta)
- `app/src/silver/` — transformações para **Silver** (tabelas agregadas)
- `app/src/gold/` — dataset final **Gold** (pronto para consumo)
- `lakehouse/` — saída local do lakehouse (Delta tables: bronze/silver/gold)
- `data/` — *inputs* locais (ex.: `data/source_zips/`)
- `exports/web/` — export para o front-end (`gold_pbf_estados_df_geo.json`)
- `app/web/` — web app local (Plotly + mapa por UF)

> Observação: o front-end usa o arquivo JSON gerado em `exports/web/` (ou a cópia versionada em `app/web/data/`, dependendo do seu fluxo).

---

# Como rodar. Passos (download → bronze → silver → gold → web)

Abaixo um fluxo típico do zero, seguindo a ideia do README legado (Passo 1..4), adaptado para Python + Delta.

## Pré-requisitos (alto nível)

- Python 3.10+ (recomendado)
- Java 11+ (necessário para Spark)
- Dependências Python: `pyspark`, `delta-spark`, `requests`, `tqdm`, etc.

Como este repositório pode ser executado de diferentes formas (venv/conda/poetry), os comandos abaixo assumem que você já tem o ambiente Python configurado e que vai rodar com `PYTHONPATH=.` quando necessário.

---

## Passo 1 — Baixar os ZIPs (dados brutos)

O script abaixo baixa os ZIPs mensais e salva em `data/source_zips/`.  
Ele pula arquivos já existentes e valida se o conteúdo baixado é realmente um ZIP (o Portal às vezes retorna HTML mesmo com HTTP 200).

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

## Passo 2 — Ingestão Bronze (Delta Lake)

A camada **Bronze** guarda os dados “raw”/normalizados em uma tabela Delta.  
O comando abaixo lê os ZIPs em `data/source_zips/` e escreve em `lakehouse/bronze/payments`.

O CLI está em `app/src/shared/cli.py`:

```bash
PYTHONPATH=. python -m app.src.shared.cli ingest-bronze \
  --source-zips-dir data/source_zips \
  --lakehouse-root lakehouse \
  --batch-size 12
```

Parâmetros úteis:

- `--only-origin PBF,AUX,NBF` para limitar origens
- `--min-competencia YYYYMM` para começar de um mês específico (ex.: `202301`)
- `--shuffle-partitions 64` / `--master local[*]` para tuning local

---

## Passo 3 — Silver (agregações e tabelas intermediárias)

Nesta etapa são construídas tabelas com agregações (por ano/mês e por UF/município), enriquecimentos e padronizações.

Os módulos principais estão em:

- `app/src/silver/total_ano_mes_estados.py`
- `app/src/silver/total_ano_mes_municipios.py`
- `app/src/silver/populacao_uf_ano.py`
- utilitários em `app/src/silver/common.py`

> Nesta base, os scripts de Silver/Gold podem estar implementados como módulos importáveis e/ou scripts a serem chamados.  
> Se você já tem um “entrypoint” específico para gerar Silver/Gold, use-o aqui.

Exemplo (caso você esteja rodando como módulo Python — ajuste para o entrypoint existente no seu projeto):

```bash
# Exemplo ilustrativo (ajuste conforme o entrypoint real do Silver no seu projeto)
PYTHONPATH=. python -c "from app.src.silver.total_ano_mes_estados import main; main()"
```

Se preferir, você pode criar um script `app/scripts/run_silver.py` para orquestrar a execução dos módulos Silver em sequência.

---

## Passo 4 — Gold (dataset final)

O dataset Gold é o que o front-end consome (ex.: `pbf_estados_df_geo`).

O gerador principal está em:

- `app/src/gold/pbf_estados_df_geo.py`

Exemplo (ajuste conforme o entrypoint real do seu projeto):

```bash
# Exemplo ilustrativo (ajuste conforme o entrypoint real do Gold no seu projeto)
PYTHONPATH=. python -c "from app.src.gold.pbf_estados_df_geo import main; main()"
```

---

## Passo 5 — Exportar Gold para o Web App (JSON compacto)

Este script lê a tabela Delta Gold e gera um JSON “flat” em `exports/web/`:

```bash
PYTHONPATH=. python app/scripts/export_gold_for_web.py
```

Saída:
- `exports/web/gold_pbf_estados_df_geo.json`

Se você quiser visualizar no web app sem mexer no código, copie (ou faça symlink) para `app/web/data/`:

```bash
cp exports/web/gold_pbf_estados_df_geo.json app/web/data/gold_pbf_estados_df_geo.json
```

---

# Rodar o Web App local (HTML/CSS/JS)

O app fica em `app/web/` e usa Plotly para:

- barras: total Brasil por ano
- mapa: distribuição por UF
- alternância Light/Dark

Inicie um servidor local:

```bash
python -m http.server 8000 --directory app/web
```

Acesse:
- http://127.0.0.1:8000/

---

# Observações e validação

- O pipeline usa Spark local com Delta. Em máquinas com pouca RAM, pode ser necessário ajustar memória/partições.
- Alguns meses/arquivos podem não existir no Portal (o downloader marca como `[MISS]`).
- O repositório contém scripts auxiliares em `app/scripts/` para checagens e comparações:
  - `verify_idempotency.py`
  - `verify_gold_population_percapita.py`
  - `gold_metric_nonnull_by_year.py`
  - etc.

---

# Autor

Leonardo Chalhoub  
E-mail: <leochalhoub@hotmail.com>  
GitHub: https://github.com/leonardochalhoub/getPBFData
