#' @export

dataPrep <- function(pasta) {

  listOfPackages <- c("janitor",
                      "dplyr", "stringr")

  for(package in listOfPackages){
    if(!require(package, character.only = TRUE)){
      install.packages(package, dependencies = TRUE)
    }
    library(package, character.only = TRUE)
  }

  pastaCSV <- paste0(pasta, '/data')
  pastaOutputs <- paste0(pasta, '/outputs')

  dir.create(file.path(pastaOutputs))

  listaCSV <- list.files(path = pastaCSV, pattern = "*.csv", full.names = TRUE)

  valor_estado = data.frame()
  valor_municipio = data.frame()

  j <- 1

  for (i in listaCSV) {
    cli::cli_h1(paste0('Iniciando leitura do arquivo %s', i, ' . Arquivo ', j, '/', length(listaCSV)))
    df <- try(
      readr::read_csv2(file = i,
                       col_names = TRUE,
                       locale = readr::locale(encoding="latin1"),
                       progress = readr::show_progress(),
                       col_select = c(1, 3, 5, 8, 9)) |>
        janitor::clean_names()
    )
    cli::cli_h2(paste0('Agregando por Estado arquivo ', j,'/', length(listaCSV), '...'))
    try(
      df_estado <- df |>
        dplyr::group_by(mes_competencia, uf) |>
        dplyr::summarize(n = dplyr::n_distinct(nome_favorecido),
                         total_estado = sum(valor_parcela, na.rm = TRUE)))
    try(valor_estado <- rbind(valor_estado, df_estado))
    try(rm(df_estado))
    cli::cli_h2(paste0('Agregando por Município arquivo ', j,'/', length(listaCSV), '...'))
    try(
      total_municipio <- df |>
        dplyr::group_by(mes_competencia, nome_municipio, uf) |>
        dplyr::summarize(n = dplyr::n_distinct(nome_favorecido),
                         total_municipio = sum(valor_parcela, na.rm = TRUE)))
    try(valor_municipio <- rbind(valor_municipio, total_municipio))
    try(rm(df, total_municipio))
    j <- j + 1
  }

  valor_estado$Ano <- stringr::str_sub(valor_estado$mes_competencia, start= 1, end = 4)
  valor_estado$Mes <- stringr::str_sub(valor_estado$mes_competencia, start= 5, end = 6)

  valor_estado <- valor_estado |>
    ungroup() |>
    dplyr::select(5, 6, 2, 3, 4) |>
    dplyr::arrange(uf, valor_estado, Ano, Mes)

  saveRDS(valor_estado, file = paste0(pastaOutputs, "/total_ano_mes_estados.rds"))

  cli::cli_alert_success("Agregado por Estado, Ano e Mês: OK")

  valor_municipio$Ano <- stringr::str_sub(valor_municipio$mes_competencia, start= 1, end = 4)
  valor_municipio$Mes <- stringr::str_sub(valor_municipio$mes_competencia, start= 5, end = 6)

  valor_municipio <- valor_municipio |>
    ungroup() |>
    dplyr::select(6, 7, 3, 2, 4, 5) |>
    dplyr::arrange(uf, nome_municipio, Ano, Mes)

  saveRDS(valor_municipio, file = paste0(pastaOutputs, "/total_ano_mes_municipio.rds"))
  cli::cli_alert_success("Agregado por Muncípio, Estado, Ano e Mês: OK")

  cli::cli_alert_success("2 Arquivos Armazenados em /outputs no formato RDS")

}
