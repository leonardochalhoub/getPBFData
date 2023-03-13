#' @export

postProc_Passo3 <- function(pasta) {

  listOfPackages <- c("dplyr", "priceR", "geobr")

  for(package in listOfPackages){
    if(!require(package, character.only = TRUE)){
      install.packages(package, dependencies = TRUE)
    }
    library(package, character.only = TRUE)
  }
  populacao_estados <- readxl::read_xlsx('https://github.com/leonardochalhoub/getPBFData/raw/master/arquivos_aux/populacao.xlsx',
                                         sheet = 'Estados') |>
    tidyr::pivot_longer(cols = 3:12,
                        names_to = 'Ano',
                        values_to = 'populacao')
  pbf_estados_df <-
    readRDS('outputs/total_ano_mes_estados.rds') |>
    dplyr::group_by(Ano, uf) |>
    dplyr::summarise(n_benef = mean(n, na.rm = TRUE),
                     valor = sum(total_estado, na.rm = TRUE) / 1e9) |>
    dplyr::left_join(populacao_estados |>
                       dplyr::select(1, 3, 4),
                     by = c('Ano', 'uf'))

  pbf_estados_df$inflac2021 <-
    priceR::adjust_for_inflation(pbf_estados_df$valor, pbf_estados_df$Ano, "BR", to_date = 2021)

  pbf_estados_df <- pbf_estados_df |>
    dplyr::mutate(
      pbfPerBenef = (inflac2021 * 1e9) / n_benef,
      pbfPerCapita = (inflac2021 * 1e9) / populacao
    )

  saveRDS(pbf_estados_df, 'outputs/pbf_estados_df.rds')

  if (!exists("states")) {
    states <- geobr::read_state(year = 2019)
  } else {
    return
  }

  estadosAgregados_df <- pbf_estados_df |>
    dplyr::group_by(uf) |>
    dplyr::summarise(
      n_benef = round(mean(n_benef, na.rm = TRUE), 0),
      valor = round(sum(valor, na.rm = TRUE), 2),
      inflac2021 = round(sum(inflac2021, na.rm = TRUE), 2),
      populacao = round(mean(populacao, na.rm = TRUE), 0),
      pbfPerBenef = round(inflac2021 * 1e9 / n_benef, 2),
      pbfPerCapita = round(inflac2021 * 1e9 / populacao, 2),
      Ano = 'Agregado 2013-2022') |>
    dplyr::select(8, 1, 2, 3, 5, 4, 6, 7)

  pbf_estados_df <- pbf_estados_df |>
    rbind(estadosAgregados_df) |>
    dplyr::left_join(states |>
                       dplyr::select(2, 5, 6),
                     by = c('uf' = 'abbrev_state'))

  rm(estadosAgregados_df)

  saveRDS(pbf_estados_df, 'outputs/pbf_estados_df_geo.rds')

  populacao_municipios <-
    readxl::read_xlsx('https://github.com/leonardochalhoub/getPBFData/raw/master/arquivos_aux/populacao.xlsx',
                      sheet = 'Municipios') |>
    tidyr::pivot_longer(cols = 3:12,
                        names_to = 'Ano',
                        values_to = 'populacao')

  populacao_municipios$nome_municipio <-
    stringr::str_to_upper(populacao_municipios$nome_municipio)

  populacao_municipios$nome_municipio <-
    iconv(populacao_municipios$nome_municipio, to = 'ASCII//TRANSLIT')

  pbf_municipios_df <-
    readRDS('outputs/total_ano_mes_municipio.rds') |>
    dplyr::group_by(Ano, uf, nome_municipio) |>
    dplyr::summarise(n_benef = mean(n, na.rm = TRUE),
                     valor = sum(total_municipio, na.rm = TRUE)) |> # / 1e9
    dplyr::left_join(populacao_municipios,
                     by = c('Ano', 'uf', 'nome_municipio')) |>
    dplyr::arrange(uf, nome_municipio, Ano)

  pbf_municipios_df$inflac2021 <-
    priceR::adjust_for_inflation(pbf_municipios_df$valor,
                                 pbf_municipios_df$Ano,
                                 "BR",
                                 to_date = 2021)

  pbf_municipios_df <- pbf_municipios_df |>
    dplyr::mutate(
      pbfPerBenef = (inflac2021) / n_benef,
      pbfPerCapita = (inflac2021) / populacao
    )

  saveRDS(pbf_municipios_df, 'outputs/pbf_municipios.rds')

  if (!exists("cities")) {
    cities <- geobr::read_municipality(code_muni = "all", year = 2019)
    cities$name_muni <- stringr::str_to_upper(cities$name_muni)
    cities$name_muni <- iconv(cities$name_muni, to = 'ASCII//TRANSLIT')
  } else {
    return
  }

  municipiosAgregados_df <- pbf_municipios_df |>
    dplyr::group_by(uf, nome_municipio) |>
    dplyr::summarise(
      n_benef = round(mean(n_benef, na.rm = TRUE), 0),
      valor = round(sum(valor, na.rm = TRUE), 2),
      inflac2021 = round(sum(inflac2021, na.rm = TRUE), 2),
      populacao = mean(populacao, na.rm = TRUE),
      pbfPerBenef = round(inflac2021 / n_benef, 2),
      pbfPerCapita = round(inflac2021 / populacao, 2),
      Ano = 'Agregado 2013-2022'
    ) |>
    dplyr::select(9, 1, 2, 3, 4, 6, 5, 7, 8)

  pbf_municipios_df <- pbf_municipios_df |>
    rbind(municipiosAgregados_df) |>
    dplyr::mutate(nome_uf = stringr::str_c(nome_municipio, ' / ', uf)) |>
    dplyr::left_join(
      cities |>
        dplyr::select(2, 4, 7, 8),
      by = c('uf' = 'abbrev_state', 'nome_municipio' = 'name_muni')
    )

  rm(municipiosAgregados_df)

  saveRDS(pbf_municipios_df, 'outputs/pbf_municipios_geo.rds')
}
