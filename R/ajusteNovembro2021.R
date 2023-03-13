#' @export
#
ajusteNov2021 <- function() {
  # Apenas no último arquivo da Bolsa Família, mês 11 de 2021,
  # os nomes das duas primeiras colunas estão invertidos, o que causa erro no algoritmo depois.
  # Ajuste 'manual' no código, melhor do que fazer na mão no arquivo csv

  pastaCSV <- paste0(pasta, '/data')

  ajuste_nov_2021 <-  readr::read_csv2(file = paste0(pastaCSV, '/202111_BolsaFamilia_Pagamentos.csv'),
                                       col_names = TRUE,
                                       locale = readr::locale(encoding="latin1"),
                                       progress = readr::show_progress()) %>%
    dplyr::rename('MÊS COMPETÊNCIA' = `MÊS REFERÊNCIA`, 'MÊS REFERÊNCIA' = `MÊS COMPETÊNCIA`)
  print('Iniciou escrita: utils::write.csv2(...')

  utils::write.csv2(ajuste_nov_2021,
                    file = paste0(pastaCSV, '/202111_BolsaFamilia_Pagamentos.csv'),
                    row.names = FALSE,
                    fileEncoding = 'latin1')

  print('Arquivo Bolsa Família Novembro 2011 Ajustado!')
}
