#' @export

downloadData_Passo1 <-  function(pasta) {

  listOfPackages <- c("plyr")

  for(package in listOfPackages){
    if(!require(package, character.only = TRUE)){
      install.packages(package, dependencies = TRUE)
    }
    library(package, character.only = TRUE)
  }

  pastaOrigem <-  paste0(pasta, '/source_data')
  pastaCSV <- paste0(pasta, '/data')
  pastaArquivosAuxiliares <- paste0(pasta, '/arquivos_aux')

  dir.create(file.path(pasta))
  dir.create(file.path(pastaOrigem))
  dir.create(file.path(pastaCSV))
  dir.create(file.path(pastaArquivosAuxiliares))

  # download arquivo populacao do github do pacote

  populacao_git_url <- 'https://github.com/leonardochalhoub/getPBFData/blob/master/arquivos_aux/populacao.xlsx'
  pop_pre =  paste0(pastaArquivosAuxiliares, '/populacao.xlsx')
  if (file.exists(pop_pre)) {
    return
  } else {
    try(
      download.file(
        url      = populacao_git_url,
        destfile =  pop_pre,
        mode     = "wb"))
  }

  listaZipFiles <- list.files(path = pastaOrigem, pattern = "*.zip", full.names = TRUE)

  listaCSV <- list.files(path = pastaCSV, pattern = "*.csv", full.names = TRUE)

  # Download dos dados do Pagamento do Bolsa Família (PBF), disponíveis de 01/2013 a 11/2021

  lista_meses = c(
    "01", "02", "03", "04",
    "05", "06", "07", "08",
    "09", "10", "11", "12"
  )

  lista_anos_PBF = c(
    "2013", "2014", "2015",
    "2016", "2017", "2018",
    "2019", "2020", "2021",
    "2023"
  )

  lista_anos_Aux_Br = c("2021", "2022", "2023")

  # O Bolsa Família existe desde 2003, mas só temos dados desde Janeiro de 2013 a Novembro de 2021.
  # Apesar de ter sido substituído pelo Auxílio Brasil em 2021, temos a notícia de que, em 2023, o Bolsa Família retornou oficialmente.

  for (i in lista_anos_PBF) {
    for (j in lista_meses) {
      base_url <- paste0(
        "http://www.portaltransparencia.gov.br/",
        "download-de-dados/bolsa-familia-pagamentos/"
      )
      full_url <- paste0(base_url, i, j)
      options(timeout=200)
      destfile_pre =  paste0(pastaOrigem, '/PBF_', i, '_', j, '.zip')
      if (file.exists(destfile_pre)) {
        return
      } else {
        try(
          download.file(
            url      = full_url,
            destfile =  destfile_pre,
            mode     = "wb"
          )
        )
      }
    }
  }

  # Download dados Auxílio Brasil de Novembro de 2021 até Novembro de 2022.

  for (i in lista_anos_Aux_Br) {
    for (j in lista_meses) {
      base_url <- paste0(
        "https://portaldatransparencia.gov.br/download-de-dados/",
        "auxilio-brasil/"
      )
      full_url <- paste0(base_url, i, j)
      options(timeout=200)
      destfile_pre =  paste0(pastaOrigem, '/Aux_Br_', i, '_', j, '.zip')
      if (file.exists(destfile_pre)) {
        return
      } else {
        try(
          download.file(
            url      = full_url,
            destfile =  destfile_pre,
            mode     = "wb"
          )
        )
      }
    }
  }

  if (length(listaCSV) == 0) {
    cli::cli_h2(paste0('Início da descompressão dos arquivos.'))
    cli::cli_h2(paste0('Essa etapa pode demorar mais de uma hora...'))

    plyr::ldply(.data = listaZipFiles, .fun = unzip, exdir = pastaCSV)

    cli::cli_alert_success("Descompressão 100% completa com sucesso!")
    cli::cli_alert_success("Todos os arquivos CSV estão na pasta /data.")
  } else {
    print(sprintf("Já existem %s arquivos CSV na pasta.", length(listaCSV)))
  }
}
