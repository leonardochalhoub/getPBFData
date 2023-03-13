#' @export
downloadData <-  function(pastaOrigem, pastaCSV, pastaOutputs) {

  listOfPackages <- c("plyr","priceR", "janitor",
                      "dplyr", "ggplot2", "stringr",
                      "geobr")

  for(package in listOfPackages){
    if(!require(package, character.only = TRUE)){
      install.packages(package, dependencies = TRUE)
    }
    library(package, character.only = TRUE)
  }

  dir.create(file.path(pastaOrigem))
  dir.create(file.path(pastaCSV))
  dir.create(file.path(pastaOutputs))

  listaZipFiles <- list.files(path = pastaOrigem, pattern = "*.zip", full.names = TRUE)

  listaCSV <- list.files(path = pastaCSV, pattern = "*.csv", full.names = TRUE)

  # Download dos dados do Pagamento do Bolsa Família, disponíveis de 01/2013 a 11/2021

  lista_meses = c(
    "01", "02", "03", "04",
    "05", "06", "07", "08",
    "09", "10", "11", "12"
  )

  lista_anos_PBF = c(
    "2013", "2014", "2015",
    "2016", "2017", "2018",
    "2019", "2020", "2021"
  )

  lista_anos_Aux_Br = c(
    "2021", "2022"
  )

  # O Bolsa Família existe desde 2003, mas só temos dados desde 2013 na fonte utilizada.
  # "2003", "2004", "2005", "2006", "2007", "2008",
  # "2009", "2010", "2011", "2012",

  for (i in lista_anos_PBF) {
    for (j in lista_meses) {
      base_url <- paste0(
        "http://www.portaltransparencia.gov.br/",
        "download-de-dados/bolsa-familia-pagamentos/"
      )
      full_url <- paste0(base_url, i, j)
      options(timeout=200)
      destfile_pre =  paste0('source_data/PBF_', i, '_', j, '.zip')
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

  # Download dados Auxílio Brasil 11/2021 até 11/2022

  for (i in lista_anos_Aux_Br) {
    for (j in lista_meses) {
      base_url <- paste0(
        "https://portaldatransparencia.gov.br/download-de-dados/",
        "auxilio-brasil/"
      )
      full_url <- paste0(base_url, i, j)
      options(timeout=200)
      destfile_pre =  paste0('source_data/Aux_Br_', i, '_', j, '.zip')
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
    plyr::ldply(.data = listaZipFiles, .fun = unzip, exdir = pastaCSV)
  } else {
    print(sprintf("Já existem %s arquivos CSV na pasta.", length(listaCSV)))
  }
}
