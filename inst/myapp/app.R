library(sf) # Se não carregar a sf, a interpretação da coluna geom não funciona e atrapalha todo o resto.

remotes::install_github("jeroen/jsonlite", force = TRUE, upgrade = c("never"))
remotes::install_github("ramnathv/htmlwidgets", force = TRUE, upgrade = c("never"))
remotes::install_github("rstudio/DT", force = TRUE, upgrade = c("never"))

listOfPackages <- c("viridis", "forcats",
                    "writexl", "devtools")

for(package in listOfPackages){
  if(!require(package, character.only = TRUE)){
    install.packages(package, dependencies = TRUE)
  }
  library(package, character.only = TRUE)
}
remove.packages("DT")
remove.packages("htmlwidgets")
remove.packages("jsonlite")


OFFSET <- 2
HEIGHT <- 300
WIDTH <- 400
DPI <- 800
HEIGHT_FILE <- 7.75
WIDTH_FILE <- 9
MESSAGE_DURATION <- 20

pasta <- folder

pastaOutputs <- paste0(pasta, '/outputs')

# -- Estados
pbf_estados_df <- readRDS(paste0(pastaOutputs, '/pbf_estados_df_geo.rds'))

# -- Municipios

pbf_municipios_df <- readRDS(paste0(pastaOutputs, '/pbf_municipios_geo.rds'))

ui <- shiny::fluidPage(
  tags$head(
    tags$link(
      rel = "icon",
      type = "image/png",
      sizes = "32x32",
      href = "https://cdn1.iconfinder.com/data/icons/flags-circle-3d/100/Brazil-1024.png"
    )
  ),
  # shinythemes::themeSelector(),
  theme = bslib::bs_theme(bootswatch = "flatly"),

  shiny::titlePanel("Dados Bolsa Família e Auxílio Brasil Jan/2013 a Fev/2023"),
  shiny::sidebarLayout(
    shiny::sidebarPanel(
      width = 2,
      shiny::selectInput(
        "selectPerCapitaColours",
        "Escala Cores 'per Capita'",
        choices = c("A", "B", "C", "D",
                    "E", "F", "G", "H"),
        selected = "F",
        multiple = FALSE
      ),
      shiny::selectInput(
        "selectPorBenefColours",
        "Escala Cores 'por Beneficiário'",
        choices = c("A", "B", "C", "D",
                    "E", "F", "G", "H"),
        selected = "E",
        multiple = FALSE
      ),
      shiny::selectInput(
        "selectColors_Valores_Absolutos",
        "Escala Cores 'Valores Absolutos'",
        choices = c("A", "B", "C", "D",
                    "E", "F", "G", "H"),
        selected = "G",
        multiple = FALSE
      ),
      shiny::selectInput(
        "selectAno",
        "Ano ou Agregado",
        choices = unique(pbf_estados_df$Ano),
        selected = "Agregado 2013-2023",
        multiple = FALSE
      ),
      shiny::downloadButton("downloadDataAnual", "Dados em Excel"),
      shiny::downloadButton(
        "downloadImage",
        "Imagens em Alta Resolução",
        icon = shiny::icon('fa-solid fa-image')
      )
    ),
    shiny::mainPanel(
      shiny::tabsetPanel(
        id = 'tabset',
        shiny::tabPanel(
          "Agregado Anos",
          shiny::fluidRow(
            shiny::column(12,
                          DT::dataTableOutput("tabelaTotalAnual")),
            shiny::br(),
            shiny::fluidRow(
              shiny::column(2,
                            shiny::plotOutput("colunasPbfPerCapitaAnual")),
              shiny::column(2,
                            offset = OFFSET,
                            shiny::plotOutput("colunasPbfPerBenefAnual")),
              shiny::column(2,
                            offset = OFFSET,
                            shiny::plotOutput("colunasTotalAnual"))
            )
          )
        ),
        shiny::tabPanel(
          "Estado/Ano",
          shiny::fluidRow(shiny::column(12,
                                        DT::dataTableOutput("tabelaTotalEstado")
          )),
          shiny::br(),
          shiny::fluidRow(
            shiny::column(2,
                          shiny::plotOutput("colPbfPerCapitaEstadual")),
            shiny::column(2,
                          offset = OFFSET,
                          shiny::plotOutput("colunasAnoEstadoPbfPerBenef")),
            shiny::column(2,
                          offset = OFFSET,
                          shiny::plotOutput("colunasTotalEstadoPorAno"))
          ),
          shiny::fluidRow(
            shiny::column(2,
                          shiny::plotOutput("mapaPbfPerCapita")),
            shiny::column(2,
                          offset = OFFSET,
                          shiny::plotOutput("mapaPbfPorBenef")),
            shiny::column(2,
                          offset = OFFSET,
                          shiny::plotOutput("mapaTotalEstado"))
          ),
        ),
        shiny::tabPanel(
          "Município/Ano",
          shiny::fluidRow(shiny::column(
            12,
            DT::dataTableOutput("tabelaTotalMunicipio")
          )),
          shiny::br(),
          shiny::fluidRow(
            shiny::column(2,
                          shiny::plotOutput("colPbfPerCapitaMun")),
            shiny::column(2,
                          offset = OFFSET,
                          shiny::plotOutput("colPbfPerBenefMun")),
            shiny::column(2,
                          offset = OFFSET,
                          shiny::plotOutput("colTotalMunicipio"))
          ),
          shiny::br(),
          shiny::br(),
          shiny::br(),
          shiny::br(),
          shiny::fluidRow(
            shiny::column(2,
                          shiny::plotOutput("mapaPbfPerCapitaMun")),
            shiny::column(2,
                          offset = OFFSET,
                          shiny::plotOutput("mapaPbfPerBenefMun")),
            shiny::column(2,
                          offset = OFFSET,
                          shiny::plotOutput("mapaTotalMunicipio"))
          ),
        ),
        shiny::tabPanel("Informações Gerais",
                        shiny::fluidRow(
                          shiny::column(
                            7,
                            shiny::h5('1) Fontes de Dados'),
                            shiny::h6(
                              paste0(
                                'O programa Auxílio Brasil substituiu o Bolsa Família a partir do final de 2021,',
                                'porém seu propósito social era o mesmo. Por essa razão, consideramos formalmente que ',
                                'o Auxílio Brasil é a continuidade do Bolsa Família e juntei as duas séries. Existem ',
                                'dois arquivos com dados para Novembro de 2021, que aqui são somados indiscriminadamente.'
                              )
                            ),
                            shiny::h6(
                              paste0(
                                'Pagamento do Bolsa Família: dados disponíveis em frequência mensal',
                                ' de Janeiro/2013 até Novembro/2021, em arquivos comprimidos em formato ZIP.',
                                ' Cada arquivo possui mais de 14 milhões de linhas. No total todos os arquivos ',
                                'ainda comprimidos totalizam 25 gigas de dados. Ao descomprimi-los, os arquivos ',
                                'em formato csv restantes totalizam 156 gigas. Link:'
                              )
                            ),
                            shiny::h6(
                              'https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/'
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Dados do Auxílio Brasil, disponíveis em frequência mensal de Novembro de 2021 ',
                                'até Fevereiro de 2023, em arquivos exatamente iguais em estrutura aos do Bolsa Família.',
                                'No total, todos os arquivos ainda comprimidos totalizam 4,8 gigas. Ao descomprimi-los, os arquivos ',
                                'em formato csv restantes totalizam ~30 gigas. Link:'
                              )
                            ),
                            shiny::h6(
                              'https://portaldatransparencia.gov.br/download-de-dados/auxilio-brasil/'
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Dados da População de 2013 a 2021, para Estados e Municípios. Como não há dados para 2022, ',
                                'utilizamos os números de 2021 para os cálculos. Link:'
                              )
                            ),
                            shiny::h6(
                              'https://www.ibge.gov.br/estatisticas/sociais/populacao/9103-estimativas-de-populacao.html?edicao=17283&t=downloads'
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Todo esse volume de dados é extraído, transformado e carregado com a linguagem R, e aqui neste site são utilizados ',
                                'apenas dados já agregados previamente. Esses dados processados podem ser baixados diretamente por aqui'
                              )
                            ),
                            shiny::h6(
                              'Link do GitHub com os scripts que desenvolvem todo os downloads e manipulam os dados: a inserir'
                            ),
                            shiny::br(),
                            shiny::h5('2) Detalhes Metodológicos'),
                            shiny::h6(
                              paste0(
                                'Dos dados disponíveis, extraímos os valores mensais pagos a título de Bolsa Família ',
                                'e Auxílio Brasil para cada indivíduo beneficiário. Assim, agregamos estes dados por Estados e Municípios',
                                ', o que nos oferece o valor total por categoria, assim como o número de beneficiários médio.'
                              )
                            ),
                            shiny::h6(
                              paste0(
                                'Todos os valores de 2013 a 2022 foram inflacionados para Reais do ano de 2021 com o pacote priceR, que utilizou ',
                                'a mediana da série anual do IPCA disponível no site do Banco Mundial.'
                              )
                            ),
                            shiny::br(),
                            shiny::h5('3) Citações do R e dos Pacotes Utilizados'),
                            shiny::h6(citation()),
                            shiny::h6('Pacotes:'),
                            shiny::h6(
                              paste0(
                                'Chang W, Cheng J, Allaire J, Sievert C, Schloerke B, Xie Y, Allen J, McPherson J, Dipert A, ',
                                'Borges B (2022). shiny: Web Application Framework for R. R package version 1.7.4, URL:',
                                ' https://CRAN.R-project.org/package=shiny .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Chang W (2021). shinythemes: Themes for Shiny. R package version 1.2.0, ',
                                'URL: https://CRAN.R-project.org/package=shinythemes .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Condylios S (2022). priceR: Economics and Pricing Tools. R package version 0.1.67, ',
                                'URL: https://CRAN.R-project.org/package=priceR .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Garnier, Simon, Ross, Noam, Rudis, Robert, Camargo, Pedro A, Sciaini, Marco, Scherer, ',
                                'Cédric (2021). _viridis - Colorblind-Friendly Color Maps for R_. doi: 10.5281/zenodo.4679424 (URL:',
                                ' https://doi.org/10.5281/zenodo.4679424), R package version 0.6.2, URL: https://sjmgarnier.github.io/viridis/ .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Ooms J (2023). writexl: Export Data Frames to Excel "xlsx" Format. R package ',
                                'version 1.4.2, URL: https://CRAN.R-project.org/package=writexl .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Pereira R, Goncalves C (2022). geobr: Download Official Spatial Data Sets of Brazil.',
                                'R package version 1.7.0, URL: https://CRAN.R-project.org/package=geobr .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Sievert C, Cheng J (2022). bslib: Custom "Bootstrap" "Sass" Themes for "shiny" ',
                                'and "rmarkdown". R package version 0.4.2, URL: https://CRAN.R-project.org/package=bslib .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Sievert C, Schloerke B, Cheng J (2021). thematic: Unified and Automatic "Theming"',
                                'of "ggplot2", "lattice", and "base" R Graphics. R package version 0.1.2.1, URL: ',
                                'https://CRAN.R-project.org/package=thematic .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Wickham H, François R, Henry L, Müller K (2022). dplyr: A Grammar of Data Manipulation. ',
                                'R package version 1.0.10, URL: https://CRAN.R-project.org/package=dplyr .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Wickham H (2016). ggplot2: Elegant Graphics for Data Analysis. Springer-Verlag New York. ',
                                'ISBN 978-3-319-24277-4, URL: https://ggplot2.tidyverse.org .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Wickham H, Bryan J (2022). readxl: Read Excel Files. R package version 1.4.1, ',
                                'URL: https://CRAN.R-project.org/package=readxl .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Wickham H (2022). stringr: Simple, Consistent Wrappers for Common String Operations. ',
                                'R package version 1.5.0, URL: https://CRAN.R-project.org/package=stringr .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Wickham H, Girlich M (2022). tidyr: Tidy Messy Data. R package version 1.2.1, ',
                                'URL: https://CRAN.R-project.org/package=tidyr .'
                              )
                            ),
                            shiny::br(),
                            shiny::h6(
                              paste0(
                                'Xie Y, Cheng J, Tan X (2023). DT: A Wrapper of the JavaScript Library "DataTables". ',
                                'R package version 0.27, URL: https://CRAN.R-project.org/package=DT .'
                              )
                            ),
                            shiny::br(),
                            shiny::div(),
                            shiny::h5('4) Autor'),
                            shiny::h6('Leonardo Chalhoub'),
                            shiny::h6('E-mail: leochalhoub@hotmail.com'),
                            shiny::h6('LinkedIn: https://www.linkedin.com/in/leonardochalhoub/'),
                            shiny::br(),
                            shiny::h6('Última atualização em Abril de 2023.'),
                            shiny::br()
                          )
                        ))
      )
    )
  )
)

server <- function(input, output, session) {
  # thematic::thematic_shiny()

  # Server Tabela e Gráficos da Aba "Agregado Anos"
  dataAnual <- shiny::reactive({
    pbf_estados_df |>
      dplyr::filter(!(Ano == 'Agregado 2013-2023')) |>
      dplyr::group_by(Ano) |>
      dplyr::summarise(
        n_benef = round(sum(n_benef, na.rm = TRUE), 0),
        valor = round(sum(valor, na.rm = TRUE), 2),
        inflac2021 = round(sum(inflac2021, na.rm = TRUE), 2),
        populacao = sum(populacao, na.rm = TRUE),
        pbfPerBenef = round(inflac2021 * 1e9 / n_benef, 2),
        pbfPerCapita = round(inflac2021 * 1e9 / populacao, 2),
        popBenef = round(n_benef / populacao, 4)
      ) |>
      dplyr::arrange(Ano) |>
      dplyr::rename(
        'Nº Médio Beneficiários' = n_benef,
        'Bilhões R$ Nominal' = valor,
        'Bilhões R$ Ajustados 2021' = inflac2021,
        'R$ Ajustados 2021 por Beneficiário' = pbfPerBenef,
        'Soma População Média Estadual IBGE' = populacao,
        'R$ Ajustados 2021 per Capita' = pbfPerCapita,
        'Beneficiários / População' = popBenef
      ) |>
      dplyr::select(1, 2, 5, 8, 3, 4, 6, 7)
  })

  output$tabelaTotalAnual <- DT::renderDataTable(
    DT::datatable(dataAnual(),
                  rownames = FALSE,
                  options = list(iDisplayLength = 25)) |>
      DT::formatPercentage(c('Beneficiários / População'), 1) |>
      DT::formatRound(
        columns = c(
          'R$ Ajustados 2021 por Beneficiário',
          'Bilhões R$ Ajustados 2021',
          'Bilhões R$ Nominal',
          'R$ Ajustados 2021 per Capita'
        ),
        digits = 2,
        mark = ''
      ),
    options = list(
      pageLength = 25,
      autoWidth = TRUE,
      searching = TRUE
    )
  )

  grafBarras_por_Anos <-
    function(data,
             var,
             escalaCores = 'G',
             nomeEscala,
             min,
             max,
             textRound,
             textDigits,
             title,
             subtitle
    ) {
      ggplot2::ggplot(
        {{data}},
        ggplot2::aes(x = Ano, y = {{var}}, fill = {{var}})
      ) +
        ggplot2::geom_col() +
        viridis::scale_fill_viridis(
          name = {{nomeEscala}},
          discrete = F,
          labels = scales::comma_format(
            # big.mark = ",",
            decimal.mark = "."),
          option = escalaCores,
          direction = -1
        ) +
        ggplot2::scale_y_continuous(limits = c({{min}}, {{max}}),
                                    oob = scales::rescale_none) +
        ggplot2::geom_text(ggplot2::aes(label = format(round({{var}}, {{textRound}}), nsmall = {{textDigits}})),
                           vjust = -0.5) +
        ggplot2::labs(
          title = {{title}},
          subtitle = {{subtitle}},
          # caption = "Fonte: www.portaldatransparencia.gov.br",
          x = '',
          y = sprintf('%s 2021', {{nomeEscala}})
        ) +
        ggplot2::theme(
          panel.background = ggplot2::element_blank(),
          panel.grid.major = ggplot2::element_line('black', linewidth = 0.04, linetype = 'dashed')
        )
    }

  output$colunasPbfPerCapitaAnual <-
    shiny::renderPlot(
      grafBarras_por_Anos(
        data = dataAnual(),
        var = `R$ Ajustados 2021 per Capita`,
        nomeEscala = "R$",
        escalaCores = input$selectPerCapitaColours,
        min = 12,
        max = 310,
        textRound = 0,
        textDigits = 0,
        title = 'PBF/Auxílio Brasil Total Anual per Capita',
        subtitle = 'R$ Ajustados 2021'
      ),
      height = HEIGHT,
      width = WIDTH
    )

  output$colunasPbfPerBenefAnual <-
    shiny::renderPlot(
      grafBarras_por_Anos(data = dataAnual() |>
                            dplyr::mutate(
                              `R$ Ajustados 2021 por Beneficiário` = round(`Bilhões R$ Ajustados 2021` * 1e9 / `Nº Médio Beneficiários` / 1000, 2)
                            ),
                          var = `R$ Ajustados 2021 por Beneficiário`,
                          escalaCores = input$selectPorBenefColours,
                          nomeEscala = "KR$",
                          min = 0.17,
                          max = 4.3,
                          textRound = 2,
                          textDigits =  2,
                          title = 'PBF/Auxílio Brasil Total Anual por Beneficiário',
                          subtitle = 'Milhares de R$ Ajustados 2021'
      ),
      height = HEIGHT,
      width = WIDTH
    )

  output$colunasTotalAnual <-
    shiny::renderPlot(
      grafBarras_por_Anos(
        data = dataAnual(),
        var = `Bilhões R$ Ajustados 2021`,
        escalaCores = input$selectColors_Valores_Absolutos,
        nomeEscala = "BR$",
        min = 2.5,
        max = 65,
        textRound = 0,
        textDigits =  0,
        title = 'PBF/Auxílio Brasil Total Anual Valores Absolutos',
        subtitle = 'Bilhões de R$ Ajustados 2021'
      ),
      height = HEIGHT,
      width = WIDTH
    )

  # Tabela e Gráficos da Aba "Estado/Ano"

  dataAnoEstado <- shiny::reactive({
    shiny::showNotification("Preparando dados dos Estados. Isso pode levar até 6 segundos.",
                            duration = 8)

    pbf_estados_df |>
      dplyr::filter(Ano == input$selectAno) |>
      dplyr::arrange(dplyr::desc(inflac2021)) |>
      dplyr::mutate(
        n_benef = round(n_benef, 0),
        valor = round(valor, 2),
        inflac2021 = round(inflac2021, 2),
        pbfPerBenef = round(pbfPerBenef, 2),
        pbfPerCapita = round(pbfPerCapita, 2),
        popBenef = round(n_benef / populacao, 4)
      ) |>
      dplyr::rename(
        'UF' = uf,
        'Região' = name_region,
        'Nº Médio Beneficiários' = n_benef,
        'Bilhões R$ Nominal' = valor,
        'Bilhões R$ Ajustados 2021' = inflac2021,
        'R$ Ajustados 2021 por Beneficiário' = pbfPerBenef,
        'População IBGE' = populacao,
        'R$ Ajustados 2021 per Capita' = pbfPerCapita,
        'Beneficiários / População' = popBenef
      ) |>
      dplyr::select(1, 9, 2, 3, 5, 11, 4, 6, 7, 8)
  })

  output$tabelaTotalEstado <- DT::renderDataTable(
    DT::datatable(dataAnoEstado(),
                  rownames = FALSE) |>
      DT::formatPercentage(c('Beneficiários / População'), 1) |>
      DT::formatRound(
        columns = c(
          'R$ Ajustados 2021 por Beneficiário',
          'Bilhões R$ Ajustados 2021',
          'Bilhões R$ Nominal',
          'R$ Ajustados 2021 per Capita'
        ),
        digits = 2,
        mark = ''
      ),
    options = list(
      pageLength = 10,
      autoWidth = TRUE,
      searching = TRUE
    )
  )

  # Colunas da aba 'Estado / Ano'
  grafBarrasAnoEstado <- function(
    data,
    var,
    min,
    max,
    escalaCores,
    nomeEscala,
    textRound,
    textDigits,
    title,
    subtitle) {

    ggplot2::ggplot(
      data,
      ggplot2::aes(
        x = factor(UF) |>
          forcats::fct_reorder({{var}}, .desc = TRUE),
        y = {{var}},
        fill = {{var}}
      )) +
      ggplot2::geom_col() +

      ggplot2::scale_y_continuous(limits = c(min,
                                             max),
                                  oob = scales::rescale_none) +
      ggplot2::geom_text(ggplot2::aes(label = format(round({{var}}, {{textRound}}), nsmall = {{textDigits}})),
                         vjust = -0.5,
                         size = 2.5) +
      ggplot2::theme(
        panel.background = ggplot2::element_blank(),
        panel.grid.major = ggplot2::element_line('black', linewidth = 0.04, linetype = 'dashed'),
        axis.text.x = ggplot2::element_text(
          angle = 90,
          vjust = 0.5,
          hjust = 1
        )
      ) +
      viridis::scale_fill_viridis(
        name = nomeEscala,
        discrete = F,
        labels = scales::comma_format(
          big.mark = ".",
          decimal.mark = ","),
        option = escalaCores,
        direction = -1) +
      ggplot2::labs(
        title = {{title}},
        subtitle = {{subtitle}},
        x = '',
        y = sprintf('%s 2021', {{nomeEscala}})
      )

  }

  output$colPbfPerCapitaEstadual <-
    shiny::renderPlot(if (input$selectAno == 'Agregado 2013-2023') {
      grafBarrasAnoEstado(
        data = dataAnoEstado() |>
          dplyr::mutate(`R$ Ajustados 2021 per Capita` = `R$ Ajustados 2021 per Capita` / 1000),
        var = `R$ Ajustados 2021 per Capita`,
        escalaCores = input$selectPerCapitaColours,
        nomeEscala = "KR$",
        min = 0.2,
        max = 4.5,
        textRound = 1,
        textDigits =  1,
        title = sprintf('PBF/Auxílio Brasil %s per Capita', input$selectAno),
        subtitle = 'Milhares de R$ Ajustados 2021'
      )
    } else {
      grafBarrasAnoEstado(
        data = dataAnoEstado(),
        var = `R$ Ajustados 2021 per Capita`,
        escalaCores = input$selectPerCapitaColours,
        nomeEscala = "R$",
        min = 26,
        max = 610,
        textRound = 0,
        textDigits =  0,
        title = sprintf('PBF/Auxílio Brasil %s per Capita', input$selectAno),
        subtitle = 'R$ Ajustados 2021'
      )
    },
    height = HEIGHT,
    width = WIDTH)

  output$colunasAnoEstadoPbfPerBenef <-
    shiny::renderPlot(if (input$selectAno == 'Agregado 2013-2023') {
      grafBarrasAnoEstado(
        data = dataAnoEstado() |>
          dplyr::mutate(`R$ Ajustados 2021 por Beneficiário` = `R$ Ajustados 2021 por Beneficiário` / 1000),
        var = `R$ Ajustados 2021 por Beneficiário`,
        escalaCores = input$selectPorBenefColours,
        nomeEscala = "KR$",
        min = 1.6,
        max = 39,
        textRound = 1,
        textDigits =  1,
        title = sprintf('PBF/Auxílio Brasil Total %s por Beneficiário', input$selectAno),
        subtitle = 'Milhares de R$ Ajustados 2021'
      )
    } else {
      grafBarrasAnoEstado(
        data = dataAnoEstado() |>
          dplyr::mutate(`R$ Ajustados 2021 por Beneficiário` = `R$ Ajustados 2021 por Beneficiário` / 1000),
        var = `R$ Ajustados 2021 por Beneficiário`,
        escalaCores = input$selectPorBenefColours,
        nomeEscala = "KR$",
        min = 0.18,
        max = 4.7,
        textRound = 1,
        textDigits =  1,
        title = sprintf('PBF/Auxílio Brasil Total %s por Beneficiário', input$selectAno),
        subtitle = 'Milhares de R$ Ajustados 2021'
      )
    },
    height = HEIGHT,
    width = WIDTH)

  output$colunasTotalEstadoPorAno <-
    shiny::renderPlot(if (input$selectAno == 'Agregado 2013-2023') {
      grafBarrasAnoEstado(
        data = dataAnoEstado(),
        var = `Bilhões R$ Ajustados 2021`,
        escalaCores = input$selectColors_Valores_Absolutos,
        nomeEscala = "BR$",
        min = 2.2,
        max = 53,
        textRound = 1,
        textDigits =  1,
        title = sprintf('PBF/Auxílio Brasil Total %s Valores Absolutos ', input$selectAno),
        subtitle = 'Bilhões de R$ Ajustados 2021'
      )
    } else {
      grafBarrasAnoEstado(
        data = dataAnoEstado(),
        var = `Bilhões R$ Ajustados 2021`,
        escalaCores = input$selectColors_Valores_Absolutos,
        nomeEscala = "BR$",
        min = 0.33,
        max = 8,
        textRound = 1,
        textDigits =  1,
        title = sprintf('PBF/Auxílio Brasil Total %s Valores Absolutos ', input$selectAno),
        subtitle = 'Bilhões de R$ Ajustados 2021'
      )
    },
    height = HEIGHT,
    width = WIDTH)

  ## Mapas aba 'Estado/Ano'
  mapaPbfPerCapita <- function() {
    ggplot2::ggplot() +
      ggplot2::geom_sf(
        data = pbf_estados_df |>
          dplyr::filter(Ano == input$selectAno),
        ggplot2::aes(fill = pbfPerCapita, geometry = geom)
      ) +
      ggplot2::geom_sf_label(
        ggplot2::aes(label = pbf_estados_df$uf,
                     geometry = pbf_estados_df$geom),
        label.size = 0.25,
        label.padding = grid::unit(0.15, "lines"),
        size = 2
      ) +
      viridis::scale_fill_viridis(
        name = "R$",
        discrete = F,
        option = input$selectPerCapitaColours,
        direction = -1
      ) +
      ggplot2::labs(
        title = sprintf(
          'Total %s PBF e Auxílio Brasil per Capita',
          input$selectAno
        ),
        x = '',
        y = 'KR$ 2021',
        subtitle = "R$ Ajustados 2021"
        # caption = "Fonte: www.portaldatransparencia.gov.br"
      ) +
      ggplot2::theme(
        panel.background = ggplot2::element_blank(),
        panel.grid.major = ggplot2::element_line('black', linewidth = 0.01, linetype = 'dashed')
      )
  }

  output$mapaPbfPerCapita <- shiny::renderPlot(mapaPbfPerCapita(),
                                               height = HEIGHT,
                                               width = WIDTH
  )

  mapaPbfPorBenef <- function() {
    ggplot2::ggplot() +
      ggplot2::geom_sf(
        data = pbf_estados_df |>
          dplyr::filter(Ano == input$selectAno) |>
          dplyr::mutate(pbfPerBenef = pbfPerBenef / 1000),
        ggplot2::aes(fill = pbfPerBenef, geometry = geom)
      ) +
      ggplot2::geom_sf_label(
        ggplot2::aes(label = pbf_estados_df$uf,
                     geometry = pbf_estados_df$geom),
        label.size = 0.25,
        label.padding = grid::unit(0.15, "lines"),
        size = 2
      ) +
      viridis::scale_fill_viridis(
        name = "KR$",
        discrete = F,
        option = input$selectPorBenefColours,
        direction = -1
      ) +
      ggplot2::labs(
        title = sprintf(
          'Total %s PBF e Auxílio Brasil por Beneficiário',
          input$selectAno
        ),
        x = '',
        y = 'KR$ 2021',
        subtitle = "Milhares de R$ Ajustados 2021"
        # caption = "Fonte: www.portaldatransparencia.gov.br"
      ) +
      ggplot2::theme(
        panel.background = ggplot2::element_blank(),
        panel.grid.major = ggplot2::element_line('black', linewidth = 0.01, linetype = 'dashed')
      )
  }

  output$mapaPbfPorBenef <- shiny::renderPlot(mapaPbfPorBenef(),
                                              height = HEIGHT,
                                              width = WIDTH)

  mapaTotalEstado <- function() {
    ggplot2::ggplot() +
      ggplot2::geom_sf(
        data = pbf_estados_df |>
          dplyr::filter(Ano == input$selectAno),
        ggplot2::aes(fill = inflac2021, geometry = geom)
      ) +
      ggplot2::geom_sf_label(
        ggplot2::aes(label = pbf_estados_df$uf,
                     geometry = pbf_estados_df$geom),
        label.size = 0.25,
        label.padding = grid::unit(0.15, "lines"),
        size = 2
      ) +
      viridis::scale_fill_viridis(
        name = "BR$",
        discrete = F,
        option = input$selectColors_Valores_Absolutos,
        direction = -1
      ) +
      ggplot2::labs(
        title = sprintf(
          'Total %s PBF e Auxílio Brasil - Valores Absolutos',
          input$selectAno
        ),
        x = '',
        y = 'BR$ 2021',
        subtitle = "Bilhões de R$ Ajustados 2021"
        # caption = "Fonte: www.portaldatransparencia.gov.br"
      ) +
      ggplot2::theme(
        panel.background = ggplot2::element_blank(),
        panel.grid.major = ggplot2::element_line('black', linewidth = 0.01, linetype = 'dashed')
      )
  }

  output$mapaTotalEstado <- shiny::renderPlot(mapaTotalEstado(),
                                              height = HEIGHT,
                                              width = WIDTH)
  # Aba Município/Ano

  dataMun <- shiny::reactive({
    shiny::showNotification("Preparando dados dos Municípios. Isso pode levar até 10 segundos.",
                            duration = 12)
    pbf_municipios_df |>
      dplyr::filter(Ano == input$selectAno) |>
      dplyr::arrange(dplyr::desc(inflac2021)) |>
      dplyr::mutate(
        valor = round(valor / 1e6, 0),
        inflac2021 = round(inflac2021 / 1e6, 2),
        n_benef = as.integer(n_benef),
        pbfPerBenef = round(pbfPerBenef, 2),
        pbfPerCapita = round(pbfPerCapita, 2),
        popBenef = round(n_benef / populacao, 4),
        populacao = round(populacao, 0)
      ) |>
      dplyr::rename(
        'UF' = uf,
        'Nº Médio Beneficiários' = n_benef,
        'Região' = name_region,
        'Milhões R$ Nominal' = valor,
        'Milhões R$ Ajustados 2021' = inflac2021,
        'R$ Ajustados 2021 por Beneficiário' = pbfPerBenef,
        'População IBGE' = populacao,
        'R$ Ajustados 2021 per Capita' = pbfPerCapita,
        'Município' = nome_municipio,
        'Beneficiários / População' = popBenef
      ) |>
      dplyr::select(1, 11, 2, 3, 6, 4, 13, 5, 7, 8, 9)

  })

  output$tabelaTotalMunicipio <- DT::renderDataTable(
    DT::datatable(dataMun(),
                  rownames = FALSE) |>
      DT::formatPercentage(c('Beneficiários / População'), 1) |>
      DT::formatRound(
        columns = c(
          'R$ Ajustados 2021 por Beneficiário',
          'Milhões R$ Ajustados 2021',
          'Milhões R$ Nominal',
          'R$ Ajustados 2021 per Capita'
        ),
        digits = 2,
        mark = ''
      ),
    options = list(
      pageLength = 10,
      autoWidth = TRUE,
      searching = TRUE,
      rownames = FALSE
    )
  )

  # Colunas para aba 'Município / Ano

  colPbfPerCapitaMun <- function() {
    if (input$selectAno == 'Agregado 2013-2023') {
      ggplot2::ggplot(
        data = pbf_municipios_df |>
          dplyr::filter(Ano == input$selectAno) |>
          dplyr::arrange(dplyr::desc(inflac2021)) |>
          dplyr::mutate(
            valor = round(valor / 1e6, 0),
            inflac2021 = round(inflac2021 / 1e6, 2),
            n_benef = as.integer(n_benef),
            pbfPerBenef = round(pbfPerBenef, 2),
            pbfPerCapita = round(pbfPerCapita / 1000, 2),
            popBenef = round(n_benef / populacao, 4)
          ) |>
          dplyr::mutate(nome_uf = stringr::str_c(nome_municipio, ' / ', uf)) |>
          dplyr::arrange(dplyr::desc(pbfPerCapita)) |>
          head(25),
        ggplot2::aes(
          x = factor(nome_uf) |>
            forcats::fct_reorder(pbfPerCapita, .desc = TRUE),
          y = pbfPerCapita,
          fill = pbfPerCapita
        )
      )  +
        ggplot2::geom_col() +
        ggplot2::theme(
          panel.background = ggplot2::element_blank(),
          panel.grid.major = ggplot2::element_line('black', linewidth = 0.02, linetype = 'dashed'),
          axis.text.x = ggplot2::element_text(angle = 90,
                                              vjust = 0.5,
                                              hjust = 1
          )) +
        viridis::scale_fill_viridis(
          name = "KR$",
          discrete = F,
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          option = input$selectPerCapitaColours,
          direction = -1
        ) +
        ggplot2::scale_y_continuous(
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          limits = c(0, 13),
          oob = scales::rescale_none
        ) +
        ggplot2::geom_text(ggplot2::aes(label = round(pbfPerCapita, 1)),
                           vjust = -0.5,
                           size = 2.5) +
        ggplot2::labs(
          title = 'PBF + Aux Brasil: Total 2013-2023 Munic per Capita (Top 25)',
          x = '',
          y = 'KR$ 2021',
          subtitle = "Milhares de R$ Ajustados 2021"
          # caption = "Fonte: www.portaldatransparencia.gov.br"
        )
    } else {
      pbf_municipios_df_2 <- pbf_municipios_df |>
        dplyr::filter(Ano == input$selectAno) |>
        dplyr::mutate(
          nome_uf = stringr::str_c(nome_municipio, ' / ', uf),
          pbfPerCapita = pbfPerCapita / 1000
        ) |>
        dplyr::arrange(dplyr::desc(pbfPerCapita)) |>
        head(25)

      ggplot2::ggplot(
        data = pbf_municipios_df_2,
        ggplot2::aes(
          x = factor(nome_uf) |>
            forcats::fct_reorder(pbfPerCapita, .desc = TRUE),
          y = pbfPerCapita,
          fill = pbfPerCapita
        )
      ) +
        ggplot2::geom_col() +
        ggplot2::theme(
          panel.background = ggplot2::element_blank(),
          panel.grid.major = ggplot2::element_line('black', linewidth = 0.04, linetype = 'dashed'),
          axis.text.x = ggplot2::element_text(angle = 90,
                                              vjust = 0.5,
                                              hjust = 1)) +
        viridis::scale_fill_viridis(
          name = "KR$",
          discrete = F,
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          option = input$selectPerCapitaColours,
          direction = -1
        ) +
        ggplot2::geom_text(ggplot2::aes(label = round(pbfPerCapita, 1)),
                           vjust = -0.5,
                           size = 2.5) +
        ggplot2::scale_y_continuous(
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          limits = c(0, 3),
          oob = scales::rescale_none
        ) +
        ggplot2::labs(
          title = sprintf(
            'PBF e Auxílio Brasil Top 25 Municípios %s per Capita',
            input$selectAno
          ),
          x = '',
          y = 'KR$ 2021',
          subtitle = "Milhares de R$ Ajustados 2021"
          # caption = "Fonte: www.portaldatransparencia.gov.br"
        )
    }
  }

  output$colPbfPerCapitaMun <-
    shiny::renderPlot(colPbfPerCapitaMun(),
                      height = HEIGHT + 200,
                      width = WIDTH)

  colPbfPerBenefMun <- function() {
    if (input$selectAno == 'Agregado 2013-2023') {
      ggplot2::ggplot(
        data = pbf_municipios_df_2 <- pbf_municipios_df |>
          dplyr::filter(Ano == input$selectAno) |>
          dplyr::mutate(
            nome_uf = stringr::str_c(nome_municipio, ' / ', uf),
            pbfPerBenef = pbfPerBenef / 1000
          ) |>
          dplyr::arrange(dplyr::desc(pbfPerBenef)) |>
          head(25),
        ggplot2::aes(
          x = factor(nome_uf) |>
            forcats::fct_reorder(pbfPerBenef, .desc = TRUE),
          y = pbfPerBenef,
          fill = pbfPerBenef
        )
      )  +
        ggplot2::geom_col() +
        ggplot2::theme(
          panel.background = ggplot2::element_blank(),
          panel.grid.major = ggplot2::element_line('black', linewidth = 0.02, linetype = 'dashed'),
          axis.text.x = ggplot2::element_text(
            angle = 90,
            vjust = 0.5,
            hjust = 1
          )
        ) +
        viridis::scale_fill_viridis(
          name = "KR$",
          discrete = F,
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          option = input$selectPorBenefColours,
          direction = -1
        ) +
        ggplot2::scale_y_continuous(
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          limits = c(2, 58.5),
          oob = scales::rescale_none
        ) +
        ggplot2::geom_text(ggplot2::aes(label = round(pbfPerBenef, 1)),
                           vjust = -0.5,
                           size = 2.5) +
        ggplot2::labs(
          title = 'PBF + Aux Brasil: Total 2013-2023 Município por Beneficiário (Top 25)',
          x = '',
          y = 'KR$ 2021',
          subtitle = "Milhares de R$ Ajustados 2021"
          # caption = "Fonte: www.portaldatransparencia.gov.br"
        )
    } else {
      pbf_municipios_df_2 <- pbf_municipios_df |>
        dplyr::filter(Ano == input$selectAno) |>
        dplyr::mutate(
          nome_uf = stringr::str_c(nome_municipio, ' / ', uf),
          pbfPerBenef = pbfPerBenef / 1000
        ) |>
        dplyr::arrange(dplyr::desc(pbfPerBenef)) |>
        head(25)

      ggplot2::ggplot(
        data = pbf_municipios_df_2,
        ggplot2::aes(
          x = factor(nome_uf) |>
            forcats::fct_reorder(pbfPerBenef, .desc = TRUE),
          y = pbfPerBenef,
          fill = pbfPerBenef
        )
      ) +
        ggplot2::geom_col() +
        ggplot2::theme(
          panel.background = ggplot2::element_blank(),
          panel.grid.major = ggplot2::element_line('black', linewidth = 0.04, linetype = 'dashed'),
          axis.text.x = ggplot2::element_text(angle = 90,
                                              vjust = 0.5,
                                              hjust = 1)) +
        viridis::scale_fill_viridis(
          name = "KR$",
          discrete = F,
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          option = input$selectPorBenefColours,
          direction = -1
        ) +
        ggplot2::geom_text(ggplot2::aes(label = round(pbfPerBenef, 1)),
                           vjust = -0.5,
                           size = 2.5) +
        ggplot2::scale_y_continuous(
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          limits = c(0.1, 6.5),
          oob = scales::rescale_none
        ) +
        ggplot2::labs(
          title = sprintf(
            'PBF e Auxílio Brasil Top 25 Municípios %s por Beneficiário',
            input$selectAno
          ),
          x = '',
          y = 'KR$ 2021',
          subtitle = "Milhares de R$ Ajustados 2021"
          # caption = "Fonte: www.portaldatransparencia.gov.br"
        )
    }
  }

  output$colPbfPerBenefMun <-
    shiny::renderPlot(colPbfPerBenefMun(),
                      height = HEIGHT + 200,
                      width = WIDTH)



  colTotalMunicipio <- function() {
    if (input$selectAno == 'Agregado 2013-2023') {
      ggplot2::ggplot(
        data = pbf_municipios_df_2 <- pbf_municipios_df |>
          dplyr::filter(Ano == input$selectAno) |>
          dplyr::mutate(
            nome_uf = stringr::str_c(nome_municipio, ' / ', uf),
            inflac2021 = inflac2021 / 1e9
          ) |>
          dplyr::arrange(dplyr::desc(inflac2021)) |>
          head(25),
        ggplot2::aes(
          x = factor(nome_uf) |>
            forcats::fct_reorder(inflac2021, .desc = TRUE),
          y = inflac2021,
          fill = inflac2021
        )
      )  +
        ggplot2::geom_col() +
        ggplot2::theme(
          panel.background = ggplot2::element_blank(),
          panel.grid.major = ggplot2::element_line('black', linewidth = 0.02, linetype = 'dashed'),
          axis.text.x = ggplot2::element_text(
            angle = 90,
            vjust = 0.5,
            hjust = 1
          )
        ) +
        viridis::scale_fill_viridis(
          name = "BR$",
          discrete = F,
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          option = input$selectColors_Valores_Absolutos,
          direction = -1
        ) +
        ggplot2::scale_y_continuous(
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          limits = c(0, 11.5),
          oob = scales::rescale_none
        ) +
        ggplot2::geom_text(ggplot2::aes(label = round(inflac2021, 1)),
                           vjust = -0.5,
                           size = 2.5) +
        ggplot2::labs(
          title = sprintf(
            'PBF e Auxílio Brasil Top 25 Municípios Total %s Valores Absolutos',
            input$selectAno
          ),
          x = '',
          y = 'BR$ 2021',
          subtitle = "Bilhões de R$ Ajustados 2021"
          # caption = "Fonte: www.portaldatransparencia.gov.br"
        )
    } else {
      ggplot2::ggplot(
        data = pbf_municipios_df_2 <- pbf_municipios_df |>
          dplyr::filter(Ano == input$selectAno) |>
          dplyr::mutate(
            nome_uf = stringr::str_c(nome_municipio, ' / ', uf),
            inflac2021 = inflac2021 / 1e9
          ) |>
          dplyr::arrange(dplyr::desc(inflac2021)) |>
          head(25),
        ggplot2::aes(
          x = factor(nome_uf) |>
            forcats::fct_reorder(inflac2021, .desc = TRUE),
          y = inflac2021,
          fill = inflac2021
        )
      )  +
        ggplot2::geom_col() +
        ggplot2::theme(
          panel.background = ggplot2::element_blank(),
          panel.grid.major = ggplot2::element_line('black', linewidth = 0.02, linetype = 'dashed'),
          axis.text.x = ggplot2::element_text(
            angle = 90,
            vjust = 0.5,
            hjust = 1
          )
        ) +
        viridis::scale_fill_viridis(
          name = "BR$",
          discrete = F,
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          option = input$selectColors_Valores_Absolutos,
          direction = -1
        ) +
        ggplot2::scale_y_continuous(
          labels = scales::comma_format(big.mark = ".",
                                        decimal.mark = ","),
          limits = c(0, 2.3),
          oob = scales::rescale_none
        ) +
        ggplot2::geom_text(ggplot2::aes(label = round(inflac2021, 1)),
                           vjust = -0.5,
                           size = 2.5) +
        ggplot2::labs(
          title = sprintf(
            'PBF e Auxílio Brasil Top 25 Municípios Total %s Valores Absolutos',
            input$selectAno
          ),
          x = '',
          y = 'BR$ 2021',
          subtitle = "Bilhões de R$ Ajustados 2021"
          # caption = "Fonte: www.portaldatransparencia.gov.br"
        )
    }
  }

  output$colTotalMunicipio <-
    shiny::renderPlot(colTotalMunicipio(),
                      height = HEIGHT + 200,
                      width = WIDTH)

  # Mapas Aba 'Município/Ano'
  mapaPbfPerCapitaMun <- function() {
    ggplot2::ggplot() +
      ggplot2::geom_sf(
        data = pbf_municipios_df |>
          dplyr::filter(Ano == input$selectAno) |>
          dplyr::mutate(
            pbfPerBenef = round(pbfPerBenef / 1000, 0),
            pbfPerCapita = round(pbfPerCapita, 0)
          ),
        linewidth = 0,
        ggplot2::aes(fill = pbfPerCapita, geometry = geom)
      ) +
      ggplot2::geom_sf_label(
        ggplot2::aes(label = pbf_estados_df$uf,
                     geometry = pbf_estados_df$geom),
        label.size = 0.25,
        label.padding = grid::unit(0.15, "lines"),
        size = 2
      ) +
      viridis::scale_fill_viridis(
        name = "R$",
        discrete = F,
        option = input$selectPerCapitaColours,
        direction = -1
      ) +
      ggplot2::labs(
        title = sprintf('Total %s PBF e Auxílio Brasil per Capita', input$selectAno),
        subtitle = "R$ Ajustados 2021",
        # caption = "Fonte: www.portaldatransparencia.gov.br",
        x = '',
        y = 'R$ 2021'
      ) +
      ggplot2::theme(
        panel.background = ggplot2::element_blank(),
        panel.grid.major = ggplot2::element_line('black', linewidth = 0.04, linetype = 'dashed'))
  }

  output$mapaPbfPerCapitaMun <-
    shiny::renderPlot(mapaPbfPerCapitaMun(),
                      height = HEIGHT,
                      width = WIDTH)

  mapaPbfPerBenefMun <- function() {
    ggplot2::ggplot() +
      ggplot2::geom_sf(
        data = pbf_municipios_df |>
          dplyr::filter(Ano == input$selectAno) |>
          dplyr::mutate(
            pbfPerBenef = round(pbfPerBenef / 1000, 0),
            pbfPerCapita = round(pbfPerCapita / 1000, 0)
          ),
        linewidth = 0,
        ggplot2::aes(fill = pbfPerBenef, geometry = geom)
      ) +
      ggplot2::geom_sf_label(
        ggplot2::aes(label = pbf_estados_df$uf,
                     geometry = pbf_estados_df$geom),
        label.size = 0.25,
        label.padding = grid::unit(0.15, "lines"),
        size = 2
      ) +
      viridis::scale_fill_viridis(
        name = "KR$",
        discrete = F,
        option = input$selectPorBenefColours,
        direction = -1
      ) +
      ggplot2::labs(
        title = sprintf(
          'Total %s PBF e Auxílio Brasil por Beneficiário',
          input$selectAno
        ),
        subtitle = "Milhares de R$ Ajustados 2021",
        x = '',
        y = 'KR$ 2021'
        # caption = "Fonte: www.portaldatransparencia.gov.br"
      ) +
      ggplot2::theme(
        panel.background = ggplot2::element_blank(),
        panel.grid.major = ggplot2::element_line('black', linewidth = 0.04, linetype = 'dashed'))
  }

  output$mapaPbfPerBenefMun <-
    shiny::renderPlot(mapaPbfPerBenefMun(),
                      height = HEIGHT,
                      width = WIDTH)

  mapaTotalMunicipio <- function() {
    ggplot2::ggplot() +
      ggplot2::geom_sf(
        data = pbf_municipios_df |>
          dplyr::filter(Ano == input$selectAno) |>
          dplyr::mutate(
            valor = round(valor / 1e9, 0),
            inflac2021 = round(inflac2021 / 1e9, 0)
          ),
        linewidth = 0.1,
        ggplot2::aes(fill = inflac2021, geometry = geom)
      ) +
      ggplot2::geom_sf_label(
        ggplot2::aes(label = pbf_estados_df$uf,
                     geometry = pbf_estados_df$geom),
        label.size = 0.25,
        label.padding = grid::unit(0.15, "lines"),
        size = 2
      ) +
      viridis::scale_fill_viridis(
        name = "BR$",
        discrete = F,
        option = input$selectColors_Valores_Absolutos,
        direction = -1
      ) +
      ggplot2::labs(
        title = sprintf(
          'Total %s PBF e Auxílio Brasil - Valores Absolutos',
          input$selectAno
        ),
        subtitle = "Bilhões R$ Ajustados 2021",
        x = '',
        y = 'BR$ 2021'
        # caption = "Fonte: www.portaldatransparencia.gov.br"
      ) +
      ggplot2::theme(
        panel.background = ggplot2::element_blank(),
        panel.grid.major = ggplot2::element_line('black', linewidth = 0.04, linetype = 'dashed'))

  }

  output$mapaTotalMunicipio <-
    shiny::renderPlot(mapaTotalMunicipio(),
                      height = HEIGHT,
                      width = WIDTH)

  # Botões para download das tabelas e gráficos

  output$downloadDataMunGeral <- shiny::downloadHandler(
    filename = function() {
      paste("pBF_auxBr_Municipios_GERAL.xlsx")
    },
    content = function(filename) {
      writexl::write_xlsx(pbf_municipios_df |>
                            dplyr::select(-11), filename)
    }
  )

  output$downloadDataAnual <- shiny::downloadHandler(
    filename = function() {
      paste("pBF_auxBr_Dados_2013-2023.xlsx")
    },
    content = function(filename) {
      writexl::write_xlsx(list(
        'por Anos' = dataAnual(),
        'por Estados' =  pbf_estados_df |>
          dplyr::select(-10) |>
          dplyr::mutate(valor = valor * 1e9,
                        inflac2021 = inflac2021 * 1e9),
        'por Municípios' = pbf_municipios_df |>
          dplyr::select(-11)
      ),
      filename)
    }
  )

  output$downloadImage <- shiny::downloadHandler(
    filename = function() {
      paste("Graficos_pBF_auxBr.zip", sep = "")
    },
    content = function(filename) {
      shiny::showNotification("Preparando os gráficos em alta resolução: Aguarde um pouco que o download já começará!",
                              duration = MESSAGE_DURATION)

      temp_dir = tempdir()
      unlink(paste0(temp_dir, '/*'), recursive = TRUE)

      # Gráficos da aba 'Agregado Anos'
      shiny::showNotification("Preparando Gráfico 1...",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir,
               '/grafBarras_PerCapita_por_Anos.png'),
        plot = grafBarras_por_Anos(
          data = dataAnual(),
          var = `R$ Ajustados 2021 per Capita`,
          nomeEscala = "R$",
          escalaCores = input$selectPerCapitaColours,
          min = 12,
          max = 310,
          textRound = 0,
          textDigits = 0,
          title = 'PBF/Auxílio Brasil Total Anual per Capita',
          subtitle = 'R$ Ajustados 2021'
        ),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 2...",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir,
               '/grafBarras_PorBenef_por_Anos.png'),
        plot = grafBarras_por_Anos(data = dataAnual() |>
                                     dplyr::mutate(
                                       `R$ Ajustados 2021 por Beneficiário` = round(`Bilhões R$ Ajustados 2021` * 1e9 / `Nº Médio Beneficiários` / 1000, 2)
                                     ),
                                   var = `R$ Ajustados 2021 por Beneficiário`,
                                   escalaCores = input$selectPorBenefColours,
                                   nomeEscala = "KR$",
                                   min = 0.17,
                                   max = 4.3,
                                   textRound = 2,
                                   textDigits =  2,
                                   title = 'PBF/Auxílio Brasil Total Anual por Beneficiário',
                                   subtitle = 'Milhares de R$ Ajustados 2021'
        ),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 3... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir,
               '/grafBarras_ValoresAbsolutos_por_Anos.png'),
        plot = grafBarras_por_Anos(
          data = dataAnual(),
          var = `Bilhões R$ Ajustados 2021`,
          escalaCores = input$selectColors_Valores_Absolutos,
          nomeEscala = "BR$",
          min = 2.5,
          max = 65,
          textRound = 0,
          textDigits =  0,
          title = 'PBF/Auxílio Brasil Total Anual Valores Absolutos',
          subtitle = 'Bilhões de R$ Ajustados 2021'
        ),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      #
      shiny::showNotification("Preparando Gráfico 4... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(
          temp_dir,
          '/barras_PbfPerCapita_Estados_',
          input$selectAno,
          '.png'
        ),
        plot = if (input$selectAno == 'Agregado 2013-2023') {
          grafBarrasAnoEstado(
            data = dataAnoEstado() |>
              dplyr::mutate(`R$ Ajustados 2021 per Capita` = `R$ Ajustados 2021 per Capita` / 1000),
            var = `R$ Ajustados 2021 per Capita`,
            escalaCores = input$selectPerCapitaColours,
            nomeEscala = "KR$",
            min = 0.2,
            max = 4.5,
            textRound = 1,
            textDigits =  1,
            title = sprintf('PBF/Auxílio Brasil %s per Capita', input$selectAno),
            subtitle = 'Milhares de R$ Ajustados 2021'
          )
        } else {
          grafBarrasAnoEstado(
            data = dataAnoEstado(),
            var = `R$ Ajustados 2021 per Capita`,
            escalaCores = input$selectPerCapitaColours,
            nomeEscala = "R$",
            min = 26,
            max = 610,
            textRound = 0,
            textDigits =  0,
            title = sprintf('PBF/Auxílio Brasil %s per Capita', input$selectAno),
            subtitle = 'R$ Ajustados 2021'
          )
        },
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 5... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(
          temp_dir,
          '/barras_PbfPerBenef_Estados_',
          input$selectAno,
          '.png'
        ),
        plot = if (input$selectAno == 'Agregado 2013-2023') {
          grafBarrasAnoEstado(
            data = dataAnoEstado() |>
              dplyr::mutate(`R$ Ajustados 2021 por Beneficiário` = `R$ Ajustados 2021 por Beneficiário` / 1000),
            var = `R$ Ajustados 2021 por Beneficiário`,
            escalaCores = input$selectPorBenefColours,
            nomeEscala = "KR$",
            min = 1.6,
            max = 39,
            textRound = 1,
            textDigits =  1,
            title = sprintf('PBF/Auxílio Brasil Total %s por Beneficiário', input$selectAno),
            subtitle = 'Milhares de R$ Ajustados 2021'
          )
        } else {
          grafBarrasAnoEstado(
            data = dataAnoEstado() |>
              dplyr::mutate(`R$ Ajustados 2021 por Beneficiário` = `R$ Ajustados 2021 por Beneficiário` / 1000),
            var = `R$ Ajustados 2021 por Beneficiário`,
            escalaCores = input$selectPorBenefColours,
            nomeEscala = "KR$",
            min = 0.18,
            max = 4.6,
            textRound = 1,
            textDigits =  1,
            title = sprintf('PBF/Auxílio Brasil Total %s por Beneficiário', input$selectAno),
            subtitle = 'Milhares de R$ Ajustados 2021'
          )
        }
      )

      shiny::showNotification("Preparando Gráfico 6... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir,
               '/barras_Total_Estados_',
               input$selectAno,
               '.png'),
        plot = if (input$selectAno == 'Agregado 2013-2023') {
          grafBarrasAnoEstado(
            data = dataAnoEstado(),
            var = `Bilhões R$ Ajustados 2021`,
            escalaCores = input$selectColors_Valores_Absolutos,
            nomeEscala = "BR$",
            min = 2.2,
            max = 53,
            textRound = 1,
            textDigits =  1,
            title = sprintf('PBF/Auxílio Brasil Total %s Valores Absolutos ', input$selectAno),
            subtitle = 'Bilhões de R$ Ajustados 2021'
          )
        } else {
          grafBarrasAnoEstado(
            data = dataAnoEstado(),
            var = `Bilhões R$ Ajustados 2021`,
            escalaCores = input$selectColors_Valores_Absolutos,
            nomeEscala = "BR$",
            min = 0.33,
            max = 8,
            textRound = 1,
            textDigits =  1,
            title = sprintf('PBF/Auxílio Brasil Total %s Valores Absolutos ', input$selectAno),
            subtitle = 'Bilhões de R$ Ajustados 2021'
          )
        },

        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 7... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(
          temp_dir,
          '/mapaEstadosPerCapita_',
          input$selectAno,
          '.png'
        ),
        plot = mapaPbfPerCapita(),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 8... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(
          temp_dir,
          '/mapaEstadosPerBenef_',
          input$selectAno,
          '.png'
        ),
        plot = mapaPbfPorBenef(),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 9... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir, '/mapaEstadosValoresAbsolutos_', input$selectAno, '.png'),
        plot = mapaTotalEstado(),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 10... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir,
               '/barrasMunicipiosPerCapita_',
               input$selectAno,
               '.png'),
        plot = colPbfPerCapitaMun(),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 11... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir,
               '/barrasMunicipiosPorBenef_',
               input$selectAno,
               '.png'),
        plot = colPbfPerBenefMun(),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 12... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir,
               '/barrasMunicipiosValoresAbsolutos_',
               input$selectAno,
               '.png'),
        plot = colTotalMunicipio(),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 13... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir,
               '/mapasMunicipiosPerCapita_',
               input$selectAno,
               '.png'),
        plot = mapaPbfPerCapitaMun(),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 14... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir,
               '/mapasMunicipiosPorBenef_',
               input$selectAno,
               '.png'),
        plot = mapaPbfPerBenefMun(),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Preparando Gráfico 15... ",
                              duration = MESSAGE_DURATION)
      ggplot2::ggsave(
        paste0(temp_dir,
               '/mapasMunicipiosValoresAbsolutos_',
               input$selectAno,
               '.png'),
        plot = mapaTotalMunicipio(),
        width = WIDTH_FILE,
        height = HEIGHT_FILE,
        dpi = DPI
      )

      shiny::showNotification("Quase pronto, arquivo ZIP em criação...")

      utils::tar(filename,
                 temp_dir)
    }
  )
}

shiny::shinyApp(ui, server)
