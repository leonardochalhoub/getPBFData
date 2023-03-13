`GetPBFData` é um pacote em linguagem R que objetiva permitir acesso simplificado a dados públicos relativos aos pagamentos do Programa Bolsa Família (PBF) e também de seu substituto, o Auxílio Brasil. Todos os dados estão disponíveis totalmente livres pelo governo brasileiro, porém a forma em que se apresenta atualmente pode trazer desafios e bastante trabalho manual para quem deseja explorá-los a fundo, o que talvez até impossibilite pesquisas.

Uma dificuldade é que os dados estão disponibilizados em arquivos zip contendo csv, em frequência mensal, com cada arquivo possuindo 14 milhões de linhas, ou seja, uma planilha que não é possível de ser aberta no Excel. Outra dificuldade é o tamanho dos arquivos que, além de precisarem ser baixados um a um no site oficial, são enormes: em Março de 2023, temos dados desde 2013 até Novembro de 2022, o que totalizam 180 gigas de dados em csv.

Para análise destes dados, o foco não é nos beneficiários de forma individual mas, sim, nas agregações. Este pacote está sendo desenvolvido para ser útil a pesquisadores, cientistas de dados, mestres, especialistas, bacharéis e estudantes que possam se interessar por Gestão Pública no Brasil e queiram acessar de forma mais rápida e resumida os dados oficiais. Os dados em sua forma bruta estão disponíveis desde Janeiro de 2013 até Novembro de 2021 para o PBF, e de Novembro de 2021 até Novembro de 2022 para o Auxílio Brasil. Considero aqui os programas como literais substitutos - Novembro/2021 é somado e as séries são consideradas uma continuidade do principal programa de assistência social do Brasil.

[https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/](https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/)

[https://portaldatransparencia.gov.br/download-de-dados/auxilio-brasil/](https://portaldatransparencia.gov.br/download-de-dados/auxilio-brasil/)

Uma interface em Shiny para visualização dos dados refinados está disponível na plataforma grátis shinyapps.io:  [https://g7eewc-oldman0ds.shinyapps.io/app_pbf_auxbr/](https://g7eewc-oldman0ds.shinyapps.io/app_pbf_auxbr/)

Na pasta /arquivos_aux, aqui no github, está o arquivo populacao.xlsx, resultado de coleta e verificações manuais que fiz a partir da [fonte oficial do IBGE](https://www.ibge.gov.br/estatisticas/sociais/populacao/9103-estimativas-de-populacao.html?edicao=17283&t=downloads). Aqui estão os dados para estados e municípios, de 2013 a 2021. Para 2022, eu repeti os valores de 2021.


# Instalação do pacote getPBFData

```r
# github (dev version)
devtools::install_github('leonardochalhoub/getPBFData')
```

# Como usar o pacote. 4 Passos / Methods

## Passo 1: Baixar todos os dados
É importante fazer sua base de dados local, porque o volume de dados é bastante grande. O algoritmo detecta arquivos zip na pasta /source_data, evitando downloads desnecessários. Se houver arquivos csv na pasta /data, não é executada a descompressão dos arquivos na pasta /source_data. Esse processo pode demorar algumas horas, dependendo não só da sua conexão com a Internet, mas também da banda que o servidor está conseguindo oferecer.

```r
folder <- '~/Dados_Bolsa_Familia_Aux_Brasil'

getPBFData::downloadData_Passo1(pasta = folder)
```

## Passo 2: Processamento dos Dados e Summarising para Municípios, Estados, Anos e Meses

No final desta etapa, que também pode demorar algumas horas, são gerados arquivos em formato RDS na pasta /outputs. A ideia é não precisar processar tudo de novo. A partir da próxima etapa, os dados são carregados dos RDS, o que facilita muito o trabalho posterior.

```{r}
getPBFData::dataPrep_Passo2(pasta = folder)
```

## Passo 3: Pós-Processamento

Já temos dados mais fáceis de manusear disponíveis nesta etapa. Aqui agrego mais ainda os dados e faço algumas manipulações importantes, como por exemplo inflacionar os valores para Reais de 2021 com referência pela mediana anual do IPCA através do pacote priceR, que utiliza como fonte o site do Banco Mundial. Também agrego geolocalizações, para ser possível depois trabalhar com mapas. No final desta etapa, quatro arquivos novos são escritos na pasta /outputs, dois agregados por estado e dois por municípios.

```r
getPBFData::postProc_Passo3(pasta = folder)
```

## Passo 4: Visualizando os dados num Web App em Shiny

```r
getPBFData::shinyApp_Passo4(pasta = folder)
```


