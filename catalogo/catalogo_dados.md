# CATÁLOGO DE DADOS DO DATA LAKEHOUSE (ANP / SSD)

---

## 1. INTRODUÇÃO E POLÍTICA DE METADADOS
O Catálogo de Dados descreve formalmente todos os atributos integrantes do Esquema Estrela (Star Schema) de Produção de Petróleo e Gás Natural, modelado para suportar decisões táticas e operacionais de intervenção em poços maduros (*workover*).

Conforme estabelecido no guião da disciplina de Sistemas de Suporte à Decisão (SSD) do Departamento de Engenharia de Produção da Universidade de Brasília (UnB), cada atributo contém rigorosamente as **6 dimensões mínimas obrigatórias**:
1. **Nome do Atributo:** Identificador na tabela final da camada Gold.
2. **Tipo:** Numérico, categórico, texto, data ou booleano.
3. **Descrição:** Significado no contexto de negócio e engenharia de reservatório.
4. **Domínio:** Faixa de valores esperados (mín/máx) ou vocabulário controlado de categorias.
5. **Obrigatoriedade:** Admissibilidade de nulos e o significado semântico de eventuais ausências.
6. **Linhagem:** Rastreabilidade de procedência, data de extração e regras de transformação aplicadas.

---

## 2. TABELA FATO: `fato_producao_mensal`
- **Conceito de Negócio:** Tabela central de eventos quantitativos de extração de fluidos e operação física dos poços.
- **Grão (Grain):** Uma linha por poço por mês civil de produção.

| Nome do Atributo | Tipo | Descrição | Domínio | Obrigatoriedade | Linhagem |
|:---|:---|:---|:---|:---|:---|
| `sk_tempo` | Numérico (Inteiro) | Chave estrangeira que referencia a dimensão tempo na granularidade mês/ano. | `[202001, 203512]` no formato `YYYYMM` | Obrigatório (NOT NULL). A ausência invalida a análise de séries temporais. | Derivado no ETL: `ano * 100 + mes`. |
| `sk_poco` | Numérico (Inteiro) | Chave estrangeira para a dimensão poço. | Inteiro de 32 bits gerado via hash determinístico. | Obrigatório (NOT NULL). A ausência indicaria fluido sem poço de origem. | Hash SHA-256 do `cod_poco` normalizado da ANP. |
| `sk_instalacao` | Numérico (Inteiro) | Chave estrangeira para a unidade de recebimento e processo de superfície. | Inteiro de 32 bits gerado via hash determinístico. | Obrigatório (NOT NULL). Ausência indicaria poço sem rota de escoamento. | Hash SHA-256 do campo `instalacao` da ANP. |
| `sk_campo` | Numérico (Inteiro) | Chave estrangeira para o campo geológico concessionado. | Inteiro de 32 bits gerado via hash determinístico. | Obrigatório (NOT NULL). Ausência descaracteriza a concessão regulatória. | Hash SHA-256 do campo `campo` da ANP. |
| `volume_oleo_m3` | Numérico (Float) | Volume total líquido de óleo fiscalizado produzido no mês pelo poço em metros cúbicos ($m^3$). | `[0.0, 350000.0]` $m^3$/mês | Obrigatório (NOT NULL). Zero indica poço sem vazão no período. | Campo bruto `Producao_Oleo_m3` da ANP. Tratamento de nulos convertidos para 0.0. |
| `volume_oleo_bbl` | Numérico (Float) | Volume de óleo equivalente produzido no mês expresso em barris de petróleo ($bbl$). | `[0.0, 2200000.0]` barris/mês | Obrigatório (NOT NULL). Zero reflete produção nula de óleo. | Calculado no ETL Gold: `volume_oleo_m3 * 6.28981077`. Arredondado em 2 decimais. |
| `volume_gas_associado_Mm3` | Numérico (Float) | Volume de gás natural produzido em solução com o petróleo em mil metros cúbicos ($Mm^3$). | `[0.0, 150000.0]` $Mm^3$/mês | Obrigatório (NOT NULL). Nulo imputado para 0.0. | Campo `Producao_Gas_Associado_Mm3` do BMP/ANP. |
| `volume_gas_nao_associado_Mm3` | Numérico (Float) | Volume de gás natural livre produzido em reservatórios gasíferos em mil metros cúbicos ($Mm^3$). | `[0.0, 150000.0]` $Mm^3$/mês | Obrigatório (NOT NULL). Nulo imputado para 0.0. | Campo `Producao_Gas_Nao_Associado_Mm3` do BMP/ANP. |
| `volume_gas_total_Mm3` | Numérico (Float) | Soma do gás associado e não associado produzido pelo poço ($Mm^3$). | `[0.0, 300000.0]` $Mm^3$/mês | Obrigatório (NOT NULL). | Campo `Producao_Gas_Total_Mm3` ou soma analítica dos gases. |
| `volume_gas_queima_Mm3` | Numérico (Float) | Volume de gás natural queimado no flare (tocha) da plataforma ou estação coletora ($Mm^3$). | `[0.0, 50000.0]` $Mm^3$/mês | Obrigatório (NOT NULL). Não pode ser maior que `volume_gas_total_Mm3`. | Campo `Queima_Gas_Mm3` da ANP. |
| `volume_gas_injecao_Mm3` | Numérico (Float) | Volume de gás comprimido e reinjetado no reservatório para manutenção de pressão ($Mm^3$). | `[0.0, 200000.0]` $Mm^3$/mês | Obrigatório (NOT NULL). Nulo imputado como 0.0. | Campo `Injecao_Gas_Mm3` do BMP/ANP. |
| `volume_agua_m3` | Numérico (Float) | Volume total de água de formação e condensada co-produzida com o petróleo ($m^3$). | `[0.0, 500000.0]` $m^3$/mês | Obrigatório (NOT NULL). Nulos tratados como 0.0. | Campo `Producao_Agua_m3` do BMP/ANP. |
| `tempo_producao_dias` | Numérico (Inteiro) | Número de dias efetivos em que o poço operou com fluxo aberto para a instalação no mês civil. | `[0, 31]` dias | Obrigatório (NOT NULL). Valor 0 indica poço fechado/parado durante todo o mês. | Campo `Tempo_Producao_Dias` da ANP. Limitado ao número de dias do respectivo mês civil. |
| `vazao_media_oleo_m3d` | Numérico (Float) | Vazão média diária de óleo considerando estritamente os dias ativos de fluxo ($m^3$/dia). | `[0.0, 15000.0]` $m^3$/dia | Obrigatório (NOT NULL). Zero quando poço não produziu. | Calculado: `volume_oleo_m3 / tempo_producao_dias` (quando dias > 0; senão 0.0). |
| `vazao_media_agua_m3d` | Numérico (Float) | Vazão média diária de efluente aquoso considerando os dias ativos de fluxo ($m^3$/dia). | `[0.0, 20000.0]` $m^3$/dia | Obrigatório (NOT NULL). Zero quando poço não produziu. | Calculado: `volume_agua_m3 / tempo_producao_dias` (quando dias > 0; senão 0.0). |
| `corte_agua_bsw_pct` | Numérico (Float) | Proporção de água presente no volume total de fluido líquido extraído (Basic Sediments & Water %). | `[0.0, 100.0]` % | Obrigatório (NOT NULL). Zero para poços sem produção de água. | Calculado: `(volume_agua_m3 / (volume_oleo_m3 + volume_agua_m3)) * 100`. |
| `razao_agua_oleo_wor` | Numérico (Float) | Razão matemática entre o volume de água e o volume de óleo produzido (Water-Oil Ratio). | `[0.0, 999.99]` | Obrigatório (NOT NULL). Teto 999.99 adotado quando óleo = 0 e água > 0. | Calculado: `volume_agua_m3 / volume_oleo_m3`. |
| `taxa_queima_gas_flare_pct` | Numérico (Float) | Percentual de queima em tocha sobre o total de gás extraído pelo poço (Flare Ratio %). | `[0.0, 100.0]` % | Obrigatório (NOT NULL). Zero quando não houve queima. | Calculado: `(volume_gas_queima_Mm3 / volume_gas_total_Mm3) * 100`. |

---

## 3. DIMENSÃO TEMPO: `dim_tempo`
- **Conceito de Negócio:** Calendário corporativo e fiscal de competências regulatórias.

| Nome do Atributo | Tipo | Descrição | Domínio | Obrigatoriedade | Linhagem |
|:---|:---|:---|:---|:---|:---|
| `sk_tempo` | Numérico (Inteiro) | Chave primária substituta da dimensão tempo. | Formato `YYYYMM` (ex: `202403`) | Obrigatório (Chave Primária). | Derivado determinístico no pipeline Silver. |
| `ano_mes` | Texto | Identificador textual canônico da competência contábil mensal. | Formato `YYYY-MM` (ex: `2024-03`) | Obrigatório (NOT NULL). | Concatenação das colunas `ano` e `mes` formatado com 2 dígitos. |
| `ano` | Numérico (Inteiro) | Ano civil da medição da produção. | `[2020, 2035]` | Obrigatório (NOT NULL). | Extraído diretamente do BMP/ANP. |
| `mes` | Numérico (Inteiro) | Mês civil da medição da produção. | `[1, 12]` | Obrigatório (NOT NULL). | Extraído diretamente do BMP/ANP. |
| `nome_mes` | Texto | Nome por extenso do mês na língua portuguesa. | `Janeiro` a `Dezembro` | Obrigatório (NOT NULL). | Mapeado via tabela interna de calendário. |
| `trimestre` | Texto | Identificador do trimestre fiscal e civil do ano. | `1T2024`, `2T2024`, `3T2024`, `4T2024` | Obrigatório (NOT NULL). | Calculado: `((mes - 1) // 3 + 1) + 'T' + ano`. |
| `semestre` | Texto | Identificador do semestre do ano. | `1S2024`, `2S2024` | Obrigatório (NOT NULL). | Calculado: `1S` se `mes <= 6` senão `2S`. |
| `dias_no_mes` | Numérico (Inteiro) | Quantidade total de dias calendário presentes no respectivo mês civil. | `28, 29, 30, 31` | Obrigatório (NOT NULL). | Calculado via módulo `calendar` respeitando anos bissextos. |

---

## 4. DIMENSÃO POÇO: `dim_poco`
- **Conceito de Negócio:** Ativo de subsuperfície perfurado para comunicação com o reservatório de petróleo e gás.

| Nome do Atributo | Tipo | Descrição | Domínio | Obrigatoriedade | Linhagem |
|:---|:---|:---|:---|:---|:---|
| `sk_poco` | Numérico (Inteiro) | Chave primária substituta do poço. | Inteiro de 32 bits | Obrigatório (Chave Primária). | Hash SHA-256 gerado a partir do `cod_poco`. |
| `cod_poco` | Texto | Código alfanumérico cadastral padronizado pela ANP (Padrão de Nomenclatura Oficial). | Código único oficial (ex: `7-MRL-215D-RJS`) | Obrigatório (Chave Natural). | Coluna `Poco` da ANP, convertida para maiúsculas e sem espaços. |
| `nome_poco` | Texto | Nome operacional curto utilizado pelos engenheiros na plataforma. | Texto livre (ex: `MRL-215D`) | Obrigatório (NOT NULL). | Extraído do BMP ou sufixo da sigla do campo. |
| `operador` | Texto | Razão social ou denominação da operadora concessionária do bloco. | `PETROBRAS`, `3R PETROLEUM`, `PETRORECONCAVO`, etc. | Obrigatório (NOT NULL). | Coluna `Operador` do BMP/ANP com padronização semântica. |
| `ambiente` | Categórico (Texto) | Meio geográfico em que a cabeça do poço se situa. | `MAR` (Offshore) ou `TERRA` (Onshore) | Obrigatório (NOT NULL). | Coluna `Ambiente` do BMP/ANP validada contra domínio estrito. |
| `status_operacional` | Categórico (Texto) | Situação cadastral do poço perante a regulamentação mineral. | `ATIVO`, `FECHADO TEMPORARIAMENTE`, `ABANDONADO` | Obrigatório (NOT NULL). | Regra Silver: `ATIVO` se produziu nos últimos 12 meses. |

---

## 5. DIMENSÃO INSTALAÇÃO: `dim_instalacao`
- **Conceito de Negócio:** Planta de processo e separação primária (óleo, gás, água) em superfície.

| Nome do Atributo | Tipo | Descrição | Domínio | Obrigatoriedade | Linhagem |
|:---|:---|:---|:---|:---|:---|
| `sk_instalacao` | Numérico (Inteiro) | Chave primária substituta da instalação. | Inteiro de 32 bits | Obrigatório (Chave Primária). | Hash SHA-256 gerado a partir de `nome_instalacao`. |
| `nome_instalacao` | Texto | Denominação oficial da plataforma ou estação de coleta. | Texto (ex: `P-35`, `P-52`, `FPSO CIDADE DE MARICA`) | Obrigatório (Chave Natural). | Coluna `Instalacao` do BMP/ANP normalizada em maiúsculas. |
| `tipo_instalacao` | Categórico (Texto) | Tipologia estrutural da planta de superfície. | `FPSO`, `SEMISUBMERSIVEL`, `PLATAFORMA FIXA`, `ESTACAO COLETORA` | Obrigatório (NOT NULL). | Classificado a partir do cadastro de instalações da ANP. |
| `bacia` | Categórico (Texto) | Bacia sedimentar brasileira em que a instalação está ancorada. | `CAMPOS`, `SANTOS`, `POTIGUAR`, `RECONCAVO`, `SOLIMOES` | Obrigatório (NOT NULL). | Coluna `Bacia` do BMP/ANP. |
| `estado` | Categórico (Texto) | Sigla da Unidade Federativa correspondente ao confronto territorial. | `RJ`, `SP`, `ES`, `RN`, `BA`, `SE`, `AL`, `AM` | Obrigatório (NOT NULL). | Coluna `Estado` do BMP/ANP com validação de 2 caracteres. |

---

## 6. DIMENSÃO CAMPO: `dim_campo`
- **Conceito de Negócio:** Unidade geológica e geográfica produtora delimitada em contrato de concessão de E&P.

| Nome do Atributo | Tipo | Descrição | Domínio | Obrigatoriedade | Linhagem |
|:---|:---|:---|:---|:---|:---|
| `sk_campo` | Numérico (Inteiro) | Chave primária substituta do campo produtor. | Inteiro de 32 bits | Obrigatório (Chave Primária). | Hash SHA-256 gerado a partir de `nome_campo`. |
| `nome_campo` | Texto | Denominação pública oficial do campo delimitado pela ANP. | Texto (ex: `MARLIM`, `RONCADOR`, `CANTO DO AMARO`, `BUZIOS`) | Obrigatório (Chave Natural). | Coluna `Campo` do BMP/ANP convertida para maiúsculas. |
| `bacia` | Categórico (Texto) | Província geológica petrolífera sedimentar. | `CAMPOS`, `SANTOS`, `POTIGUAR`, `RECONCAVO` | Obrigatório (NOT NULL). | Coluna `Bacia` do BMP/ANP. |
| `estado` | Categórico (Texto) | Sigla da Unidade Federativa confrontante. | Siglas oficiais do Brasil (`RJ`, `RN`, `BA`, etc.) | Obrigatório (NOT NULL). | Coluna `Estado` do BMP/ANP. |
| `estagio_vida` | Categórico (Texto) | Classificação técnica quanto à maturidade produtiva do campo. | `PRÉ-SAL / EXPANSÃO`, `OFFSHORE MADURO`, `ONSHORE MADURO` | Obrigatório (NOT NULL). | Regra de Negócio: inferida no ETL combinando ambiente e histórico. |

---

## 7. POLÍTICA DE ATUALIZAÇÃO E RETENÇÃO DE DADOS
- **Frequência de Ingestão:** Mensal (batch agendado), sincronizada com a publicação do BMP pela ANP.
- **Janela Regulatória:** Defasagem oficial de $D+60$ dias decorrente dos prazos de envio e homologação dos Boletins pelas concessionárias.
- **Histórico Acumulado:** Retenção perpétua em armazenamento frio (Data Lake S3/ADLS/Blob) e retenção quente de 10 anos em Delta Lake para treinamento de modelos de declínio exponencial de Arps.
