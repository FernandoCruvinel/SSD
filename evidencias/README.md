# GUIA E ROTEIRO DE EVIDÊNCIAS DE EXECUÇÃO EM NUVEM (DATABRICKS)

---

## 1. OBJETIVO DO CADERNO DE EVIDÊNCIAS
Em estrito cumprimento ao item **5.2 (Evidências)** e às rubricas de avaliação do Trabalho Final de Sistemas de Suporte à Decisão (SSD/UnB), este documento estabelece o roteiro padronizado e a descrição detalhada das capturas de tela (screenshots) e gravações que comprovam a execução real do pipeline ponta a ponta na plataforma de nuvem **Databricks**.

As evidências aqui referenciadas atestam:
1. **Configuração e operação do cluster de processamento distribuído.**
2. **Execução bem-sucedida do Pipeline ETL (Camadas Bronze, Silver e Gold em Delta Lake).**
3. **Persistência física dos dados no sistema de arquivos distribuído (DBFS / Unity Catalog / Cloud Storage).**
4. **Execução automatizada da auditoria das 6 dimensões de qualidade de dados.**
5. **Execução das consultas analíticas SQL que respondem às 4 Perguntas de Negócio.**
6. **Resultados e recomendações do Algoritmo Multicritério TOPSIS para intervenção em poços.**

---

## 2. MATRIZ DE RASTREABILIDADE DAS EVIDÊNCIAS

| ID Evidência | Artefato / Tela Comprovada | Seção Relacionada no README | Descrição do Conteúdo Comprovado |
|:---|:---|:---|:---|
| `EVID-01` | Databricks Compute Cluster | Seção 6 (Pipeline ETL e Nuvem) | Cluster Spark ativo (Runtime 13.3+ LTS, nós de execução, recursos alocados). |
| `EVID-02` | Pipeline ETL Bronze -> Silver | Seção 6 (Carga e Nuvem) | Leitura dos CSVs da ANP, inclusão de metadados técnicos de auditoria e gravação em Delta Bronze. |
| `EVID-03` | Carga Dimensional Gold | Seção 5 e 6 (Modelagem e Carga) | Criação das tabelas `dim_tempo`, `dim_poco`, `dim_instalacao`, `dim_campo` e `fato_producao_mensal`. |
| `EVID-04` | Persistência Física no DBFS | Seção 6 (Persistência na Nuvem) | Comando `DESCRIBE DETAIL fato_producao_mensal` comprovando formato `delta`, tamanho em disco e arquivos Parquet. |
| `EVID-05` | Testes de Qualidade (DQA) | Seção 7 (Qualidade de Dados) | Saída do console executando as 6 dimensões de qualidade com 99,17% de conformidade. |
| `EVID-06` | P1: Perda e Declínio de Óleo | Seção 8 (Pergunta de Negócio 1) | Resultado da query SQL comparando 2023-S1 vs 2024-S2 e identificando P-35 e P-31 com maior declínio. |
| `EVID-07` | P2: Corte de Água (BSW / WOR) | Seção 8 (Pergunta de Negócio 2) | Tabela e gráfico de BSW por plataforma demonstrando saturação hídrica em P-25 e Canto do Amaro. |
| `EVID-08` | P3: Queima de Gás (Flare Ratio)| Seção 8 (Pergunta de Negócio 3) | Consulta SQL demonstrando anomalia na plataforma P-54 (queima > 16% violando Resolução ANP). |
| `EVID-09` | P4: Curva de Pareto 80/20 | Seção 8 (Pergunta de Negócio 4) | Ranqueamento ABC e Pareto demonstrando que poços do Pré-sal e Roncador concentram 80% do volume. |
| `EVID-10` | Otimização TOPSIS | Seção 8 (Módulo Prescritivo SSD) | Tabela com o ranking final dos poços prioritários para despacho da sonda de workover. |

---

## 3. ROTEIRO PASSO A PASSO PARA REPRODUÇÃO E CAPTURA

### Passo 1: Configuração do Cluster no Databricks
1. No menu lateral do Databricks, acesse **Compute** e inicie um cluster com as configurações mínimas:
   - *Cluster Name:* `ssd-unb-cluster-anp`
   - *Databricks Runtime Version:* `13.3 LTS (Apache Spark 3.4.1, Scala 2.12)`
   - *Node Type:* `Standard_DS3_v2` (ou máquina gratuita do Community Edition)
2. Capture o print mostrando o cluster em estado **Running** (`evidencias/01_cluster_databricks.png`).

### Passo 2: Execução do Notebook `01_pipeline_etl_anp.py`
1. Importe o notebook `notebooks/01_pipeline_etl_anp.py` no workspace do Databricks.
2. Execute todas as células (`Run All`).
3. Verifique a escrita das tabelas Delta:
   ```sql
   %sql
   SHOW TABLES IN default;
   ```
4. Execute o comando de auditoria de armazenamento para comprovar a persistência efetiva em nuvem:
   ```sql
   %sql
   DESCRIBE DETAIL fato_producao_mensal;
   ```
5. Capture os prints dos logs de conclusão e da tabela `DESCRIBE DETAIL` (`evidencias/02_etl_delta_gold.png` e `evidencias/03_persistencia_dbfs.png`).

### Passo 3: Execução do Notebook `02_analise_e_decisao.py`
1. Importe e anexe o notebook `notebooks/02_analise_e_decisao.py` ao cluster ativo.
2. Execute a seção de **Auditoria de Qualidade** e capture a saída tabular com as 6 dimensões (`evidencias/04_data_quality_report.png`).
3. Execute as consultas SQL analíticas das 4 Perguntas de Negócio, gerando as visualizações gráficas nativas do Databricks (barras para BSW, dispersão para declínio e pizza/Pareto para concentração).
4. Capture os prints dos resultados de cada pergunta (`evidencias/05_p1_declinio.png`, `evidencias/06_p2_bsw.png`, `evidencias/07_p3_flare.png`, `evidencias/08_p4_pareto.png`).
5. Execute a célula final do **Algoritmo TOPSIS** e capture a tabela final com os escores de priorização de intervenção (`evidencias/09_topsis_ranking.png`).

---

## 4. LOCALIZAÇÃO E REPOSITÓRIO PÚBLICO
Todos os prints gerados devem ser nomeados conforme a tabela acima e armazenados nesta pasta `/evidencias`.

> [!IMPORTANT]
> O link para o repositório público do GitHub e a visualização das evidências foram conferidos em **janela anônima** do navegador, assegurando a ausência de barreiras de autenticação para a comissão avaliadora do Departamento de Engenharia de Produção da UnB.
