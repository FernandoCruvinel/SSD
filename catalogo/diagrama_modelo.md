# ARQUITETURA DIMENSIONAL E MODELAGEM DE DADOS: ESQUEMA ESTRELA (STAR SCHEMA)

---

## 1. INTRODUÇÃO E JUSTIFICATIVA DE ENGENHARIA DE DADOS

Para viabilizar o processamento em larga escala e a exploração interativa de séries históricas de produção de petróleo e gás da ANP, adotou-se o **Esquema Estrela (Star Schema)** como padrão de modelagem dimensional no Data Lakehouse (Databricks / Delta Lake).

### Por que o Esquema Estrela e não a Terceira Forma Normal (3NF) ou Snowflake?
1. **Minimização de Shuffle Joins no Apache Spark:** Em arquiteturas distribuídas de nuvem, junções excessivas entre tabelas normalizadas (3NF ou Snowflake profundo) geram tráfego massivo de rede (*shuffle* de partições), degradando severamente o tempo de resposta das consultas analíticas. O Esquema Estrela centraliza as métricas e desnormaliza as dimensões, permitindo *Broadcast Joins* eficientes em memória.
2. **Otimização Colunar em Formato Parquet / Delta:** O formato Delta Lake armazena dados colunarmente. Atributos descritivos na mesma dimensão sofrem compressão com alta taxa de compressão (Snappy/ZSTD) sem custo adicional de redundância em disco.
3. **Ergonomia e Simplicidade Cognitiva para o Decisor:** Reduz a complexidade da redação de consultas SQL por analistas e cientistas de dados, acelerando a fase de Concepção e Inteligência de Simon.

---

## 2. DIAGRAMA VISUAL DO ESQUEMA ESTRELA (MERMAID)

```mermaid
erDiagram
    dim_tempo ||--o{ fato_producao_mensal : "registra competencia temporal (1:N)"
    dim_poco ||--o{ fato_producao_mensal : "origina extracao (1:N)"
    dim_instalacao ||--o{ fato_producao_mensal : "processa e estoca (1:N)"
    dim_campo ||--o{ fato_producao_mensal : "delimita reservatorio geologico (1:N)"

    dim_tempo {
        int sk_tempo PK "Chave Substituta (YYYYMM)"
        string ano_mes "Competência Contábil (ex: 2024-05)"
        int ano "Ano civil (2023, 2024)"
        int mes "Mês civil (1 a 12)"
        string nome_mes "Nome extenso (ex: Maio)"
        string trimestre "Identificador de Trimestre (ex: 2T2024)"
        string semestre "Identificador de Semestre (ex: 1S2024)"
        int dias_no_mes "Total de dias do mês calendário"
    }

    dim_poco {
        int sk_poco PK "Chave Substituta Hash (SHA-256)"
        string cod_poco UK "Código oficial ANP (ex: 7-MRL-215D-RJS)"
        string nome_poco "Nome operacional simplificado"
        string operador "Empresa concessionária (Petrobras, 3R, etc)"
        string ambiente "Ambiente operacional (MAR, TERRA)"
        string status_operacional "Situação do ativo (ATIVO, FECHADO)"
    }

    dim_instalacao {
        int sk_instalacao PK "Chave Substituta Hash (SHA-256)"
        string nome_instalacao UK "Nome da unidade (P-35, FPSO Carioca, etc)"
        string tipo_instalacao "Tipologia (FPSO, SEMISUB, FIXA, COLETORA)"
        string bacia "Bacia sedimentar de operação"
        string estado "Unidade Federativa (RJ, RN, BA, etc)"
    }

    dim_campo {
        int sk_campo PK "Chave Substituta Hash (SHA-256)"
        string nome_campo UK "Nome oficial do campo (MARLIM, BUZIOS, etc)"
        string bacia "Bacia sedimentar geológica"
        string estado "Estado produtor"
        string estagio_vida "Maturidade (PRÉ-SAL, OFFSHORE MADURO, ONSHORE)"
    }

    fato_producao_mensal {
        int sk_tempo FK "Chave estrangeira da Dimensão Tempo"
        int sk_poco FK "Chave estrangeira da Dimensão Poço"
        int sk_instalacao FK "Chave estrangeira da Dimensão Instalação"
        int sk_campo FK "Chave estrangeira da Dimensão Campo"
        float volume_oleo_m3 "Métrica aditiva: Produção física de óleo (m³)"
        float volume_oleo_bbl "Métrica aditiva: Volume convertido em barris (bbl)"
        float volume_gas_associado_Mm3 "Métrica aditiva: Gás natural associado (Mm³)"
        float volume_gas_nao_associado_Mm3 "Métrica aditiva: Gás não associado (Mm³)"
        float volume_gas_total_Mm3 "Métrica aditiva: Total de gás extraído (Mm³)"
        float volume_gas_queima_Mm3 "Métrica aditiva: Gás queimado em flare (Mm³)"
        float volume_gas_injecao_Mm3 "Métrica aditiva: Gás reinjetado no poço (Mm³)"
        float volume_agua_m3 "Métrica aditiva: Volume de efluente hídrico (m³)"
        int tempo_producao_dias "Métrica semi-aditiva: Dias em produção no mês"
        float vazao_media_oleo_m3d "Métrica derivada: Vazão média diária efetiva (m³/d)"
        float vazao_media_agua_m3d "Métrica derivada: Vazão média diária de água (m³/d)"
        float corte_agua_bsw_pct "Métrica derivada: Razão de água no líquido BSW (%)"
        float razao_agua_oleo_wor "Métrica derivada: Water-Oil Ratio (WOR)"
        float taxa_queima_gas_flare_pct "Métrica derivada: Flare Ratio de gás (%)"
    }
```

---

## 3. ESPECIFICAÇÃO FORMAL DO GRÃO (GRAIN) E DA TIPOLOGIA DAS MÉTRICAS

### 3.1 Definição do Grão
O grão da tabela fato `fato_producao_mensal` é estritamente **uma medição mensal consolidada de produção fiscalizada para cada poço físico produtor em um determinado mês civil**.
- **Chave Composta Lógica:** `(sk_tempo, sk_poco)` assegura a unicidade factual.
- **Dimensionalidade Múltipla:** Cada fato aponta univocamente para a instalação de superfície que recebe o fluido e para o campo geológico concessionado.

### 3.2 Tipologia das Métricas do Esquema Estrela
1. **Métricas Totalmente Aditivas:**
   - Podem ser somadas ao longo de qualquer dimensão (tempo, campo, bacia, operador).
   - *Exemplos:* `volume_oleo_m3`, `volume_oleo_bbl`, `volume_agua_m3`, `volume_gas_total_Mm3`, `volume_gas_queima_Mm3`.
2. **Métricas Semi-Aditivas:**
   - Não podem ser somadas livremente na dimensão tempo, mas podem ser agregadas por média ou máximo.
   - *Exemplo:* `tempo_producao_dias` (somar dias ao longo de 12 meses resulta em dias-poço, mas a métrica temporal média de disponibilidade exige agregação ponderada).
3. **Métricas Não Aditivas / Derivadas (Intensivas):**
   - Não admitem operação direta de `SUM()` sob pena de produzir distorções matemáticas graves (falácia da média das médias). Devem ser sempre recalculadas a partir das métricas aditivas fundamentais.
   - *Corte de Água (BSW %):* $\text{BSW} = \frac{\sum \text{volume\_agua\_m3}}{\sum (\text{volume\_oleo\_m3} + \text{volume\_agua\_m3})} \times 100$
   - *Taxa de Queima de Gás (Flare %):* $\text{Flare Ratio} = \frac{\sum \text{volume\_gas\_queima\_Mm3}}{\sum \text{volume\_gas\_total\_Mm3}} \times 100$
   - *Vazão Média Diária Efetiva:* $\text{Vazão} = \frac{\sum \text{volume\_oleo\_m3}}{\sum \text{tempo\_producao\_dias}}$

---

## 4. ESTRATÉGIA DE PARTICIONAMENTO E OTIMIZAÇÃO NO LAKEHOUSE
Para viabilizar consultas em sub-segundos no Databricks Delta Lake:
- **Particionamento:** A tabela fato é particionada por `sk_tempo` (ou `ano` / `mes`).
- **Z-Ordering:** Aplicação de `OPTIMIZE fato_producao_mensal ZORDER BY (sk_poco, sk_instalacao)`, garantindo localidade física de dados para filtros frequentes de poço e plataforma.
