# Databricks notebook source
"""
====================================================================================================
PROJETO MVP - SISTEMAS DE SUPORTE À DECISÃO (EPR / UnB)
PIPELINE ETL: INGESTÃO, LIMPEZA E MODELAGEM DIMENSIONAL (BMP / ANP)
====================================================================================================
Autor: Engenharia de Dados & Especialista em SSD
Docente Responsável: Prof. Dr. André Luiz Marques Serrano
Departamento de Engenharia de Produção - Universidade de Brasília (UnB)

Arquitetura:
    - Padrão Medalhão (Medallion Architecture): Camadas Bronze -> Silver -> Gold
    - Formato de Armazenamento: Delta Lake (em ambiente Databricks) e Parquet / SQLite (local)
    - Modelo Analítico: Esquema Estrela (Star Schema) para Data Lakehouse
====================================================================================================
"""

import os
import sys
import hashlib
import datetime
import pandas as pd
import numpy as np

# Verificação do ambiente de execução (Databricks PySpark vs Local Python)
EM_DATABRICKS = False
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, when, lit, current_timestamp, input_file_name,
        round as spark_round, sha2, concat_ws, coalesce
    )
    # Tenta obter a sessão Spark ativa do cluster Databricks
    spark = SparkSession.getActiveSession()
    if spark is not None:
        EM_DATABRICKS = True
        print("[INFO] Ambiente Databricks com Apache Spark detectado com sucesso.")
except ImportError:
    pass

if not EM_DATABRICKS:
    print("[INFO] Executando em ambiente local. Utilizando motor analítico Pandas/SQLite/Parquet.")


# ==================================================================================================
# 1. CONFIGURAÇÃO DE DIRETÓRIOS E CONSTANTES OPERACIONAIS
# ==================================================================================================
FATOR_CONVERSAO_M3_PARA_BBL = 6.28981077  # Padrão internacional de O&G (1 m³ = 6.28981 barris)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
ARQUIVO_RAW_CSV = os.path.join(DATA_RAW_DIR, "producao_mar_terra_anp_2023_2024.csv")

# Diretórios das camadas do Lakehouse
BRONZE_DIR = os.path.join(DATA_PROCESSED_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_PROCESSED_DIR, "silver")
GOLD_DIR = os.path.join(DATA_PROCESSED_DIR, "gold")
SQLITE_DB_PATH = os.path.join(DATA_PROCESSED_DIR, "lakehouse_anp.sqlite")

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)
os.makedirs(GOLD_DIR, exist_ok=True)


# ==================================================================================================
# 2. FUNÇÕES AUXILIARES DE LINHAGEM E CHAVES SUBSTITUTAS (SURROGATE KEYS)
# ==================================================================================================
def gerar_surrogate_key(*valores) -> int:
    """Gera chave substituta inteira de 32-bit determinística a partir de hash SHA-256."""
    texto = "_".join(str(v).strip().upper() for v in valores)
    h = hashlib.sha256(texto.encode("utf-8")).hexdigest()
    return int(h[:8], 16)


# ==================================================================================================
# 3. CAMADA BRONZE (RAW DATA INGESTION & AUDIT METADATA)
# ==================================================================================================
def executar_camada_bronze(caminho_csv: str) -> pd.DataFrame:
    """
    Ingestão dos dados brutos com preservação da fidelidade dos registros de origem,
    adicionando metadados de governança e rastreabilidade.
    """
    print("\n" + "="*80)
    print(">>> EXECUTANDO CAMADA BRONZE: Ingestão de Dados Brutos da ANP")
    print("="*80)
    
    if not os.path.exists(caminho_csv):
        raise FileNotFoundError(f"Arquivo de dados brutos não localizado em: {caminho_csv}")

    df_raw = pd.read_csv(caminho_csv, sep=";", decimal=".", encoding="utf-8")
    
    # Inclusão de metadados técnicos de linhagem
    timestamp_ingestao = datetime.datetime.now().isoformat()
    nome_arquivo = os.path.basename(caminho_csv)
    
    df_bronze = df_raw.copy()
    df_bronze["_ingestao_timestamp"] = timestamp_ingestao
    df_bronze["_arquivo_fonte"] = nome_arquivo
    
    # Geração de hash MD5 por registro para verificação de integridade
    df_bronze["_registro_hash"] = df_bronze.apply(
        lambda row: hashlib.md5(str(row.values).encode("utf-8")).hexdigest(), axis=1
    )
    
    caminho_bronze_parquet = os.path.join(BRONZE_DIR, "bronze_producao_bmp.parquet")
    df_bronze.to_parquet(caminho_bronze_parquet, index=False)
    
    print(f"[BRONZE CONCLUÍDO] Registros brutos ingeridos: {len(df_bronze):,}")
    print(f"[BRONZE CONCLUÍDO] Persistido em: {caminho_bronze_parquet}")
    return df_bronze


# ==================================================================================================
# 4. CAMADA SILVER (DATA CLEANSING, ENRICHMENT & DEDUPLICATION)
# ==================================================================================================
def executar_camada_silver(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """
    Higienização, padronização semântica, tratamento de nulos e deduplicação
    conforme regras de negócio da ANP e Engenharia de Produção.
    """
    print("\n" + "="*80)
    print(">>> EXECUTANDO CAMADA SILVER: Limpeza, Tipagem e Padronização")
    print("="*80)
    
    df = df_bronze.copy()
    
    # 1. Padronização de nomes de colunas para snake_case
    df.columns = [c.strip().lower() for c in df.columns]
    
    # 2. Padronização de textos categóricos (maiúsculas e sem espaços residuais)
    colunas_texto = [
        "estado", "bacia", "campo", "poco", "nome_poco", 
        "ambiente", "instalacao", "tipo_instalacao", "operador"
    ]
    for col_txt in colunas_texto:
        if col_txt in df.columns:
            df[col_txt] = df[col_txt].astype(str).str.strip().str.upper()
            
    # 3. Conversão de variáveis temporais e numéricas
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype(int)
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype(int)
    
    colunas_volumes = [
        "producao_oleo_m3", "producao_gas_associado_mm3", 
        "producao_gas_nao_associado_mm3", "producao_gas_total_mm3",
        "queima_gas_mm3", "injecao_gas_mm3", "producao_agua_m3", "tempo_producao_dias"
    ]
    for col_vol in colunas_volumes:
        if col_vol in df.columns:
            df[col_vol] = pd.to_numeric(df[col_vol], errors="coerce").fillna(0.0)
            
    # 4. Regra de Negócio: Tratamento de nulos em poços parados
    # Se tempo de produção = 0, forçar volumes como zero estrito
    mascara_parado = df["tempo_producao_dias"] == 0
    for col_vol in ["producao_oleo_m3", "producao_agua_m3", "producao_gas_total_mm3", "queima_gas_mm3"]:
        df.loc[mascara_parado, col_vol] = 0.0
        
    # 5. Deduplicação determinística baseada na chave natural (poco, ano, mes)
    n_antes = len(df)
    df = df.drop_duplicates(subset=["poco", "ano", "mes"], keep="last")
    duplicatas_removidas = n_antes - len(df)
    
    # 6. Criação de coluna de competência contábil / analítica (ano_mes)
    df["ano_mes"] = df["ano"].astype(str) + "-" + df["mes"].apply(lambda m: f"{m:02d}")
    
    caminho_silver_parquet = os.path.join(SILVER_DIR, "silver_producao_bmp.parquet")
    df.to_parquet(caminho_silver_parquet, index=False)
    
    print(f"[SILVER CONCLUÍDO] Registros higienizados: {len(df):,} (Duplicatas eliminadas: {duplicatas_removidas})")
    print(f"[SILVER CONCLUÍDO] Persistido em: {caminho_silver_parquet}")
    return df


# ==================================================================================================
# 5. CAMADA GOLD (DIMENSIONAL MODELING - STAR SCHEMA & BUSINESS METRICS)
# ==================================================================================================
def executar_camada_gold(df_silver: pd.DataFrame) -> dict:
    """
    Construção do Esquema Estrela (Star Schema):
    Dimensões: dim_tempo, dim_poco, dim_instalacao, dim_campo
    Fato Central: fato_producao_mensal com cálculo de KPIs avançados de O&G.
    """
    print("\n" + "="*80)
    print(">>> EXECUTANDO CAMADA GOLD: Modelagem Dimensional em Esquema Estrela")
    print("="*80)
    
    # ----------------------------------------------------------------------------------------------
    # A. Dimensão Tempo (dim_tempo)
    # ----------------------------------------------------------------------------------------------
    tempos_unicos = df_silver[["ano", "mes", "ano_mes"]].drop_duplicates().sort_values(by=["ano", "mes"])
    dim_tempo_linhas = []
    meses_pt = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    
    for _, row in tempos_unicos.iterrows():
        a = int(row["ano"])
        m = int(row["mes"])
        dias_no_mes = 31 if m in [1, 3, 5, 7, 8, 10, 12] else (28 if (m == 2 and a % 4 != 0) else (29 if m == 2 else 30))
        trimestre = (m - 1) // 3 + 1
        semestre = 1 if m <= 6 else 2
        sk_tempo = a * 100 + m
        
        dim_tempo_linhas.append({
            "sk_tempo": sk_tempo,
            "ano_mes": row["ano_mes"],
            "ano": a,
            "mes": m,
            "nome_mes": meses_pt[m],
            "trimestre": f"{trimestre}T{a}",
            "semestre": f"{semestre}S{a}",
            "dias_no_mes": dias_no_mes
        })
    df_dim_tempo = pd.DataFrame(dim_tempo_linhas)
    
    # ----------------------------------------------------------------------------------------------
    # B. Dimensão Poço (dim_poco)
    # ----------------------------------------------------------------------------------------------
    pocos_unicos = df_silver[["poco", "nome_poco", "operador", "ambiente"]].drop_duplicates()
    df_dim_poco = pocos_unicos.copy().rename(columns={"poco": "cod_poco"})
    df_dim_poco["sk_poco"] = df_dim_poco["cod_poco"].apply(lambda p: gerar_surrogate_key("POCO", p))
    df_dim_poco["status_operacional"] = "ATIVO"
    df_dim_poco = df_dim_poco[["sk_poco", "cod_poco", "nome_poco", "operador", "ambiente", "status_operacional"]]
    
    # ----------------------------------------------------------------------------------------------
    # C. Dimensão Instalação (dim_instalacao)
    # ----------------------------------------------------------------------------------------------
    inst_unicas = df_silver[["instalacao", "tipo_instalacao", "bacia", "estado"]].drop_duplicates()
    df_dim_inst = inst_unicas.copy().rename(columns={"instalacao": "nome_instalacao"})
    df_dim_inst["sk_instalacao"] = df_dim_inst["nome_instalacao"].apply(lambda i: gerar_surrogate_key("INSTALACAO", i))
    df_dim_inst = df_dim_inst[["sk_instalacao", "nome_instalacao", "tipo_instalacao", "bacia", "estado"]]
    
    # ----------------------------------------------------------------------------------------------
    # D. Dimensão Campo (dim_campo)
    # ----------------------------------------------------------------------------------------------
    campos_unicos = df_silver[["campo", "bacia", "estado", "ambiente"]].drop_duplicates()
    df_dim_campo = campos_unicos.copy().rename(columns={"campo": "nome_campo"})
    df_dim_campo["sk_campo"] = df_dim_campo["nome_campo"].apply(lambda c: gerar_surrogate_key("CAMPO", c))
    
    def classificar_maturidade(row):
        if row["nome_campo"] in ["BUZIOS", "LULA"]:
            return "PRÉ-SAL / EXPANSÃO"
        elif row["ambiente"] == "MAR":
            return "OFFSHORE MADURO"
        else:
            return "ONSHORE MADURO"
            
    df_dim_campo["estagio_vida"] = df_dim_campo.apply(classificar_maturidade, axis=1)
    df_dim_campo = df_dim_campo[["sk_campo", "nome_campo", "bacia", "estado", "estagio_vida"]]
    
    # ----------------------------------------------------------------------------------------------
    # E. Tabela Fato Central: fato_producao_mensal
    # ----------------------------------------------------------------------------------------------
    df_fato = df_silver.copy()
    
    # Mapeamento das chaves substitutas
    df_fato["sk_tempo"] = df_fato["ano"] * 100 + df_fato["mes"]
    df_fato["sk_poco"] = df_fato["poco"].apply(lambda p: gerar_surrogate_key("POCO", p))
    df_fato["sk_instalacao"] = df_fato["instalacao"].apply(lambda i: gerar_surrogate_key("INSTALACAO", i))
    df_fato["sk_campo"] = df_fato["campo"].apply(lambda c: gerar_surrogate_key("CAMPO", c))
    
    # Renomeação padronizada de métricas
    df_fato = df_fato.rename(columns={
        "producao_oleo_m3": "volume_oleo_m3",
        "producao_gas_associado_mm3": "volume_gas_associado_Mm3",
        "producao_gas_nao_associado_mm3": "volume_gas_nao_associado_Mm3",
        "producao_gas_total_mm3": "volume_gas_total_Mm3",
        "queima_gas_mm3": "volume_gas_queima_Mm3",
        "injecao_gas_mm3": "volume_gas_injecao_Mm3",
        "producao_agua_m3": "volume_agua_m3"
    })
    
    # Conversões e Cálculos Avançados de Engenharia
    # 1. Volume de óleo em barris
    df_fato["volume_oleo_bbl"] = (df_fato["volume_oleo_m3"] * FATOR_CONVERSAO_M3_PARA_BBL).round(2)
    
    # 2. Vazão média diária efetiva (considerando apenas os dias em produção)
    df_fato["vazao_media_oleo_m3d"] = np.where(
        df_fato["tempo_producao_dias"] > 0,
        (df_fato["volume_oleo_m3"] / df_fato["tempo_producao_dias"]).round(2),
        0.0
    )
    df_fato["vazao_media_agua_m3d"] = np.where(
        df_fato["tempo_producao_dias"] > 0,
        (df_fato["volume_agua_m3"] / df_fato["tempo_producao_dias"]).round(2),
        0.0
    )
    
    # 3. Corte de Água / BSW (%) = Agua / (Oleo + Agua) * 100
    liquido_total = df_fato["volume_oleo_m3"] + df_fato["volume_agua_m3"]
    df_fato["corte_agua_bsw_pct"] = np.where(
        liquido_total > 0,
        ((df_fato["volume_agua_m3"] / liquido_total) * 100.0).round(2),
        0.0
    )
    
    # 4. Razão Água-Óleo (WOR = Water-Oil Ratio) = Agua / Oleo
    df_fato["razao_agua_oleo_wor"] = np.where(
        df_fato["volume_oleo_m3"] > 0,
        (df_fato["volume_agua_m3"] / df_fato["volume_oleo_m3"]).round(2),
        np.where(df_fato["volume_agua_m3"] > 0, 999.99, 0.0)
    )
    
    # 5. Taxa de Queima de Gás / Flare Ratio (%) = Gas Queima / Gas Total * 100
    df_fato["taxa_queima_gas_flare_pct"] = np.where(
        df_fato["volume_gas_total_Mm3"] > 0,
        ((df_fato["volume_gas_queima_Mm3"] / df_fato["volume_gas_total_Mm3"]) * 100.0).round(2),
        0.0
    )
    
    # Seleção estrita e ordenação das colunas da tabela fato
    colunas_fato = [
        "sk_tempo", "sk_poco", "sk_instalacao", "sk_campo",
        "volume_oleo_m3", "volume_oleo_bbl", "volume_gas_associado_Mm3",
        "volume_gas_nao_associado_Mm3", "volume_gas_total_Mm3",
        "volume_gas_queima_Mm3", "volume_gas_injecao_Mm3", "volume_agua_m3",
        "tempo_producao_dias", "vazao_media_oleo_m3d", "vazao_media_agua_m3d",
        "corte_agua_bsw_pct", "razao_agua_oleo_wor", "taxa_queima_gas_flare_pct"
    ]
    df_fato_final = df_fato[colunas_fato].copy()
    
    # ----------------------------------------------------------------------------------------------
    # Persistência Física (Parquet colunar particionado + Banco Relacional SQLite)
    # ----------------------------------------------------------------------------------------------
    # 1. Salvar arquivos Parquet no Gold
    df_dim_tempo.to_parquet(os.path.join(GOLD_DIR, "dim_tempo.parquet"), index=False)
    df_dim_poco.to_parquet(os.path.join(GOLD_DIR, "dim_poco.parquet"), index=False)
    df_dim_inst.to_parquet(os.path.join(GOLD_DIR, "dim_instalacao.parquet"), index=False)
    df_dim_campo.to_parquet(os.path.join(GOLD_DIR, "dim_campo.parquet"), index=False)
    df_fato_final.to_parquet(os.path.join(GOLD_DIR, "fato_producao_mensal.parquet"), index=False)
    
    # 2. Persistir em banco relacional local SQLite para suporte pleno a SQL
    import sqlite3
    conn = sqlite3.connect(SQLITE_DB_PATH)
    df_dim_tempo.to_sql("dim_tempo", conn, if_exists="replace", index=False)
    df_dim_poco.to_sql("dim_poco", conn, if_exists="replace", index=False)
    df_dim_inst.to_sql("dim_instalacao", conn, if_exists="replace", index=False)
    df_dim_campo.to_sql("dim_campo", conn, if_exists="replace", index=False)
    df_fato_final.to_sql("fato_producao_mensal", conn, if_exists="replace", index=False)
    
    # Criar índices analíticos para otimização de performance
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fato_tempo ON fato_producao_mensal(sk_tempo);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fato_poco ON fato_producao_mensal(sk_poco);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fato_inst ON fato_producao_mensal(sk_instalacao);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fato_campo ON fato_producao_mensal(sk_campo);")
    conn.commit()
    conn.close()
    
    print(f"[GOLD CONCLUÍDO] Esquema Estrela construído com sucesso.")
    print(f"  - dim_tempo: {len(df_dim_tempo):,} registros")
    print(f"  - dim_poco: {len(df_dim_poco):,} registros")
    print(f"  - dim_instalacao: {len(df_dim_inst):,} registros")
    print(f"  - dim_campo: {len(df_dim_campo):,} registros")
    print(f"  - fato_producao_mensal: {len(df_fato_final):,} medições mensais de produção")
    print(f"[GOLD CONCLUÍDO] Persistência em Parquet e SQLite realizada em: {SQLITE_DB_PATH}")
    
    return {
        "dim_tempo": df_dim_tempo,
        "dim_poco": df_dim_poco,
        "dim_instalacao": df_dim_inst,
        "dim_campo": df_dim_campo,
        "fato_producao_mensal": df_fato_final
    }


# ==================================================================================================
# 6. PIPELINE ORCHESTRATOR
# ==================================================================================================
def executar_pipeline_completo():
    inicio = datetime.datetime.now()
    print("="*80)
    print("INICIANDO EXECUÇÃO INTEGRADA DO PIPELINE ETL DE PRODUÇÃO (ANP / SSD)")
    print(f"Data e Hora de Início: {inicio.strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*80)
    
    # Passo 1: Bronze
    df_bronze = executar_camada_bronze(ARQUIVO_RAW_CSV)
    
    # Passo 2: Silver
    df_silver = executar_camada_silver(df_bronze)
    
    # Passo 3: Gold
    tabelas_gold = executar_camada_gold(df_silver)
    
    fim = datetime.datetime.now()
    duracao = (fim - inicio).total_seconds()
    print("="*80)
    print(f"PIPELINE ETL CONCLUÍDO COM SUCESSO EM {duracao:.2f} SEGUNDOS!")
    print("Todos os dados estão devidamente persistidos e prontos para consumo analítico.")
    print("="*80)


if __name__ == "__main__":
    executar_pipeline_completo()
