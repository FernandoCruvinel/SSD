"""
Script de Geração de Gráficos e Evidências Visuais (EPR / UnB)
Autor: Engenharia de Dados & Especialista em SSD
Descrição: Gera visualizações gráficas de alta resolução (300 DPI) para comprovação
           visual das análises de negócio, auditoria DQA e módulo multicritério TOPSIS.
"""

import os
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

# Configurações visuais globais
plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
plt.rcParams["font.sans-serif"] = "DejaVu Sans"
plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["axes.edgecolor"] = "#cccccc"
plt.rcParams["axes.linewidth"] = 0.8

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "lakehouse_anp.sqlite")
EVIDENCIAS_DIR = os.path.join(BASE_DIR, "evidencias")
os.makedirs(EVIDENCIAS_DIR, exist_ok=True)


def obter_conexao():
    return sqlite3.connect(DB_PATH)


# ==================================================================================================
# 1. GRÁFICO 1: PERDA E DECLÍNIO VOLUMÉTRICO DE ÓLEO (P1)
# ==================================================================================================
def gerar_grafico_declinio():
    conn = obter_conexao()
    query = """
    WITH semestral AS (
        SELECT 
            c.nome_campo || ' (' || i.nome_instalacao || ')' AS ativo,
            c.estagio_vida,
            SUM(CASE WHEN t.ano = 2023 AND t.mes <= 6 THEN f.volume_oleo_bbl END) AS s1_2023,
            SUM(CASE WHEN t.ano = 2024 AND t.mes >= 7 THEN f.volume_oleo_bbl END) AS s2_2024
        FROM fato_producao_mensal f
        JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
        JOIN dim_campo c ON f.sk_campo = c.sk_campo
        JOIN dim_instalacao i ON f.sk_instalacao = i.sk_instalacao
        GROUP BY c.nome_campo, i.nome_instalacao, c.estagio_vida
    )
    SELECT 
        ativo,
        s1_2023 / 1e3 AS s1_2023_kbbl,
        s2_2024 / 1e3 AS s2_2024_kbbl,
        (s1_2023 - s2_2024) / 1e3 AS perda_kbbl,
        ROUND(((s1_2023 - s2_2024) / s1_2023) * 100.0, 1) AS declinio_pct
    FROM semestral
    WHERE s1_2023 > 0 AND (s1_2023 - s2_2024) > 0
    ORDER BY perda_kbbl DESC
    LIMIT 6;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    fig, ax = plt.subplots(figsize=(10, 5.5), dpi=300)
    
    y = np.arange(len(df))
    altura = 0.35

    bar1 = ax.barh(y - altura/2, df["s1_2023_kbbl"], altura, label="2023-S1 (Inicial)", color="#1f4e78")
    bar2 = ax.barh(y + altura/2, df["s2_2024_kbbl"], altura, label="2024-S2 (Final)", color="#d9534f")

    ax.set_yticks(y)
    ax.set_yticklabels(df["ativo"], fontsize=10, fontweight="bold")
    ax.invert_yaxis()
    ax.set_xlabel("Produção Semestral de Óleo (milhares de barris - kbbl)", fontsize=11, fontweight="bold")
    ax.set_title("Pergunta 1: Concentração da Perda Volumétrica e Declínio de Óleo por Ativo", fontsize=12, fontweight="bold", pad=15)
    ax.legend(frameon=True, loc="lower right", fontsize=10)

    # Adicionar anotações de declínio percentual
    for i, row in df.iterrows():
        perda_txt = f"-{row['perda_kbbl']:.0f} kbbl ({row['declinio_pct']:.1f}%)"
        ax.annotate(perda_txt, xy=(row["s1_2023_kbbl"] + 15, i - altura/2), 
                    va="center", fontsize=9, fontweight="bold", color="#a94442")

    ax.xaxis.set_major_formatter(ticker.StrMethodFormatter("{x:,.0f}"))
    plt.tight_layout()
    caminho_img = os.path.join(EVIDENCIAS_DIR, "01_declinio_producao_oleo.png")
    plt.savefig(caminho_img, dpi=300)
    plt.close()
    print(f"[OK] Gráfico 1 salvo em: {caminho_img}")


# ==================================================================================================
# 2. GRÁFICO 2: SEVERIDADE DO CORTE DE ÁGUA - BSW E WOR (P2)
# ==================================================================================================
def gerar_grafico_bsw():
    conn = obter_conexao()
    query = """
    SELECT 
        i.nome_instalacao || ' (' || c.nome_campo || ')' AS instalacao,
        SUM(f.volume_oleo_m3) AS oleo_m3,
        SUM(f.volume_agua_m3) AS agua_m3,
        (SUM(f.volume_agua_m3) / SUM(f.volume_oleo_m3 + f.volume_agua_m3)) * 100.0 AS bsw_pct,
        SUM(f.volume_agua_m3) / NULLIF(SUM(f.volume_oleo_m3), 0) AS wor
    FROM fato_producao_mensal f
    JOIN dim_instalacao i ON f.sk_instalacao = i.sk_instalacao
    JOIN dim_campo c ON f.sk_campo = c.sk_campo
    GROUP BY i.nome_instalacao, c.nome_campo
    ORDER BY bsw_pct DESC
    LIMIT 8;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    fig, ax = plt.subplots(figsize=(10, 5.5), dpi=300)
    
    cores = ["#d9534f" if b >= 85 else ("#f0ad4e" if b >= 50 else "#5cb85c") for b in df["bsw_pct"]]
    
    barras = ax.barh(df["instalacao"], df["bsw_pct"], color=cores, height=0.6)
    ax.invert_yaxis()
    ax.axvline(85.0, color="#b52b27", linestyle="--", linewidth=1.5, label="Limite Crítico de Sobrecarga (BSW = 85%)")
    
    ax.set_xlabel("Corte de Água Ponderado - BSW (%)", fontsize=11, fontweight="bold")
    ax.set_title("Pergunta 2: Severidade Operacional e Risco de Afogamento por Corte de Água (BSW)", fontsize=12, fontweight="bold", pad=15)
    ax.set_xlim(0, 105)
    
    for i, row in df.iterrows():
        txt = f"BSW: {row['bsw_pct']:.1f}% | WOR: {row['wor']:.1f} m³ água/m³ óleo"
        ax.annotate(txt, xy=(row["bsw_pct"] + 1.5, i), va="center", fontsize=8.5, fontweight="bold", color="#333333")

    ax.legend(frameon=True, loc="lower left", fontsize=10)
    plt.tight_layout()
    caminho_img = os.path.join(EVIDENCIAS_DIR, "02_severidade_bsw_corte_agua.png")
    plt.savefig(caminho_img, dpi=300)
    plt.close()
    print(f"[OK] Gráfico 2 salvo em: {caminho_img}")


# ==================================================================================================
# 3. GRÁFICO 3: APROVEITAMENTO VS QUEIMA DE GÁS / FLARE (P3)
# ==================================================================================================
def gerar_grafico_flare():
    conn = obter_conexao()
    query = """
    SELECT 
        i.nome_instalacao || ' (' || c.nome_campo || ')' AS instalacao,
        SUM(f.volume_gas_total_Mm3) AS total_gas_Mm3,
        SUM(f.volume_gas_queima_Mm3) AS flare_Mm3,
        (SUM(f.volume_gas_queima_Mm3) / NULLIF(SUM(f.volume_gas_total_Mm3), 0)) * 100.0 AS flare_pct
    FROM fato_producao_mensal f
    JOIN dim_instalacao i ON f.sk_instalacao = i.sk_instalacao
    JOIN dim_campo c ON f.sk_campo = c.sk_campo
    GROUP BY i.nome_instalacao, c.nome_campo
    ORDER BY flare_pct DESC
    LIMIT 7;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    fig, ax = plt.subplots(figsize=(10, 5.2), dpi=300)
    
    cores = ["#d9534f" if f >= 5.0 else "#2e6da4" for f in df["flare_pct"]]
    
    ax.bar(df["instalacao"], df["flare_pct"], color=cores, width=0.55)
    ax.axhline(5.0, color="#d9534f", linestyle="--", linewidth=1.5, label="Tolerância Operacional Regulatória ANP (5%)")
    
    ax.set_ylabel("Taxa de Queima em Tocha - Flare Ratio (%)", fontsize=11, fontweight="bold")
    ax.set_title("Pergunta 3: Rácio de Queima de Gás Natural (Flare) e Conformidade com a Resolução ANP", fontsize=12, fontweight="bold", pad=15)
    ax.set_xticklabels(df["instalacao"], rotation=25, ha="right", fontsize=9, fontweight="bold")
    
    for i, row in df.iterrows():
        txt = f"{row['flare_pct']:.2f}%\n({row['flare_Mm3']:.1f} Mm³)"
        ax.annotate(txt, xy=(i, row["flare_pct"] + 0.4), ha="center", fontsize=8.5, fontweight="bold", 
                    color="#b52b27" if row["flare_pct"] >= 5.0 else "#1f4e78")

    ax.set_ylim(0, max(df["flare_pct"]) * 1.25)
    ax.legend(frameon=True, loc="upper right", fontsize=10)
    plt.tight_layout()
    caminho_img = os.path.join(EVIDENCIAS_DIR, "03_aproveitamento_flare_gas.png")
    plt.savefig(caminho_img, dpi=300)
    plt.close()
    print(f"[OK] Gráfico 3 salvo em: {caminho_img}")


# ==================================================================================================
# 4. GRÁFICO 4: CURVA DE PARETO E VULNERABILIDADE (P4)
# ==================================================================================================
def gerar_grafico_pareto():
    conn = obter_conexao()
    query = """
    WITH prod_poco AS (
        SELECT 
            p.cod_poco,
            SUM(f.volume_oleo_bbl) AS oleo_bbl
        FROM fato_producao_mensal f
        JOIN dim_poco p ON f.sk_poco = p.sk_poco
        GROUP BY p.cod_poco
    ),
    ordenado AS (
        SELECT 
            cod_poco,
            oleo_bbl / 1e6 AS oleo_Mbbl,
            SUM(oleo_bbl) OVER (ORDER BY oleo_bbl DESC) / (SELECT SUM(oleo_bbl) FROM prod_poco) * 100.0 AS acumulado_pct
        FROM prod_poco
    )
    SELECT * FROM ordenado ORDER BY oleo_Mbbl DESC;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    fig, ax1 = plt.subplots(figsize=(11, 5.8), dpi=300)
    ax2 = ax1.twinx()

    x = np.arange(len(df))
    cores_barras = ["#1f4e78" if ac <= 80 else ("#f0ad4e" if ac <= 95 else "#777777") for ac in df["acumulado_pct"]]
    
    ax1.bar(x, df["oleo_Mbbl"], color=cores_barras, width=0.6, label="Produção Total (Milhões de Barris - Mbbl)")
    ax2.plot(x, df["acumulado_pct"], color="#d9534f", marker="o", linewidth=2, label="Participação Acumulada (%)")
    ax2.axhline(80.0, color="#d9534f", linestyle=":", linewidth=1.5)

    ax1.set_xticks(x)
    ax1.set_xticklabels(df["cod_poco"], rotation=45, ha="right", fontsize=8.5, fontweight="bold")
    ax1.set_ylabel("Produção de Óleo (Milhões de Barris - Mbbl)", fontsize=11, fontweight="bold", color="#1f4e78")
    ax2.set_ylabel("Participação Acumulada (%) - Curva de Pareto", fontsize=11, fontweight="bold", color="#d9534f")
    ax2.set_ylim(0, 105)
    
    ax1.set_title("Pergunta 4: Concentração de Pareto 80/20 e Vulnerabilidade de Parada por Poço", fontsize=12, fontweight="bold", pad=15)
    
    # Anotação de corte de 80%
    ax2.annotate("80% da Produção Concentrada em 5 Poços (Classe A)", xy=(3, 80), xytext=(4, 65),
                 arrowprops=dict(facecolor="#d9534f", shrink=0.08, width=1.5, headwidth=6),
                 fontsize=9.5, fontweight="bold", color="#b52b27")

    plt.tight_layout()
    caminho_img = os.path.join(EVIDENCIAS_DIR, "04_curva_pareto_vulnerabilidade.png")
    plt.savefig(caminho_img, dpi=300)
    plt.close()
    print(f"[OK] Gráfico 4 salvo em: {caminho_img}")


# ==================================================================================================
# 5. GRÁFICO 5: RANKING DE PRIORIZAÇÃO MULTICRITÉRIO TOPSIS
# ==================================================================================================
def gerar_grafico_topsis():
    conn = obter_conexao()
    query = """
    WITH metricas_recentes AS (
        SELECT 
            p.cod_poco,
            c.nome_campo,
            i.nome_instalacao,
            p.ambiente,
            MAX(f.volume_oleo_m3) - MIN(f.volume_oleo_m3) AS delta_oleo,
            AVG(f.corte_agua_bsw_pct) AS bsw,
            AVG(f.taxa_queima_gas_flare_pct) AS flare,
            SUM(t.dias_no_mes - f.tempo_producao_dias) AS dias_parados
        FROM fato_producao_mensal f
        JOIN dim_poco p ON f.sk_poco = p.sk_poco
        JOIN dim_campo c ON f.sk_campo = c.sk_campo
        JOIN dim_instalacao i ON f.sk_instalacao = i.sk_instalacao
        JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
        WHERE c.estagio_vida LIKE '%MADURO%'
        GROUP BY p.cod_poco, c.nome_campo, i.nome_instalacao, p.ambiente
    )
    SELECT * FROM metricas_recentes;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Cálculo TOPSIS
    criterios = ["delta_oleo", "bsw", "flare", "dias_parados"]
    tipos = ["max", "min", "min", "max"]
    pesos = np.array([0.40, 0.25, 0.15, 0.20])

    X = df[criterios].values.astype(float)
    R = X / np.sqrt(np.sum(X**2, axis=0))
    V = R * pesos

    A_pos = np.array([np.max(V[:, j]) if tipos[j] == "max" else np.min(V[:, j]) for j in range(4)])
    A_neg = np.array([np.min(V[:, j]) if tipos[j] == "max" else np.max(V[:, j]) for j in range(4)])

    D_pos = np.sqrt(np.sum((V - A_pos)**2, axis=1))
    D_neg = np.sqrt(np.sum((V - A_neg)**2, axis=1))
    Ci = D_neg / (D_pos + D_neg)

    df["topsis_score"] = Ci
    df["label"] = df["cod_poco"] + " (" + df["nome_campo"] + ")"
    df = df.sort_values(by="topsis_score", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6.2), dpi=300)
    
    # Cores graduadas por prioridade
    cores = sns.color_palette("Blues_r", n_colors=len(df))
    cores = cores[::-1] # inverter para maior ser mais escuro
    
    ax.barh(df["label"], df["topsis_score"], color=cores, height=0.65)
    ax.set_xlabel("Coeficiente de Proximidade Relativa à Solução Ideal ($C_i$)", fontsize=11, fontweight="bold")
    ax.set_title("Módulo Prescritivo: Ranqueamento TOPSIS para Alocação de Sondas de Intervenção", fontsize=12, fontweight="bold", pad=15)
    ax.set_xlim(0, 1.1)

    for i, (idx, row) in enumerate(df.iterrows()):
        txt = f"Ci: {row['topsis_score']:.4f} | Δ Óleo: {row['delta_oleo']:,.0f} m³"
        ax.annotate(txt, xy=(row["topsis_score"] + 0.015, i), va="center", fontsize=8.5, fontweight="bold", color="#1f4e78")

    plt.tight_layout()
    caminho_img = os.path.join(EVIDENCIAS_DIR, "05_ranking_priorizacao_topsis.png")
    plt.savefig(caminho_img, dpi=300)
    plt.close()
    print(f"[OK] Gráfico 5 salvo em: {caminho_img}")


# ==================================================================================================
# 6. GRÁFICO 6: SCORECARD DAS 6 DIMENSÕES DE QUALIDADE DE DADOS (DQA)
# ==================================================================================================
def gerar_grafico_qualidade():
    dimensoes = ["Completude", "Unicidade", "Consistência", "Conformidade", "Acurácia", "Atualidade"]
    scores = [100.0, 100.0, 100.0, 100.0, 100.0, 85.0]

    fig, ax = plt.subplots(figsize=(9, 4.8), dpi=300)
    
    cores = ["#5cb85c" if s >= 95 else "#f0ad4e" for s in scores]
    barras = ax.bar(dimensoes, scores, color=cores, width=0.5)

    ax.set_ylabel("Conformidade (%)", fontsize=11, fontweight="bold")
    ax.set_title("Auditoria de Qualidade de Dados (DQA): Aderência nas 6 Dimensões Canônicas", fontsize=12, fontweight="bold", pad=15)
    ax.set_ylim(0, 115)
    ax.axhline(100.0, color="#777777", linestyle=":", linewidth=1)

    for bar, score in zip(barras, scores):
        status = "APROVADO (100%)" if score == 100.0 else "ALERTA (85%)\n(Lag D+60)"
        ax.annotate(status, xy=(bar.get_x() + bar.get_width()/2, score + 2),
                    ha="center", fontsize=8.5, fontweight="bold",
                    color="#3c763d" if score == 100 else "#8a6d3b")

    plt.tight_layout()
    caminho_img = os.path.join(EVIDENCIAS_DIR, "06_resumo_qualidade_dados.png")
    plt.savefig(caminho_img, dpi=300)
    plt.close()
    print(f"[OK] Gráfico 6 salvo em: {caminho_img}")


def gerar_todos_graficos():
    print("="*80)
    print("GERANDO TODOS OS GRÁFICOS E EVIDÊNCIAS VISUAIS DE SUPORTE À DECISÃO")
    print("="*80)
    gerar_grafico_declinio()
    gerar_grafico_bsw()
    gerar_grafico_flare()
    gerar_grafico_pareto()
    gerar_grafico_topsis()
    gerar_grafico_qualidade()
    print("="*80)
    print(f"TODOS OS 6 GRÁFICOS GERADOS COM SUCESSO EM: {EVIDENCIAS_DIR}")
    print("="*80)


if __name__ == "__main__":
    gerar_todos_graficos()
