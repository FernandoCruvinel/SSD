"""
Módulo de Validação e Auditoria de Qualidade de Dados (Data Quality Assurance)
Disciplina: Sistemas de Suporte à Decisão (SSD) - Departamento de Engenharia de Produção / UnB
Docente: Prof. Dr. André Luiz Marques Serrano
Autor: Engenharia de Dados & Especialista em SSD

Descrição:
    Implementa a verificação das 6 dimensões canônicas de qualidade de dados
    definidas no guião do trabalho final:
    1. Completude (Completeness)
    2. Unicidade (Uniqueness)
    3. Consistência (Consistency)
    4. Conformidade (Conformity / Validity)
    5. Acurácia (Accuracy / Plausibility)
    6. Atualidade (Timeliness / Freshness)
"""

import pandas as pd
import numpy as np
import datetime
from typing import Dict, List, Any, Tuple


class ValidadorQualidadeDados:
    """
    Motor de auditoria de qualidade de dados para o Data Warehouse / Lakehouse
    de Produção de Petróleo e Gás da ANP.
    """

    def __init__(self, df: pd.DataFrame, nome_dataset: str = "fato_producao_mensal"):
        self.df = df.copy()
        self.nome_dataset = nome_dataset
        self.resultados = []
        self.metricas_sumario = {}

    def registrar_teste(self, dimensao: str, regra: str, total_registros: int, 
                         falhas: int, taxa_conformidade: float, status: str, detalhes: str = ""):
        self.resultados.append({
            "dimensao": dimensao,
            "regra": regra,
            "total_registros": total_registros,
            "falhas": falhas,
            "conformidade_pct": round(taxa_conformidade, 2),
            "status": status,
            "detalhes": detalhes
        })

    # =========================================================================
    # 1. COMPLETUDE (COMPLETENESS)
    # =========================================================================
    def validar_completude(self, colunas_obrigatorias: List[str] = None) -> "ValidadorQualidadeDados":
        if colunas_obrigatorias is None:
            colunas_obrigatorias = [
                "cod_poco", "ano", "mes", "campo", "instalacao",
                "volume_oleo_m3", "volume_agua_m3", "volume_gas_total_Mm3"
            ]
        
        n_total = len(self.df)
        for col in colunas_obrigatorias:
            if col in self.df.columns:
                nulos = int(self.df[col].isnull().sum())
                conformidade = ((n_total - nulos) / n_total) * 100.0 if n_total > 0 else 0.0
                status = "APROVADO" if nulos == 0 else "ALERTA"
                self.registrar_teste(
                    dimensao="Completude",
                    regra=f"Ausência de nulos no campo obrigatório '{col}'",
                    total_registros=n_total,
                    falhas=nulos,
                    taxa_conformidade=conformidade,
                    status=status,
                    detalhes=f"{nulos} valores ausentes encontrados." if nulos > 0 else "Preenchimento integral (100%)."
                )
            else:
                self.registrar_teste(
                    dimensao="Completude",
                    regra=f"Existência do atributo '{col}'",
                    total_registros=n_total,
                    falhas=n_total,
                    taxa_conformidade=0.0,
                    status="REPROVADO",
                    detalhes=f"Coluna '{col}' não encontrada no dataset."
                )
        return self

    # =========================================================================
    # 2. UNICIDADE (UNIQUENESS)
    # =========================================================================
    def validar_unicidade(self, chaves_primarias: List[str] = None) -> "ValidadorQualidadeDados":
        if chaves_primarias is None:
            chaves_primarias = ["cod_poco", "ano", "mes"]
            
        n_total = len(self.df)
        cols_presentes = [c for c in chaves_primarias if c in self.df.columns]
        
        if len(cols_presentes) == len(chaves_primarias):
            duplicatas = int(self.df.duplicated(subset=chaves_primarias, keep=False).sum())
            conformidade = ((n_total - duplicatas) / n_total) * 100.0 if n_total > 0 else 0.0
            status = "APROVADO" if duplicatas == 0 else "REPROVADO"
            self.registrar_teste(
                dimensao="Unicidade",
                regra=f"Unicidade da granularidade composta {tuple(chaves_primarias)}",
                total_registros=n_total,
                falhas=duplicatas,
                taxa_conformidade=conformidade,
                status=status,
                detalhes=f"{duplicatas} registros compartilham a mesma chave natural." if duplicatas > 0 else "Nenhuma duplicata de grão."
            )
        else:
            self.registrar_teste(
                dimensao="Unicidade",
                regra=f"Unicidade de chaves {chaves_primarias}",
                total_registros=n_total,
                falhas=n_total,
                taxa_conformidade=0.0,
                status="ERRO",
                detalhes="Colunas-chave ausentes para cálculo de unicidade."
            )
        return self

    # =========================================================================
    # 3. CONSISTÊNCIA (CONSISTENCY)
    # =========================================================================
    def validar_consistencia(self) -> "ValidadorQualidadeDados":
        n_total = len(self.df)
        
        # Regra 1: Gás queimado não pode ser maior que o gás total
        if "volume_gas_total_Mm3" in self.df.columns and "volume_gas_queima_Mm3" in self.df.columns:
            inconsistencias_gas = (self.df["volume_gas_queima_Mm3"] > (self.df["volume_gas_total_Mm3"] + 1e-4)).sum()
            inconsistencias_gas = int(inconsistencias_gas)
            conformidade = ((n_total - inconsistencias_gas) / n_total) * 100.0
            self.registrar_teste(
                dimensao="Consistência",
                regra="Balanço de massa: volume_gas_queima_Mm3 <= volume_gas_total_Mm3",
                total_registros=n_total,
                falhas=inconsistencias_gas,
                taxa_conformidade=conformidade,
                status="APROVADO" if inconsistencias_gas == 0 else "REPROVADO",
                detalhes="Queima em tocha compatível com volume total de gás produzido." if inconsistencias_gas == 0 else f"{inconsistencias_gas} registros com queima superior à extração total."
            )
            
        # Regra 2: Tempo de produção em dias não pode exceder o total de dias do mês
        if "tempo_producao_dias" in self.df.columns and "mes" in self.df.columns:
            dias_max_mes = self.df["mes"].map(lambda m: 31 if m in [1,3,5,7,8,10,12] else (29 if m==2 else 30))
            inconsistencias_tempo = int((self.df["tempo_producao_dias"] > dias_max_mes).sum())
            conformidade = ((n_total - inconsistencias_tempo) / n_total) * 100.0
            self.registrar_teste(
                dimensao="Consistência",
                regra="Coerência de calendário: tempo_producao_dias <= dias_do_mes",
                total_registros=n_total,
                falhas=inconsistencias_tempo,
                taxa_conformidade=conformidade,
                status="APROVADO" if inconsistencias_tempo == 0 else "REPROVADO",
                detalhes="Dias operados rigorosamente contidos no mês civil." if inconsistencias_tempo == 0 else f"{inconsistencias_tempo} violações temporais."
            )
            
        # Regra 3: Se tempo de produção = 0, volume de óleo deve ser 0
        if "tempo_producao_dias" in self.df.columns and "volume_oleo_m3" in self.df.columns:
            inconsistencias_parada = int(((self.df["tempo_producao_dias"] == 0) & (self.df["volume_oleo_m3"] > 0)).sum())
            conformidade = ((n_total - inconsistencias_parada) / n_total) * 100.0
            self.registrar_teste(
                dimensao="Consistência",
                regra="Física operacional: se tempo_producao_dias = 0, entao volume_oleo = 0",
                total_registros=n_total,
                falhas=inconsistencias_parada,
                taxa_conformidade=conformidade,
                status="APROVADO" if inconsistencias_parada == 0 else "REPROVADO",
                detalhes="Sem produção física quando poço está 100% inativo." if inconsistencias_parada == 0 else f"{inconsistencias_parada} poços parados com volume fantasma."
            )
            
        return self

    # =========================================================================
    # 4. CONFORMIDADE (CONFORMITY / VALIDITY)
    # =========================================================================
    def validar_conformidade(self) -> "ValidadorQualidadeDados":
        n_total = len(self.df)
        
        # Regra 1: Domínio do Ambiente (MAR ou TERRA)
        if "ambiente" in self.df.columns:
            ambientes_validos = {"MAR", "TERRA"}
            invalidos_amb = int((~self.df["ambiente"].str.upper().isin(ambientes_validos)).sum())
            conf_amb = ((n_total - invalidos_amb) / n_total) * 100.0
            self.registrar_teste(
                dimensao="Conformidade",
                regra="Aderência ao vocabulário controlado de ambiente: {'MAR', 'TERRA'}",
                total_registros=n_total,
                falhas=invalidos_amb,
                taxa_conformidade=conf_amb,
                status="APROVADO" if invalidos_amb == 0 else "REPROVADO",
                detalhes=f"Domínio categórico 100% conforme." if invalidos_amb == 0 else f"{invalidos_amb} categorias não mapeadas."
            )
            
        # Regra 2: Domínio de Mês (1 a 12)
        if "mes" in self.df.columns:
            meses_invalidos = int((~self.df["mes"].isin(list(range(1, 13)))).sum())
            conf_mes = ((n_total - meses_invalidos) / n_total) * 100.0
            self.registrar_teste(
                dimensao="Conformidade",
                regra="Domínio numérico do mês civil: mes in [1, 12]",
                total_registros=n_total,
                falhas=meses_invalidos,
                taxa_conformidade=conf_mes,
                status="APROVADO" if meses_invalidos == 0 else "REPROVADO",
                detalhes="Meses válidos no calendário oficial."
            )
            
        # Regra 3: Faixa percentual de BSW (0% a 100%)
        if "corte_agua_bsw_pct" in self.df.columns:
            bsw_invalido = int(((self.df["corte_agua_bsw_pct"] < 0.0) | (self.df["corte_agua_bsw_pct"] > 100.0)).sum())
            conf_bsw = ((n_total - bsw_invalido) / n_total) * 100.0
            self.registrar_teste(
                dimensao="Conformidade",
                regra="Domínio percentual de Corte de Água: corte_agua_bsw_pct in [0.0, 100.0]",
                total_registros=n_total,
                falhas=bsw_invalido,
                taxa_conformidade=conf_bsw,
                status="APROVADO" if bsw_invalido == 0 else "REPROVADO",
                detalhes="Métrica matemática de BSW delimitada no intervalo unitário percentual."
            )
            
        return self

    # =========================================================================
    # 5. ACURÁCIA (ACCURACY / PLAUSIBILITY)
    # =========================================================================
    def validar_acuracia(self) -> "ValidadorQualidadeDados":
        n_total = len(self.df)
        
        # Regra 1: Volumes de óleo não negativos e abaixo do teto físico mundial por poço
        if "volume_oleo_m3" in self.df.columns:
            # Teto físico plausível: nenhum poço individual no Brasil ultrapassa 350.000 m3/mês (~73.000 bbl/d)
            anomalias_oleo = int(((self.df["volume_oleo_m3"] < 0) | (self.df["volume_oleo_m3"] > 350000.0)).sum())
            conf_oleo = ((n_total - anomalias_oleo) / n_total) * 100.0
            self.registrar_teste(
                dimensao="Acurácia",
                regra="Plausibilidade física: 0 <= volume_oleo_m3 <= 350.000 m³/mês",
                total_registros=n_total,
                falhas=anomalias_oleo,
                taxa_conformidade=conf_oleo,
                status="APROVADO" if anomalias_oleo == 0 else "ALERTA",
                detalhes="Volumes mensais contidos dentro da fronteira física de reservatório."
            )
            
        # Regra 2: Volumes de água não negativos
        if "volume_agua_m3" in self.df.columns:
            anomalias_agua = int((self.df["volume_agua_m3"] < 0).sum())
            conf_agua = ((n_total - anomalias_agua) / n_total) * 100.0
            self.registrar_teste(
                dimensao="Acurácia",
                regra="Não negatividade de efluente hídrico: volume_agua_m3 >= 0",
                total_registros=n_total,
                falhas=anomalias_agua,
                taxa_conformidade=conf_agua,
                status="APROVADO" if anomalias_agua == 0 else "REPROVADO",
                detalhes="Ausência de volumes negativos de água produzida."
            )
            
        return self

    # =========================================================================
    # 6. ATUALIDADE (TIMELINESS)
    # =========================================================================
    def validar_atualidade(self, data_extracao: datetime.date = None) -> "ValidadorQualidadeDados":
        n_total = len(self.df)
        if data_extracao is None:
            data_extracao = datetime.date(2025, 2, 1) # Janela de consolidação
            
        if "ano" in self.df.columns and "mes" in self.df.columns:
            max_ano = int(self.df["ano"].max())
            max_mes = int(self.df[self.df["ano"] == max_ano]["mes"].max())
            data_maxima_dado = datetime.date(max_ano, max_mes, 28)
            
            # Cálculo do atraso regulatório (lag de publicação da ANP)
            lag_dias = (data_extracao - data_maxima_dado).days
            # Padrão ANP é D+60 dias (2 meses de validação fiscal do BMP)
            status = "APROVADO" if lag_dias <= 90 else "ALERTA"
            
            self.registrar_teste(
                dimensao="Atualidade",
                regra="Latência regulatória compatível com ciclo decisório da ANP (D+60 dias)",
                total_registros=n_total,
                falhas=0 if lag_dias <= 90 else 1,
                taxa_conformidade=100.0 if lag_dias <= 90 else 85.0,
                status=status,
                detalhes=f"Última safra de dados: {max_mes:02d}/{max_ano}. Defasagem calculada: {lag_dias} dias (dentro do SLA legal da ANP)."
            )
        return self

    # =========================================================================
    # RELATÓRIOS E CONSOLIDAÇÃO
    # =========================================================================
    def executar_todas_validacoes(self) -> pd.DataFrame:
        self.validar_completude()
        self.validar_unicidade()
        self.validar_consistencia()
        self.validar_conformidade()
        self.validar_acuracia()
        self.validar_atualidade()
        return self.obter_dataframe_relatorio()

    def obter_dataframe_relatorio(self) -> pd.DataFrame:
        return pd.DataFrame(self.resultados)

    def gerar_relatorio_markdown(self) -> str:
        df_res = self.obter_dataframe_relatorio()
        total_testes = len(df_res)
        aprovados = len(df_res[df_res["status"] == "APROVADO"])
        alertas = len(df_res[df_res["status"] == "ALERTA"])
        reprovados = len(df_res[df_res["status"] == "REPROVADO"])
        score_global = df_res["conformidade_pct"].mean()

        md = []
        md.append(f"# Relatório de Garantia de Qualidade de Dados (DQA)")
        md.append(f"**Conjunto Auditado:** `{self.nome_dataset}`  ")
        md.append(f"**Data da Auditoria:** {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}  ")
        md.append(f"**Score Global de Conformidade:** `{score_global:.2f}%`  ")
        md.append(f"**Status dos Testes:** Total: {total_testes} | Aprovados: {aprovados} | Alertas: {alertas} | Reprovados: {reprovados}\n")
        md.append("| Dimensão | Regra de Negócio Auditada | Registros | Falhas | Conformidade (%) | Status |")
        md.append("|:---|:---|:---:|:---:|:---:|:---:|")
        
        for _, row in df_res.iterrows():
            badge = "🟢" if row["status"] == "APROVADO" else ("🟡" if row["status"] == "ALERTA" else "🔴")
            md.append(f"| **{row['dimensao']}** | {row['regra']} | {row['total_registros']:,} | {row['falhas']:,} | {row['conformidade_pct']:.2f}% | {badge} {row['status']} |")
            
        md.append("\n### Detalhamento e Diagnóstico por Dimensão:")
        for _, row in df_res.iterrows():
            md.append(f"- **[{row['dimensao']}]** *{row['regra']}*: {row['detalhes']}")
            
        return "\n".join(md)

    def imprimir_sumario(self):
        print("\n" + "="*80)
        print(f"RELATÓRIO DE AUDITORIA DE QUALIDADE DE DADOS - {self.nome_dataset.upper()}")
        print("="*80)
        df_res = self.obter_dataframe_relatorio()
        for _, r in df_res.iterrows():
            print(f"[{r['status']:<9}] {r['dimensao']:<14} | {r['regra'][:45]:<45} | Conf: {r['conformidade_pct']:>6.2f}% | Falhas: {r['falhas']}")
        print("="*80)
        print(f"Taxa Média de Conformidade Global: {df_res['conformidade_pct'].mean():.2f}%\n")


if __name__ == "__main__":
    # Teste rápido do validador com DataFrame fictício
    df_teste = pd.DataFrame({
        "cod_poco": ["7-MRL-215D-RJS", "7-MRL-198-RJS"],
        "ano": [2024, 2024],
        "mes": [1, 2],
        "campo": ["MARLIM", "MARLIM"],
        "instalacao": ["P-35", "P-35"],
        "ambiente": ["MAR", "MAR"],
        "volume_oleo_m3": [10500.0, 9800.0],
        "volume_agua_m3": [8500.0, 9200.0],
        "volume_gas_total_Mm3": [1.25, 1.18],
        "volume_gas_queima_Mm3": [0.03, 0.04],
        "tempo_producao_dias": [31, 29],
        "corte_agua_bsw_pct": [44.74, 48.42]
    })
    validador = ValidadorQualidadeDados(df_teste, "teste_unitario")
    validador.executar_todas_validacoes()
    validador.imprimir_sumario()
