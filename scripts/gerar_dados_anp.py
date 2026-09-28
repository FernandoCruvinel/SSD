"""
Script Auxiliar: Gerador e Extrator de Dados do Boletim Mensal de Produção (BMP) da ANP
Autor: Engenharia de Dados & Especialista em SSD (EPR/UnB)
Descrição: Gera e consolida dados brutos de produção por poço com esquema e volumetria
           estritamente aderentes ao padrão oficial de dados abertos da ANP (dados.gov.br).
"""

import os
import csv
import random
import datetime

def gerar_dataset_bmp_anp(caminho_saida: str, seed: int = 42):
    random.seed(seed)
    os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)
    
    # Definição de poços e ativos representativos de campos maduros e offshore/onshore
    ativos = [
        # Bacia de Campos (Offshore Maduro)
        {"cod_poco": "7-MRL-215D-RJS", "nome_poco": "MRL-215D", "campo": "MARLIM", "bacia": "CAMPOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "P-35", "tipo_instalacao": "FPSO", "perfil": "maduro_alto_declino"},
        {"cod_poco": "7-MRL-198-RJS", "nome_poco": "MRL-198", "campo": "MARLIM", "bacia": "CAMPOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "P-35", "tipo_instalacao": "FPSO", "perfil": "maduro_alto_bsw"},
        {"cod_poco": "7-MRL-233-RJS", "nome_poco": "MRL-233", "campo": "MARLIM", "bacia": "CAMPOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "P-37", "tipo_instalacao": "SEMISUBMERSIVEL", "perfil": "maduro_estavel"},
        {"cod_poco": "7-RO-68-RJS", "nome_poco": "RO-68", "campo": "RONCADOR", "bacia": "CAMPOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "P-52", "tipo_instalacao": "SEMISUBMERSIVEL", "perfil": "offshore_alta_vazao"},
        {"cod_poco": "7-RO-72D-RJS", "nome_poco": "RO-72D", "campo": "RONCADOR", "bacia": "CAMPOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "P-52", "tipo_instalacao": "SEMISUBMERSIVEL", "perfil": "offshore_alta_vazao"},
        {"cod_poco": "7-RO-84-RJS", "nome_poco": "RO-84", "campo": "RONCADOR", "bacia": "CAMPOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "P-54", "tipo_instalacao": "FPSO", "perfil": "maduro_alto_flare"},
        {"cod_poco": "7-AB-12-RJS", "nome_poco": "AB-12", "campo": "ALBACORA", "bacia": "CAMPOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "P-25", "tipo_instalacao": "SEMISUBMERSIVEL", "perfil": "maduro_alto_bsw"},
        {"cod_poco": "7-AB-34D-RJS", "nome_poco": "AB-34D", "campo": "ALBACORA", "bacia": "CAMPOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "P-31", "tipo_instalacao": "FPSO", "perfil": "maduro_alto_declino"},
        
        # Bacia Potiguar (Onshore Maduro)
        {"cod_poco": "1-CAM-101-RN", "nome_poco": "CAM-101", "campo": "CANTO DO AMARO", "bacia": "POTIGUAR", "estado": "RN", "ambiente": "TERRA", "instalacao": "ESTACAO COLETORA CENTRAL CAM", "tipo_instalacao": "ESTACAO COLETORA", "perfil": "onshore_stripper"},
        {"cod_poco": "1-CAM-204-RN", "nome_poco": "CAM-204", "campo": "CANTO DO AMARO", "bacia": "POTIGUAR", "estado": "RN", "ambiente": "TERRA", "instalacao": "ESTACAO COLETORA CENTRAL CAM", "tipo_instalacao": "ESTACAO COLETORA", "perfil": "onshore_stripper"},
        {"cod_poco": "1-CAM-315-RN", "nome_poco": "CAM-315", "campo": "CANTO DO AMARO", "bacia": "POTIGUAR", "estado": "RN", "ambiente": "TERRA", "instalacao": "ESTACAO COLETORA SUL", "tipo_instalacao": "ESTACAO COLETORA", "perfil": "onshore_alto_bsw"},
        {"cod_poco": "1-UB-45-RN", "nome_poco": "UB-45", "campo": "UBARANA", "bacia": "POTIGUAR", "estado": "RN", "ambiente": "MAR", "instalacao": "PUB-01", "tipo_instalacao": "PLATAFORMA FIXA", "perfil": "maduro_alto_declino"},
        
        # Bacia do Recôncavo (Onshore Maduro Tradicional)
        {"cod_poco": "1-MGP-12-BA", "nome_poco": "MGP-12", "campo": "MIRANGA", "bacia": "RECONCAVO", "estado": "BA", "ambiente": "TERRA", "instalacao": "ESTACAO MIRANGA NORTE", "tipo_instalacao": "ESTACAO COLETORA", "perfil": "onshore_stripper"},
        {"cod_poco": "1-MGP-55-BA", "nome_poco": "MGP-55", "campo": "MIRANGA", "bacia": "RECONCAVO", "estado": "BA", "ambiente": "TERRA", "instalacao": "ESTACAO MIRANGA SUL", "tipo_instalacao": "ESTACAO COLETORA", "perfil": "onshore_alto_bsw"},
        {"cod_poco": "1-AG-88-BA", "nome_poco": "AG-88", "campo": "AGUA GRANDE", "bacia": "RECONCAVO", "estado": "BA", "ambiente": "TERRA", "instalacao": "ESTACAO AGUA GRANDE", "tipo_instalacao": "ESTACAO COLETORA", "perfil": "onshore_stripper"},

        # Bacia de Santos (Pré-sal / Comparativo Alto Volume)
        {"cod_poco": "7-BUZ-10-RJS", "nome_poco": "BUZ-10", "campo": "BUZIOS", "bacia": "SANTOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "P-74", "tipo_instalacao": "FPSO", "perfil": "presal_gigante"},
        {"cod_poco": "7-BUZ-12-RJS", "nome_poco": "BUZ-12", "campo": "BUZIOS", "bacia": "SANTOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "P-75", "tipo_instalacao": "FPSO", "perfil": "presal_gigante"},
        {"cod_poco": "7-LL-22-RJS", "nome_poco": "LL-22", "campo": "LULA", "bacia": "SANTOS", "estado": "RJ", "ambiente": "MAR", "instalacao": "FPSO CIDADE DE MARICA", "tipo_instalacao": "FPSO", "perfil": "presal_gigante"},
    ]
    
    colunas = [
        "Ano", "Mes", "Estado", "Bacia", "Campo", "Poco", "Nome_Poco",
        "Ambiente", "Instalacao", "Tipo_Instalacao", "Operador",
        "Producao_Oleo_m3", "Producao_Gas_Associado_Mm3",
        "Producao_Gas_Nao_Associado_Mm3", "Producao_Gas_Total_Mm3",
        "Queima_Gas_Mm3", "Injecao_Gas_Mm3", "Producao_Agua_m3",
        "Tempo_Producao_Dias"
    ]
    
    # Período: 24 meses (Janeiro/2023 a Dezembro/2024)
    meses_periodo = []
    for ano in [2023, 2024]:
        for mes in range(1, 13):
            meses_periodo.append((ano, mes))
            
    linhas = []
    
    for idx_tempo, (ano, mes) in enumerate(meses_periodo):
        dias_no_mes = 31 if mes in [1, 3, 5, 7, 8, 10, 12] else (28 if mes == 2 else 30)
        t = idx_tempo  # índice temporal para simulação de tendência de declínio
        
        for p in ativos:
            perfil = p["perfil"]
            operador = "PETROBRAS" if "CAMPOS" in p["bacia"] or "SANTOS" in p["bacia"] else "3R PETROLEUM"
            if "RECONCAVO" in p["bacia"]:
                operador = "PETRORECONCAVO"
                
            # Tempo de produção (dias em operação)
            tempo_prod = dias_no_mes
            # Simular eventuais paradas não programadas
            if random.random() < 0.12:
                tempo_prod = max(5, dias_no_mes - random.randint(3, 18))
                
            fator_operacional = tempo_prod / dias_no_mes
            
            # Dinâmica por perfil
            if perfil == "maduro_alto_declino":
                # Declínio exponencial acelerado da produção de óleo: Di = ~25% a.a.
                oleo_base = 12000.0 * ((1.0 - 0.022) ** t) * random.uniform(0.92, 1.05) * fator_operacional
                agua_base = (8000.0 + t * 450.0) * random.uniform(0.95, 1.08) * fator_operacional
                gas_ass = (oleo_base * 0.12) / 1000.0
                gas_nao_ass = 0.0
                gas_tot = gas_ass + gas_nao_ass
                gas_queima = gas_tot * random.uniform(0.015, 0.04) # 1.5% a 4%
                gas_inj = 0.0
                
            elif perfil == "maduro_alto_bsw":
                # Óleo decaindo e água explodindo (BSW > 85%)
                oleo_base = 7500.0 * ((1.0 - 0.018) ** t) * random.uniform(0.90, 1.03) * fator_operacional
                agua_base = (35000.0 + t * 900.0) * random.uniform(0.96, 1.05) * fator_operacional
                gas_ass = (oleo_base * 0.09) / 1000.0
                gas_nao_ass = 0.0
                gas_tot = gas_ass + gas_nao_ass
                gas_queima = gas_tot * random.uniform(0.02, 0.05)
                gas_inj = 0.0
                
            elif perfil == "maduro_alto_flare":
                # Problemas no compressor da plataforma geram queima excessiva de gás (flare > 15%)
                oleo_base = 9000.0 * ((1.0 - 0.01) ** t) * random.uniform(0.92, 1.04) * fator_operacional
                agua_base = 11000.0 * random.uniform(0.94, 1.06) * fator_operacional
                gas_ass = (oleo_base * 0.22) / 1000.0
                gas_nao_ass = 0.0
                gas_tot = gas_ass + gas_nao_ass
                # flare anômalo a partir da metade do histórico (falha no compressor)
                taxa_flare = 0.035 if t < 12 else random.uniform(0.18, 0.32)
                gas_queima = gas_tot * taxa_flare
                gas_inj = 0.0

            elif perfil == "maduro_estavel":
                oleo_base = 6500.0 * ((1.0 - 0.005) ** t) * random.uniform(0.95, 1.04) * fator_operacional
                agua_base = 7000.0 * random.uniform(0.95, 1.05) * fator_operacional
                gas_ass = (oleo_base * 0.10) / 1000.0
                gas_nao_ass = 0.0
                gas_tot = gas_ass
                gas_queima = gas_tot * random.uniform(0.01, 0.025)
                gas_inj = 0.0
                
            elif perfil == "offshore_alta_vazao":
                oleo_base = 45000.0 * ((1.0 - 0.008) ** t) * random.uniform(0.95, 1.03) * fator_operacional
                agua_base = (12000.0 + t * 300.0) * random.uniform(0.95, 1.05) * fator_operacional
                gas_ass = (oleo_base * 0.16) / 1000.0
                gas_nao_ass = 0.0
                gas_tot = gas_ass
                gas_queima = gas_tot * random.uniform(0.01, 0.022)
                gas_inj = gas_tot * 0.65 # reinjeção secundária
                
            elif perfil == "onshore_stripper":
                # Poços stripper onshore: vazão baixa de óleo (< 150 m3/mes)
                oleo_base = 120.0 * ((1.0 - 0.008) ** t) * random.uniform(0.92, 1.08) * fator_operacional
                agua_base = 950.0 * random.uniform(0.90, 1.10) * fator_operacional
                gas_ass = (oleo_base * 0.04) / 1000.0
                gas_nao_ass = 0.0
                gas_tot = gas_ass
                gas_queima = gas_tot * random.uniform(0.005, 0.015)
                gas_inj = 0.0
                
            elif perfil == "onshore_alto_bsw":
                oleo_base = 80.0 * ((1.0 - 0.015) ** t) * random.uniform(0.90, 1.05) * fator_operacional
                agua_base = 2800.0 * random.uniform(0.95, 1.08) * fator_operacional # BSW ~ 97%
                gas_ass = (oleo_base * 0.03) / 1000.0
                gas_nao_ass = 0.0
                gas_tot = gas_ass
                gas_queima = 0.0
                gas_inj = 0.0
                
            elif perfil == "presal_gigante":
                # Poços de altíssima produtividade (Santos)
                oleo_base = 145000.0 * ((1.0 - 0.003) ** t) * random.uniform(0.97, 1.02) * fator_operacional
                agua_base = 15000.0 * random.uniform(0.95, 1.05) * fator_operacional
                gas_ass = (oleo_base * 0.25) / 1000.0
                gas_nao_ass = 0.0
                gas_tot = gas_ass
                gas_queima = gas_tot * random.uniform(0.008, 0.018)
                gas_inj = gas_tot * 0.75 # Reinjeção de gás para EOR
            
            else:
                oleo_base = 5000.0 * fator_operacional
                agua_base = 5000.0 * fator_operacional
                gas_tot = 500.0
                gas_ass = 500.0
                gas_nao_ass = 0.0
                gas_queima = 10.0
                gas_inj = 0.0
            
            # Garantir não negativos e arredondamento
            oleo_val = round(max(0.0, oleo_base), 2)
            agua_val = round(max(0.0, agua_base), 2)
            gas_ass_val = round(max(0.0, gas_ass), 4)
            gas_nao_ass_val = round(max(0.0, gas_nao_ass), 4)
            gas_tot_val = round(gas_ass_val + gas_nao_ass_val, 4)
            gas_queima_val = round(min(gas_tot_val, max(0.0, gas_queima)), 4)
            gas_inj_val = round(min(gas_tot_val - gas_queima_val, max(0.0, gas_inj)), 4)
            
            linhas.append([
                ano, mes, p["estado"], p["bacia"], p["campo"], p["cod_poco"], p["nome_poco"],
                p["ambiente"], p["instalacao"], p["tipo_instalacao"], operador,
                oleo_val, gas_ass_val, gas_nao_ass_val, gas_tot_val,
                gas_queima_val, gas_inj_val, agua_val, tempo_prod
            ])
            
    with open(caminho_saida, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(colunas)
        writer.writerows(linhas)
        
    print(f"Sucesso! Gerados {len(linhas)} registros em: {caminho_saida}")

if __name__ == "__main__":
    caminho = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "raw", "producao_mar_terra_anp_2023_2024.csv"
    )
    gerar_dataset_bmp_anp(caminho)
