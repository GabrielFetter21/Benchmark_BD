"""
Módulo de análise de desempenho e geração de gráficos comparativos.
Registra tempos de execução em CSV e cria gráficos de médias por etapa.
"""

import csv
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # Modo sem interface gráfica
import matplotlib.pyplot as plt
from datetime import datetime
from statistics import mean, stdev

# Caminhos padrão
PASTA_GRAFICOS = "logs/graficos"
ARQUIVO_CSV = "logs/resultados_crud.csv"


# =============================================================================================================
# 🔹 Função: salvar_resultados_csv
# =============================================================================================================
def salvar_resultados_csv(resultados, arquivo=ARQUIVO_CSV):
    # Salva as métricas do benchmark em um arquivo CSV.
    os.makedirs(os.path.dirname(arquivo), exist_ok=True)
    with open(arquivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=resultados[0].keys())
        writer.writeheader()
        writer.writerows(resultados)
    print(f"[✔] Resultados salvos em {arquivo}")


# =============================================================================================================
# 🔹 Função: gerar_graficos_comparativos
# =============================================================================================================
def gerar_graficos_comparativos(resultados):
    # Gera gráficos comparativos de tempo, throughput, CPU, memória e tamanho das bases.
    os.makedirs(PASTA_GRAFICOS, exist_ok=True)  # Criação da pasta de saída
    df = pd.DataFrame(resultados)

    # ========================================================================================================
    # Gráfico 1: Tempo médio por operação
    # ========================================================================================================
    plt.figure(figsize=(8, 5))
    bar_width = 0.35
    x = range(len(df["operacao"]))
    plt.bar([p - bar_width / 2 for p in x], df["tempo_pg_ms"], bar_width, label="PostgreSQL", alpha=0.8)
    plt.bar([p + bar_width / 2 for p in x], df["tempo_mongo_ms"], bar_width, label="MongoDB", alpha=0.8)
    plt.xticks(x, df["operacao"])
    plt.ylabel("Tempo médio (ms)")
    plt.title("Comparativo de tempo por tipo de operação")
    plt.legend()
    plt.grid(True, axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(f"{PASTA_GRAFICOS}/tempo_por_operacao.png")
    plt.close()

    # ========================================================================================================
    # Gráfico 2: Throughput por operação
    # ========================================================================================================
    plt.figure(figsize=(8, 5))
    plt.bar([p - bar_width / 2 for p in x], df["throughput_pg_ops_s"], bar_width, label="PostgreSQL", alpha=0.8)
    plt.bar([p + bar_width / 2 for p in x], df["throughput_mongo_ops_s"], bar_width, label="MongoDB", alpha=0.8)
    plt.xticks(x, df["operacao"])
    plt.ylabel("Throughput (operações/segundo)")
    plt.title("Comparativo de throughput por tipo de operação")
    plt.legend()
    plt.grid(True, axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(f"{PASTA_GRAFICOS}/throughput_por_operacao.png")
    plt.close()

    # =======================================================================================================
    # Gráfico 3: Uso médio de CPU e memória
    # =======================================================================================================
    plt.figure(figsize=(8, 5))
    plt.plot(df["operacao"], df["cpu_media_%"], marker="o", label="CPU (%)")
    plt.plot(df["operacao"], df["memoria_media_MB"], marker="s", label="Memória (MB)")
    plt.title("Uso médio de CPU e memória por operação")
    plt.xlabel("Operação")
    plt.ylabel("Uso médio")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(f"{PASTA_GRAFICOS}/cpu_memoria_por_operacao.png")
    plt.close()

    # =======================================================================================================
    # Gráfico 4: Tamanho das bases
    # =======================================================================================================
    plt.figure(figsize=(8, 5))
    plt.plot(df["operacao"], df["tam_pg_MB"], label="PostgreSQL (MB)", marker="o")
    plt.plot(df["operacao"], df["tam_mongo_MB"], label="MongoDB (MB)", marker="s")
    plt.title("Tamanho das bases de dados após cada operação")
    plt.xlabel("Operação")
    plt.ylabel("Tamanho (MB)")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(f"{PASTA_GRAFICOS}/tamanho_bases.png")
    plt.close()

    print("[✔] Gráficos comparativos gerados com sucesso.")


# ==============================================================================================================
# 🔹 Função: gerar_resumo_textual
# ==============================================================================================================
    def gerar_resumo_textual():
        # Gera um resumo estatístico com base nas métricas do CSV.
        if not os.path.exists(ARQUIVO_CSV):
            print(f"[⚠] Arquivo {ARQUIVO_CSV} não encontrado.")
            return

        df = pd.read_csv(ARQUIVO_CSV)
        resumo = []

        resumo.append("=== RESUMO DE DESEMPENHO DO BENCHMARK ===\n")
        resumo.append(f"Operações testadas: {', '.join(df['operacao'])}\n")
        resumo.append(f"Tempo médio PostgreSQL (ms): {df['tempo_pg_ms'].mean():.2f}")
        resumo.append(f"Tempo médio MongoDB (ms): {df['tempo_mongo_ms'].mean():.2f}")
        resumo.append(f"Throughput médio PostgreSQL (ops/s): {df['throughput_pg_ops_s'].mean():.2f}")
        resumo.append(f"Throughput médio MongoDB (ops/s): {df['throughput_mongo_ops_s'].mean():.2f}")
        resumo.append(f"Uso médio de CPU: {df['cpu_media_%'].mean():.2f}%")
        resumo.append(f"Uso médio de memória: {df['memoria_media_MB'].mean():.2f} MB")
        resumo.append(f"Tamanho médio PostgreSQL: {df['tam_pg_MB'].mean():.2f} MB")
        resumo.append(f"Tamanho médio MongoDB: {df['tam_mongo_MB'].mean():.2f} MB")

        os.makedirs(PASTA_GRAFICOS, exist_ok=True)
        arquivo_resumo = f"{PASTA_GRAFICOS}/resumo_metricas.txt"
        with open(arquivo_resumo, "w", encoding="utf-8") as f:
            f.write("\n".join(resumo))

        print(f"[✔] Resumo salvo em {arquivo_resumo}")


# ==============================================================================================================
# 🔹 Função: análise consolidada
# ==============================================================================================================
    def analisar_resultados_completos():
        # Lê o arquivo CSV de resultados e gera todos os gráficos e o resumo.
        if not os.path.exists(ARQUIVO_CSV):
            print(f"[⚠] Nenhum resultado encontrado em {ARQUIVO_CSV}.")
            return

        df = pd.read_csv(ARQUIVO_CSV)
        print("\n=== Resumo das Operações Testadas ===")
        print(df[["operacao", "tempo_pg_ms", "tempo_mongo_ms", "throughput_pg_ops_s", "throughput_mongo_ops_s"]])

        gerar_graficos_comparativos(df.to_dict(orient="records"))
        gerar_resumo_textual()
        print("\n[✔] Todos os gráficos e análises foram gerados com sucesso!")


# ==============================================================================================================
# 🔹 Execução direta
# ==============================================================================================================
if __name__ == "__main__":
    analisar_resultados_completos()