"""
Benchmark completo de desempenho: PostgreSQL → MongoDB
Executa operações CRUD em ambas as bases, coleta métricas de tempo, CPU, memória,
throughput e tamanho das bases, e gera relatórios gráficos comparativos.
"""

import os
import csv
from db_postgres import *
from db_mongo import *
from data_generator import gerar_dados_simulados
from resource_monitor import ResourceMonitor
from logger_config import configurar_logger
from performance_analyzer import gerar_graficos_comparativos


# =============================================================================================================
# 🔹Função genérica de execução com coleta de métricas
# =============================================================================================================
def executar_benchmark_operacao(tipo, func_pg, func_mongo, conn_pg, cursor_pg, db_mongo, dados, logger,
    total_ops=1000):
    logger.info(f"\n Executando operação {tipo}...")

    monitor = ResourceMonitor()
    monitor.start()

    # PostgreSQL
    t_pg = func_pg(cursor_pg, conn_pg, logger) if func_pg else 0
    # MongoDB
    t_mongo = func_mongo(db_mongo, logger) if func_mongo else 0

    monitor.stop()
    recursos = monitor.get_stats()

    throughput_pg = total_ops / (t_pg / 1000) if t_pg > 0 else 0
    throughput_mongo = total_ops / (t_mongo / 1000) if t_mongo > 0 else 0

    resultado = {
        "operacao": tipo,
        "tempo_pg_ms": round(t_pg, 2),
        "tempo_mongo_ms": round(t_mongo, 2),
        "throughput_pg_ops_s": round(throughput_pg, 2),
        "throughput_mongo_ops_s": round(throughput_mongo, 2),
        "cpu_media_%": round(recursos["cpu_avg"], 2),
        "memoria_media_MB": round(recursos["mem_avg"], 2),
        "tam_pg_MB": tamanho_postgres(conn_pg),
        "tam_mongo_MB": tamanho_mongo(db_mongo),
    }

    logger.info(f"Operação {tipo} concluída.")
    return resultado


# =============================================================================================================
# 🔹Benchmark completo
# =============================================================================================================
def executar_benchmark_completo():
    logger = configurar_logger()
    logger.info("=" * 70)
    logger.info("INICIANDO BENCHMARK COMPLETO - PostgreSQL x MongoDB")
    logger.info("=" * 70)

    # Conexões
    conn_pg, cursor_pg = conectar_postgres(logger)
    client_mongo, db_mongo = conectar_mongo(logger)

    # Limpeza inicial
    limpar_tabelas(cursor_pg, conn_pg, logger)
    limpar_colecoes(db_mongo, logger)

    # Geração de dataset simulado
    logger.info("Gerando dataset simulado...")
    dados = gerar_dados_simulados(
        qtd_clientes=100,
        qtd_produtos=50,
        qtd_pedidos=40,
        qtd_categorias=5,
    )

    resultados = []

    # 1️)INSERT
    resultados.append(
        executar_benchmark_operacao(
            "INSERT",
            lambda c, conn, log: inserir_dados_postgres(c, conn, log, dados),
            lambda db, log: inserir_dados_mongo(db, dados, log),
            conn_pg, cursor_pg, db_mongo, dados, logger
        )
    )

    # 2️)Sincronização PostgreSQL → MongoDB
    logger.info("Sincronizando dados PostgreSQL → MongoDB...")
    sincronizar_para_mongo(cursor_pg, db_mongo, logger)

    # 3️)SELECT
    resultados.append(
        executar_benchmark_operacao(
            "SELECT",
            selecionar_dados_postgres,
            selecionar_dados_mongo,
            conn_pg, cursor_pg, db_mongo, dados, logger
        )
    )

    # 4️)UPDATE
    resultados.append(
        executar_benchmark_operacao(
            "UPDATE",
            atualizar_dados_postgres,
            atualizar_dados_mongo,
            conn_pg, cursor_pg, db_mongo, dados, logger
        )
    )

    # 5️)DELETE
    resultados.append(
        executar_benchmark_operacao(
            "DELETE",
            deletar_dados_postgres,
            deletar_dados_mongo,
            conn_pg, cursor_pg, db_mongo, dados, logger
        )
    )

    # Salvando resultados
    os.makedirs("logs", exist_ok=True)
    arquivo_csv = "logs/resultados_crud.csv"
    with open(arquivo_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=resultados[0].keys())
        writer.writeheader()
        writer.writerows(resultados)
    logger.info(f"Resultados salvos em {arquivo_csv}")

    # Geração de gráficos comparativos
    gerar_graficos_comparativos(resultados)
    logger.info("Gráficos de desempenho gerados com sucesso em /logs/graficos/")

    logger.info("=" * 70)
    logger.info("BENCHMARK FINALIZADO COM SUCESSO!")
    logger.info("=" * 70)

    client_mongo.close()
    cursor_pg.close()
    conn_pg.close()


# =============================================================================================================
# 🔹Ponto de entrada principal
# =============================================================================================================
if __name__ == "__main__":
    executar_benchmark_completo()
