import psycopg2
from psycopg2 import sql
import time
from datetime import datetime
import random

# ===========================================================================================================
# 🔹 Conexão com PostgreSQL
# ===========================================================================================================
def conectar_postgres(logger):
    # Cria conexão e cursor com o banco PostgreSQL
    try:
        inicio = time.time()
        conn = psycopg2.connect(
            dbname="Ecommerce",
            user="postgres",
            password="GACF1102",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        if logger:
            logger.info("Conexão PostgreSQL estabelecida com sucesso!")
        return conn, cursor
    except Exception as e:
        if logger:
            logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
        raise

# ===========================================================================================================
# 🔹 Limpeza das tabelas e reinicialização dos IDs
# ===========================================================================================================
def limpar_tabelas(cursor, conn, logger):
    """Remove todos os registros das tabelas, mantendo a estrutura."""
    try:
        cursor.execute("""
            TRUNCATE TABLE itens_pedido, pedidos, produtos, clientes, categorias 
            RESTART IDENTITY CASCADE;
        """)
        conn.commit()
        logger.info("Tabelas do PostgreSQL limpas com sucesso!")
    except Exception as e:
        logger.exception("Erro ao limpar tabelas: %s", e)
        conn.rollback()

# ===========================================================================================================
# 🔹 Inserção de dados no PostgreSQL
# ===========================================================================================================
def inserir_dados_postgres(cursor, conn, logger, dados=None):
    """
    Insere dados gerados pelo data_generator nas tabelas PostgreSQL, mantendo o relacionamento entre
    as chaves.
    """
    try:
        inicio = time.time()

        # Tabela de Categorias
        for c in dados["categorias"]:
            cursor.execute(
                "INSERT INTO categorias (nome) VALUES (%s);",
                (c["nome"],)
            )

        # Tabela de Clientes
        for cli in dados["clientes"]:
            cursor.execute("""
                        INSERT INTO clientes (nome, cpf, email, endereco, telefone, data_cadastro)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (cli["nome"], cli["cpf"], cli["email"], cli["endereco"], cli["telefone"],
                          cli["data_cadastro"]))

        # Tabela de Produtos
        for p in dados["produtos"]:
            cursor.execute("""
                        INSERT INTO produtos (nome, preco, estoque, categoria_id)
                        VALUES (%s, %s, %s, %s);
                    """, (p["nome"], p["preco"], p["estoque"], p["categoria_id"]))

        # Tabela de Pedidos
        for ped in dados["pedidos"]:
            cursor.execute("""
                        INSERT INTO pedidos (cliente_id, data_pedido, valor_total, status)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id_pedido;
                    """, (ped["cliente_id"], ped["data_pedido"], ped["valor_total"], ped["status"]))
            ped["id_pedido"] = cursor.fetchone()[0]

        # Tabela de Itens do pedido
        for item in dados["itens_pedido"]:
            cursor.execute("""
                        INSERT INTO itens_pedido (pedido_id, produto_id, quantidade, preco_unitario)
                        VALUES (%s, %s, %s, %s);
                    """, (item["pedido_id"], item["produto_id"], item["quantidade"], item["preco_unitario"]))

        conn.commit()
        tempo = (time.time() - inicio) * 1000
        logger.info(f"Dados inseridos no PostgreSQL em {round(tempo, 2)} ms.")
        return tempo

    except Exception as e:
        logger.exception("Erro ao inserir dados no PostgreSQL: %s", e)
        conn.rollback()
        return 0

# ===========================================================================================================
# 🔹 Selecionar dados das tabelas do PostgreSQL
# ===========================================================================================================
def selecionar_dados_postgres(cursor, conn, logger):
    """Executa consultas de exemplo (JOINs entre as principais tabelas)."""
    try:
        inicio = time.perf_counter()
        cursor.execute("""
            SELECT p.id_pedido, c.nome, pr.nome, i.quantidade, i.preco_unitario
            FROM pedidos p
            JOIN clientes c ON p.cliente_id = c.id_cliente
            JOIN itens_pedido i ON p.id_pedido = i.pedido_id
            JOIN produtos pr ON i.produto_id = pr.id_produto
            LIMIT 500;
        """)
        _ = cursor.fetchall()
        return (time.perf_counter() - inicio) * 1000
    except Exception as e:
        logger.exception("Erro ao selecionar dados: %s", e)
        return 0

# ===========================================================================================================
# 🔹 Atualização de dados no PostgreSQL
# ===========================================================================================================
def atualizar_dados_postgres(cursor, conn, logger):
    """Atualiza registros para medir a performance de UPDATE."""
    try:
        inicio = time.perf_counter()
        cursor.execute("UPDATE produtos SET preco = preco * 1.1;")
        cursor.execute("UPDATE clientes SET nome = nome || ' (Atualizado)';")
        conn.commit()
        return (time.perf_counter() - inicio) * 1000
    except Exception as e:
        logger.exception("Erro ao atualizar dados: %s", e)
        conn.rollback()
        return 0

# ===========================================================================================================
# 🔹 Excluir dados no PostgreSQL
# ===========================================================================================================
def deletar_dados_postgres(cursor, conn, logger):
    """Exclui alguns registros para medir a performance de DELETE."""
    try:
        inicio = time.perf_counter()
        cursor.execute("DELETE FROM pedidos WHERE id_pedido > 50;")
        conn.commit()
        return (time.perf_counter() - inicio) * 1000
    except Exception as e:
        logger.exception("Erro ao deletar dados: %s", e)
        conn.rollback()
        return 0

# ===========================================================================================================
# 🔹 Tamanho da base
# ===========================================================================================================
def tamanho_postgres(conn):
    """Obtém o tamanho total do banco de dados em MB."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT pg_database_size(current_database());")
        tamanho_bytes = cursor.fetchone()[0]
        return round(tamanho_bytes / (1024 ** 2), 2)
    except Exception:
        return 0

# ======================================================================================================
# 🔹 Encerramento da conexão
# ======================================================================================================
def fechar_conexao(conn, cursor, logger):
    """Fecha a conexão com segurança."""
    try:
        cursor.close()
        conn.close()
        logger.info("Conexão com PostgreSQL encerrada.")
    except Exception as e:
        logger.exception("Falha ao encerrar conexão com PostgreSQL: %s", e)