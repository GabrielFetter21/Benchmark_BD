from pymongo import MongoClient
from decimal import Decimal
from datetime import datetime
import time
import random

# =============================================================================================================
# 🔹 Conexão com MongoDB
# =============================================================================================================
def conectar_mongo(logger):
    """Estabelece conexão com o MongoDB."""
    try:
        inicio = time.time()
        client = MongoClient("mongodb://localhost:27017/")
        db = client["e-commerce"]
        logger.info(f"Conectado ao MongoDB ({round(time.time() - inicio, 2)}s).")
        return client, db
    except Exception as e:
        logger.exception("Falha ao conectar ao MongoDB: %s", e)
        raise

# =============================================================================================================
# 🔹 Limpeza das coleções
# =============================================================================================================
def limpar_colecoes(db, logger):
    """Remove todas as coleções e seus documentos."""
    try:
        db.clientes.delete_many({})
        db.categorias.delete_many({})
        db.produtos.delete_many({})
        db.pedidos.delete_many({})
        logger.info("Coleções do MongoDB limpas com sucesso!")
    except Exception as e:
        logger.exception("Erro ao limpar coleções do MongoDB: %s", e)

# =============================================================================================================
# 🔹 Inserção inicial de dados simulados (gerados via Faker)
# =============================================================================================================
def inserir_dados_mongo(db, dados, logger):
    """Insere dados simulados no MongoDB com estrutura equivalente ao PostgreSQL."""
    try:
        inicio = time.time()

        db.categorias.insert_many(dados["categorias"])
        db.clientes.insert_many(dados["clientes"])
        db.produtos.insert_many(dados["produtos"])

        # Embute os itens dentro dos pedidos
        pedidos_embutidos = []
        for pedido in dados["pedidos"]:
            itens = [i for i in dados["itens_pedido"] if i["pedido_id"] == pedido["id_pedido"]]
            pedido_doc = {
                "id_pedido": pedido["id_pedido"],
                "cliente_id": pedido["cliente_id"],
                "data_pedido": pedido["data_pedido"],
                "valor_total": pedido["valor_total"],
                "status": pedido.get("status", "Pendente"),
                "itens": itens
            }
            pedidos_embutidos.append(pedido_doc)

        db.pedidos.insert_many(pedidos_embutidos)
        logger.info(f"Dados inseridos no MongoDB em {round(time.time() - inicio, 2)}s.")

        return (time.time() - inicio) * 1000  # tempo em ms

    except Exception as e:
        logger.exception("Erro ao inserir dados no MongoDB: %s", e)
        return 0

# =============================================================================================================
# 🔹 Sincronização: PostgreSQL → MongoDB
# =============================================================================================================
def sincronizar_para_mongo(cursor_pg, db_mongo, logger):
    """Sincroniza as tabelas PostgreSQL com as coleções MongoDB, convertendo valores do tipo decimal
       para floats."""
    def decimal_to_float(obj):
        if isinstance(obj, list):
            return [decimal_to_float(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: decimal_to_float(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal):
            return float(obj)
        return obj

    try:
        inicio = time.time()
        logger.info("Iniciando sincronização PostgreSQL → MongoDB...")

        limpar_colecoes(db_mongo, logger)

        # CATEGORIAS
        cursor_pg.execute("SELECT id_categoria, nome FROM categorias;")
        categorias_docs = [{"_id_pg": c[0], "nome": c[1]} for c in cursor_pg.fetchall()]
        db_mongo.categorias.insert_many(categorias_docs)

        # CLIENTES
        cursor_pg.execute("""
            SELECT id_cliente, nome, cpf, email, endereco, telefone, data_cadastro
            FROM clientes;
        """)
        clientes_docs = [
            {
                "_id_pg": c[0],
                "nome": c[1],
                "cpf": c[2],
                "email": c[3],
                "endereco": c[4],
                "telefone": c[5],
                "data_cadastro": c[6],
            }
            for c in cursor_pg.fetchall()
        ]
        db_mongo.clientes.insert_many(clientes_docs)

        # PRODUTOS
        cursor_pg.execute("""
            SELECT id_produto, nome, preco, estoque, categoria_id
            FROM produtos;
        """)
        produtos_docs = [
            {
                "_id_pg": p[0],
                "nome": p[1],
                "preco": float(p[2]),
                "estoque": p[3],
                "categoria_id": p[4]
            }
            for p in cursor_pg.fetchall()
        ]
        db_mongo.produtos.insert_many(produtos_docs)

        # PEDIDOS + ITENS
        cursor_pg.execute("""
            SELECT p.id_pedido, p.cliente_id, p.data_pedido, p.valor_total, p.status,
                   i.produto_id, i.quantidade, i.preco_unitario
            FROM pedidos p
            JOIN itens_pedido i ON p.id_pedido = i.pedido_id
            ORDER BY p.id_pedido;
        """)
        pedidos_raw = cursor_pg.fetchall()

        pedidos_docs = []
        pedido_atual = None
        pedido_dict = {}

        for row in pedidos_raw:
            id_pedido, cliente_id, data_pedido, valor_total, status, produto_id, quantidade, preco_unitario = row

            if pedido_atual != id_pedido:
                if pedido_dict:
                    pedidos_docs.append(decimal_to_float(pedido_dict))
                pedido_atual = id_pedido
                pedido_dict = {
                    "_id_pg": id_pedido,
                    "cliente": {"id": cliente_id},
                    "data_pedido": data_pedido,
                    "valor_total": valor_total,
                    "status": status,
                    "itens": []
                }

            pedido_dict["itens"].append({
                "produto_id": produto_id,
                "quantidade": quantidade,
                "preco_unitario": preco_unitario
            })

        if pedido_dict:
            pedidos_docs.append(decimal_to_float(pedido_dict))

        db_mongo.pedidos.insert_many(pedidos_docs)

        logger.info(f"Sincronização concluída ({round(time.time() - inicio, 2)}s).")
        return (time.time() - inicio) * 1000

    except Exception as e:
        logger.exception("Erro ao sincronizar PostgreSQL → MongoDB: %s", e)
        return 0

# =============================================================================================================
# 🔹 Selecionando dados das coleções do MongoDB
# =============================================================================================================
def selecionar_dados_mongo(db, logger):
    inicio = time.perf_counter()
    pipeline = [
        {"$lookup": {"from": "clientes", "localField": "cliente.id", "foreignField": "_id_pg", "as": "cliente_info"}},
        {"$lookup": {"from": "produtos", "localField": "itens.produto_id", "foreignField": "_id_pg", "as": "produtos_info"}},
        {"$limit": 500}
    ]
    _ = list(db.pedidos.aggregate(pipeline))
    return (time.perf_counter() - inicio) * 1000

# =============================================================================================================
# 🔹 Atualização de dados no MongoDB
# =============================================================================================================
def atualizar_dados_mongo(db, logger):
    inicio = time.perf_counter()
    db.produtos.update_many({}, {"$mul": {"preco": 1.1}})
    db.clientes.update_many({}, {"$set": {"status": "Atualizado"}})
    return (time.perf_counter() - inicio) * 1000

# =============================================================================================================
# 🔹 Excluindo dados no MongoDB
# =============================================================================================================
def deletar_dados_mongo(db, logger):
    inicio = time.perf_counter()
    db.pedidos.delete_many({"_id_pg": {"$gt": 50}})
    return (time.perf_counter() - inicio) * 1000

# =============================================================================================================
# 🔹 Tamanho da base do MongoDB
# =============================================================================================================
def tamanho_mongo(db):
    stats = db.command("dbStats")
    return round(stats["dataSize"] / (1024 ** 2), 2)

# =============================================================================================================
# 🔹 Encerramento da conexão
# =============================================================================================================
def fechar_conexao(client, logger):
    """Fecha a conexão com segurança."""
    try:
        client.close()
        logger.debug("Conexão MongoDB encerrada.")
    except Exception as e:
        logger.warning("Falha ao encerrar conexão MongoDB: %s", e)