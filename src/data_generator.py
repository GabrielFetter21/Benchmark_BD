"""
Módulo responsável por gerar dados simulados para popular os bancos PostgreSQL e MongoDB.
Compatível com as tabelas: categorias, clientes, produtos, pedidos e itens_pedido.
"""

from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker("pt_BR")

# ===============================================================================================================
# 🔹 Função principal de geração dos dados
# ===============================================================================================================
def gerar_dados_simulados(qtd_clientes=100, qtd_produtos=50, qtd_pedidos=40, qtd_categorias=5):
    """Gera dados aleatórios coerentes com a estrutura do banco relacional e NoSQL."""

    # ===========================================================================================================
    # 🔹 CATEGORIAS
    # ===========================================================================================================
    todas_categorias = ["Eletrônicos", "Vestuário", "Livros", "Brinquedos", "Móveis", "Beleza", "Esportes",
                        "Automotivo", "Casa", "Saúde"]
    categorias = random.sample(todas_categorias, k=min(qtd_categorias, len(todas_categorias)))
    categorias_dados = [{"id_categoria": i + 1, "nome": nome} for i, nome in enumerate(categorias)]

    # ===========================================================================================================
    # 🔹 CLIENTES
    # ===========================================================================================================
    clientes = [
        {
            "id_cliente": i + 1,
            "nome": fake.name(),
            "cpf": fake.cpf().replace(".", "").replace("-", ""),
            "email": fake.email(),
            "endereco": fake.address(),
            "telefone": fake.phone_number(),
            "data_cadastro": fake.date_time_between(start_date="-2y", end_date="now")
        }
        for i in range(qtd_clientes)
    ]

    # ===========================================================================================================
    # 🔹 PRODUTOS
    # ===========================================================================================================
    produtos = [
        {
            "id_produto": i + 1,
            "categoria_id": random.randint(1, len(categorias_dados)),
            "nome": fake.word().capitalize(),
            "preco": round(random.uniform(10.0, 5000.0), 2),
            "estoque": random.randint(5, 200)
        }
        for i in range(qtd_produtos)
    ]

    # ===========================================================================================================
    # 🔹 PEDIDOS
    # ===========================================================================================================
    pedidos = []
    for i in range(qtd_pedidos):
        cliente = random.choice(clientes)
        data_pedido = fake.date_time_between(start_date="-6M", end_date="now")
        pedidos.append({
            "id_pedido": i + 1,
            "cliente_id": cliente["id_cliente"],
            "data_pedido": data_pedido,
            "valor_total": 0.0,  # será atualizado ao gerar os itens
            "status": random.choice(["Pendente", "Pago", "Enviado", "Entregue", "Cancelado"])
        })

    # ===========================================================================================================
    # 🔹 ITENS DE PEDIDO
    # ===========================================================================================================
    itens_pedido = []
    for pedido in pedidos:
        qtd_itens = random.randint(1, 5)
        total_pedido = 0.0
        for _ in range(qtd_itens):
            produto = random.choice(produtos)
            quantidade = random.randint(1, 3)
            subtotal = produto["preco"] * quantidade
            total_pedido += subtotal
            itens_pedido.append({
                "pedido_id": pedido["id_pedido"],
                "produto_id": produto["id_produto"],
                "quantidade": quantidade,
                "preco_unitario": produto["preco"],
                "subtotal": subtotal,
                "cliente_cpf": next(c["cpf"] for c in clientes if c["id_cliente"] == pedido["cliente_id"]),
                "data_pedido": pedido["data_pedido"]
            })
        pedido["valor_total"] = round(total_pedido, 2)

    # ===========================================================================================================
    # 🔹 Retorno unificado
    # ===========================================================================================================
    return {"categorias": categorias_dados,
        "clientes": clientes,
        "produtos": produtos,
        "pedidos": pedidos,
        "itens_pedido": itens_pedido
    }