# Benchmark de Performance: PostgreSQL x MongoDB

Este projeto implementa um **benchmark completo de desempenho** comparando dois sistemas de banco de dados ( **PostgreSQL** e **MongoDB** ) executando operações CRUD, coleta de métricas, sincronização entre bancos, monitoramento de recursos e geração de gráficos comparativos.

Ele foi desenvolvido como parte de um estudo sobre diferenças entre bancos de dados relacionais e NoSQL, analisando **tempo de execução**, **throughput**, **uso de CPU**, **memória** e **tamanho das bases** após cada operação.

---

## Estrutura do Projeto

```
├── data_generator.py
├── db_mongo.py
├── db_postgres.py
├── logger_config.py
├── main_benchmark.py
├── performance_analyzer.py
├── resource_monitor.py
├── logs/
│   ├── execucao.log
│   └── graficos/
│       ├── tempo_por_operacao.png
│       ├── throughput_por_operacao.png
│       ├── cpu_memoria_por_operacao.png
│       ├── tamanho_bases.png
│       └── resumo_metricas.txt
└── README.md
```

---

## Visão Geral

### `data_generator.py`
Gera automaticamente dados realistas para preencher ambas as bases de dados, utilizando Faker.

### `db_postgres.py`
Conecta ao PostgreSQL, limpa tabelas, insere dados e executa operações CRUD.

### `db_mongo.py`
Gerencia a conexão e operações no MongoDB, incluindo sincronização PostgreSQL → MongoDB.

### `logger_config.py`
Configura o sistema de logs.

### `resource_monitor.py`
Coleta uso médio de CPU e memória em tempo real.

### `performance_analyzer.py`
Gera os gráficos comparativos e o resumo estatístico dos resultados.

### `main_benchmark.py`
Executa o benchmark completo e gera a saída final.

---

## Pré-Requisitos

### Banco de dados
- PostgreSQL
- MongoDB

### Python
- Python 3.10+

### Dependências

```
pip install faker psycopg2 pymongo pandas matplotlib psutil
```

---

## Como Executar
Configure as credenciais nos arquivos `db_postgres.py` e `db_mongo.py` certificando de ter as tabelas e coleções criados em ambos os bancos
```
python main_benchmark.py
```

---

## Saídas Geradas

- `logs/resultados_crud.csv`
- `logs/execucao.log`
- `logs/graficos/*.png`
- `logs/graficos/resumo_metricas.txt`

---

## Gráficos Gerados

- Tempo por operação  
- Throughput  
- CPU e Memória  
- Tamanho das bases  

---

## Metodologia do Benchmark

- Monitoramento de CPU/memória  
- Medição de tempo individual  
- Cálculo de throughput  
- Crescimento das bases  
- Sincronização entre bancos  

---

## Contribuições

Pull requests são bem-vindos.

---

## Licença

Escolha a licença desejada.
