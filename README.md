🚀 SQL Advanced: Cost Leakage & Audit Pipeline

Este projeto implementa uma pipeline de Engenharia de Dados e Auditoria Financeira utilizando PostgreSQL no Debian 12. O objetivo é identificar ineficiências de custo, pagamentos duplicados e descumprimento de contratos em uma operação de suprimentos/PCP.

📌 Problemas Reais Resolvidos

Maverick Spending: Compras realizadas acima do preço teto negociado em contrato.

Duplicate Invoicing: Falhas sistêmicas que geram cobranças duplicadas para o mesmo serviço/produto.

Data Cleaning: Padronização de nomes de fornecedores e saneamento de strings para garantir a integridade da análise.

🏗️ Arquitetura de Dados (Medallion)

O projeto segue o padrão de camadas para garantir a governança:

Bronze (Staging): Ingestão de dados brutos (bronze.compras).

Gold (Analytics): Camada refinada com visões de auditoria (gold.v_analise_vazamento_custos) e dados processados (gold.faturas_processadas).

Quarentena: Isolamento de registros críticos para revisão humana, evitando prejuízo financeiro automático.

🛠️ Tecnologias e Técnicas Utilizadas

PostgreSQL 15 (Rodando em ambiente Debian GNU/Linux 12).

Window Functions: Uso de COUNT(*) OVER(PARTITION BY...) para detecção de duplicidade sem a necessidade de múltiplos JOINS.

Common Table Expressions (CTEs): Para maior legibilidade e organização da lógica de limpeza.

Stored Procedures: Automação do processo de ETL e movimentação de dados entre camadas.

Índices BRIN: Otimização de performance para grandes volumes de dados de data, mantendo baixo consumo de memória no sistema.

📊 Resultados Obtidos (Impacto de Negócio)

Através da query de Resumo Executivo, o sistema é capaz de reportar:

Volume Financeiro Protegido: Total de reais que deixaram de ser pagos indevidamente.

Ticket Médio de Erro: Identificação da gravidade das falhas por categoria.

Ranking de Risco de Fornecedores: Visibilidade sobre quais parceiros comerciais possuem maior índice de divergência.

🚀 Como Executar

Certifique-se de ter o PostgreSQL instalado no seu Debian.

Execute o script de criação de schemas e tabelas.

Popule a camada bronze com os dados de transações.

Chame a automação:

CALL gold.pr_processar_auditoria();
