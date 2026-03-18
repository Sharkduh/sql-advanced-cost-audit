-- ==========================================================
-- PROJETO: PIPELINE DE AUDITORIA DE CUSTOS (SQL ADVANCED)
-- ANALISTA: Sharkduh
-- OBJETIVO: Identificar vazamentos de caixa e duplicidade
-- ==========================================================

-- 1. CRIAÇÃO DOS SCHEMAS (ORGANIZAÇÃO DE CAMADAS)
CREATE SCHEMA IF NOT EXISTS bronze; -- Dados brutos/Staging
CREATE SCHEMA IF NOT EXISTS gold;   -- Dados refinados/Analytics

-- 2. CAMADA BRONZE: TABELA DE COMPRAS (INGESTÃO)
DROP TABLE IF EXISTS bronze.compras;
CREATE TABLE bronze.compras (
    id_transacao SERIAL PRIMARY KEY,
    fornecedor VARCHAR(100),
    produto VARCHAR(100),
    quantidade INT,
    valor_unitario DECIMAL(10, 2),
    data_compra DATE,
    centro_custo VARCHAR(50)
);

-- 3. CAMADA GOLD: VIEW DE AUDITORIA DE PREÇO (MAVERICK SPENDING)
-- Identifica compras acima do teto negociado de R$ 500,00
CREATE OR REPLACE VIEW gold.v_analise_vazamento_custo AS
SELECT 
    fornecedor,
    produto,
    valor_unitario,
    (valor_unitario - 500.00) AS prejuizo_unidade,
    data_compra
FROM bronze.compras
WHERE valor_unitario > 500.00
ORDER BY prejuizo_unidade DESC;

-- 4. PROCEDURE: DETECÇÃO DE COBRANÇAS DUPLICADAS
-- Busca registros iguais no mesmo dia para o mesmo fornecedor
CREATE OR REPLACE PROCEDURE gold.sp_detectar_duplicados()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Iniciando auditoria de duplicidade...';
    
    PERFORM fornecedor, produto, COUNT(*)
    FROM bronze.compras
    GROUP BY fornecedor, produto, data_compra, valor_unitario
    HAVING COUNT(*) > 1;
    
    IF FOUND THEN
        RAISE NOTICE 'Alerta: Cobranças duplicadas encontradas!';
    ELSE
        RAISE NOTICE 'Nenhuma duplicidade detectada.';
    END IF;
END;
$$;

-- 5. CARGA DE DADOS PARA TESTE (MASSA DE DADOS)
INSERT INTO bronze.compras (fornecedor, produto, quantidade, valor_unitario, data_compra, centro_custo) VALUES
('FORNECEDOR A', 'PEÇA INDUSTRIAL X', 10, 550.00, '2026-03-15', 'PRODUCAO'), -- Maverick Spending
('FORNECEDOR B', 'MATERIA PRIMA Y', 5, 400.00, '2026-03-16', 'LOGISTICA'),
('FORNECEDOR B', 'MATERIA PRIMA Y', 5, 400.00, '2026-03-16', 'LOGISTICA'),   -- Duplicata
('FORNECEDOR C', 'FERRAMENTA Z', 1, 620.00, '2026-03-17', 'MANUTENCAO');      -- Maverick Spending

-- 6. QUERY DE RESULTADO (O QUE O DIRETOR VÊ)
SELECT 
    'ALERTA DE CUSTO' as status,
    fornecedor,
    produto,
    SUM(quantidade * valor_unitario) as valor_total_vazamento
FROM gold.v_analise_vazamento_custo
GROUP BY fornecedor, produto;
