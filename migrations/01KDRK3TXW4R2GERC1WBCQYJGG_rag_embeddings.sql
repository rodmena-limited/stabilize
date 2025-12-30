-- migration: rag_embeddings
-- id: 01KDRK3TXW4R2GERC1WBCQYJGG

-- migrate: up

CREATE TABLE rag_embeddings (
    id SERIAL PRIMARY KEY,
    doc_id VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    embedding JSONB NOT NULL,
    embedding_model VARCHAR(100) NOT NULL,
    chunk_index INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(doc_id, chunk_index, embedding_model)
);

CREATE INDEX idx_rag_embeddings_doc ON rag_embeddings(doc_id);
CREATE INDEX idx_rag_embeddings_model ON rag_embeddings(embedding_model);

-- migrate: down

DROP INDEX IF EXISTS idx_rag_embeddings_model;
DROP INDEX IF EXISTS idx_rag_embeddings_doc;
DROP TABLE IF EXISTS rag_embeddings;
