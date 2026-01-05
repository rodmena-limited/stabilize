-- migration: add_performance_indexes
-- id: 01KE39HEAN221S7AWNS4A0H750

-- migrate: up

-- Priority 1: Critical performance indexes
-- Composite index for message polling - the most frequent query in the system
CREATE INDEX IF NOT EXISTS idx_queue_message_polling
    ON queue_messages(deliver_at, locked_until, attempts);

-- Index for dashboard/list sorting by creation time
CREATE INDEX IF NOT EXISTS idx_execution_created_at
    ON pipeline_executions(created_at DESC);

-- Priority 2: Composite indexes for common query patterns
-- Combined filter for retrieve_by_application queries
CREATE INDEX IF NOT EXISTS idx_execution_application_status
    ON pipeline_executions(application, status);

-- Combined filter for pipeline config queries
CREATE INDEX IF NOT EXISTS idx_execution_config_status
    ON pipeline_executions(pipeline_config_id, status);

-- Stage lookup with ref_id optimization
CREATE INDEX IF NOT EXISTS idx_stage_execution_ref
    ON stage_executions(execution_id, ref_id);

-- Synthetic stage queries optimization
CREATE INDEX IF NOT EXISTS idx_stage_synthetic
    ON stage_executions(execution_id, parent_stage_id);

-- Priority 3: RAG embeddings composite index for ordered retrieval
CREATE INDEX IF NOT EXISTS idx_rag_model_doc_chunk
    ON rag_embeddings(embedding_model, doc_id, chunk_index);


-- migrate: down

DROP INDEX IF EXISTS idx_queue_message_polling;
DROP INDEX IF EXISTS idx_execution_created_at;
DROP INDEX IF EXISTS idx_execution_application_status;
DROP INDEX IF EXISTS idx_execution_config_status;
DROP INDEX IF EXISTS idx_stage_execution_ref;
DROP INDEX IF EXISTS idx_stage_synthetic;
DROP INDEX IF EXISTS idx_rag_model_doc_chunk;
