-- Удаляем полнотекстовый индекс PostgreSQL, так как теперь используем Elasticsearch
DROP INDEX IF EXISTS idx_events_search;

