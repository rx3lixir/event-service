-- Удаление индекса для фильтрации по цене
DROP INDEX IF EXISTS idx_events_price;

-- Удаление индекса для полнотекстового поиска
DROP INDEX IF EXISTS idx_events_search;

