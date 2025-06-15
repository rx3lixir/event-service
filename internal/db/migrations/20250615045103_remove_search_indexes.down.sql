-- Восстанавливаем полнотекстовый индекс (на случай отката)
CREATE INDEX idx_events_search ON events USING gin(to_tsvector('russian', name || ' ' || description));

