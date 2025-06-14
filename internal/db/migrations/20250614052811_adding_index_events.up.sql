-- Для оптимизации фильтров по цене
CREATE INDEX idx_events_price ON events(price);

-- Для полнотекстового поиска
CREATE INDEX idx_events_search ON events USING gin(to_tsvector('russian', name || ' ' || description));

