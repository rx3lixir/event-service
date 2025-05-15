-- Удаляем таблицу events и связанные с ней индексы
DROP INDEX IF EXISTS idx_events_date;
DROP INDEX IF EXISTS idx_events_category_id;
DROP TABLE IF EXISTS events;

-- Удаляем таблицу категорий
DROP TABLE IF EXISTS categories;

