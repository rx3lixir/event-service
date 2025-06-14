package db

import (
	"fmt"
	"strings"
)

// buildFilteredQuery строит SQL запрос с WHERE условиями на основе фильтра.
// Возвращает готовый SQL запрос и массив аргументов для защиты от SQL injection.
func (s *PostgresStore) buildFilteredQuery(filter *EventFilter) (string, []any) {
	// Базовый SELECT запрос с теми же полями что и в других методах
	baseQuery := `SELECT id, name, description, category_id, date, time, location, price, image, source, created_at, updated_at FROM events`

	var conditions []string
	var args []any
	argIndex := 1

	// == Условия == \\

	// Фильтр по категориям (IN clause для множественного выбора)
	if len(filter.CategoryIDs) > 0 {
		placeholders := make([]string, len(filter.CategoryIDs))
		for i, categoryID := range filter.CategoryIDs {
			placeholders[i] = fmt.Sprintf("$%d", argIndex)
			args = append(args, categoryID)
			argIndex++
		}
		conditions = append(conditions, fmt.Sprintf("category_id IN (%s)", strings.Join(placeholders, ",")))
	}

	// Фильтр по минимальной цене
	if filter.MinPrice != nil {
		conditions = append(conditions, fmt.Sprintf("price >= $%d", argIndex))
		args = append(args, *filter.MinPrice)
		argIndex++
	}

	// Фильтр по максимальной цене
	if filter.MaxPrice != nil {
		conditions = append(conditions, fmt.Sprintf("price <= $%d", argIndex))
		args = append(args, *filter.MaxPrice)
		argIndex++
	}

	// Фильтр по дате от (больше или равно)
	if filter.DateFrom != nil {
		dateToStr := filter.DateFrom.Format("2006-01-02")
		conditions = append(conditions, fmt.Sprintf("date >= $%d", argIndex))
		args = append(args, dateToStr)
		argIndex++
	}

	// Фильтр по дате от (меньше или равно)
	if filter.DateFrom != nil {
		dateToStr := filter.DateTo.Format("2006-01-02")
		conditions = append(conditions, fmt.Sprintf("date <= $%d", argIndex))
		args = append(args, dateToStr)
		argIndex++
	}

	// Фильтр по точному совпадению локации
	if filter.Location != nil {
		conditions = append(conditions, fmt.Sprintf("location = $%d", argIndex))
		args = append(args, *filter.Location)
		argIndex++
	}

	// Фильтр по точному совпадению источника
	if filter.Source != nil {
		conditions = append(conditions, fmt.Sprintf("source = $%d", argIndex))
		args = append(args, *filter.Source)
		argIndex++
	}

	// Полнотекстовый поиск по названию и описанию (регистронезависимый)
	if filter.SearchText != nil {
		searchPattern := "%" + *filter.SearchText + "%"
		conditions = append(conditions, fmt.Sprintf("(name ILIKE $%d OR description ILIKE $%d)", argIndex, argIndex+1))
		args = append(args, searchPattern, searchPattern)
		argIndex += 2
	}

	// == Собираем запрос == \\

	// Добавляем WHERRE условия, если есть
	if len(conditions) > 0 {
		baseQuery += " WHERE " + strings.Join(conditions, " AND ")
	}

	// Сортировка по дате создания (новые сверху)
	baseQuery += " ORDER BY created_at DESC"

	// Пагинация: LIMIT
	if filter.Limit != nil {
		baseQuery += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, *filter.Limit)
		argIndex++
	}

	// Пагинация: OFFSET
	if filter.Limit != nil {
		baseQuery += fmt.Sprintf(" OFFSET $%d", argIndex)
		args = append(args, *filter.Limit)
		argIndex++
	}

	return baseQuery, args
}

// buildCountQuery строит запрос для подсчета общего количества записей с учетом фильтров.
// Используется для реализации пагинации с информацией об общем количестве.
func (s *PostgresStore) buildCountQuery(filter *EventFilter) (string, []any) {
	baseQuery := `SELECT COUNT(*) FROM events`

	var conditions []string
	var args []any
	argIndex := 1

	// Применяем те же условия фильтрации, что и в основном запросе
	// (копируем логику из buildFilteredQuery, но без ORDER BY, LIMIT, OFFSET)

	if len(filter.CategoryIDs) > 0 {
		placeholders := make([]string, len(filter.CategoryIDs))
		for i, categoryID := range filter.CategoryIDs {
			placeholders[i] = fmt.Sprintf("$%d", argIndex)
			args = append(args, categoryID)
			argIndex++
		}
		conditions = append(conditions, fmt.Sprintf("category_id IN (%s)", strings.Join(placeholders, ",")))
	}

	if filter.MinPrice != nil {
		conditions = append(conditions, fmt.Sprintf("price >= $%d", argIndex))
		args = append(args, *filter.MinPrice)
		argIndex++
	}

	if filter.MaxPrice != nil {
		conditions = append(conditions, fmt.Sprintf("price <= $%d", argIndex))
		args = append(args, *filter.MaxPrice)
		argIndex++
	}

	if filter.DateFrom != nil {
		dateFromStr := filter.DateFrom.Format("2006-01-02")
		conditions = append(conditions, fmt.Sprintf("date >= $%d", argIndex))
		args = append(args, dateFromStr)
		argIndex++
	}

	if filter.DateTo != nil {
		dateToStr := filter.DateTo.Format("2006-01-02")
		conditions = append(conditions, fmt.Sprintf("date <= $%d", argIndex))
		args = append(args, dateToStr)
		argIndex++
	}

	if filter.Location != nil {
		conditions = append(conditions, fmt.Sprintf("location = $%d", argIndex))
		args = append(args, *filter.Location)
		argIndex++
	}

	if filter.Source != nil {
		conditions = append(conditions, fmt.Sprintf("source = $%d", argIndex))
		args = append(args, *filter.Source)
		argIndex++
	}

	if filter.SearchText != nil {
		searchPattern := "%" + *filter.SearchText + "%"
		conditions = append(conditions, fmt.Sprintf("(name ILIKE $%d OR description ILIKE $%d)", argIndex, argIndex+1))
		args = append(args, searchPattern, searchPattern)
		argIndex += 2
	}

	// Добавляем WHERE условия
	if len(conditions) > 0 {
		baseQuery += " WHERE " + strings.Join(conditions, " AND ")
	}

	return baseQuery, args
}

// validateFilter проверяет корректность параметров фильтра.
// Возвращает ошибку, если найдены некорректные значения.
func validateFilter(filter *EventFilter) error {
	if filter == nil {
		return fmt.Errorf("filter cannot be nil")
	}

	// Проверяем лимит
	if filter.Limit != nil && *filter.Limit <= 0 {
		return fmt.Errorf("limit must be positive, got: %d", *filter.Limit)
	}

	// Проверяем offset
	if filter.Offset != nil && *filter.Offset < 0 {
		return fmt.Errorf("offset cannot be negative, got: %d", *filter.Offset)
	}

	// Проверяем диапазон цен
	if filter.MinPrice != nil && *filter.MinPrice < 0 {
		return fmt.Errorf("min_price cannot be negative, got: %f", *filter.MinPrice)
	}

	if filter.MaxPrice != nil && *filter.MaxPrice < 0 {
		return fmt.Errorf("max_price cannot be negative, got: %f", *filter.MaxPrice)
	}

	if filter.MinPrice != nil && filter.MaxPrice != nil && *filter.MinPrice > *filter.MaxPrice {
		return fmt.Errorf("min_price (%f) cannot be greater than max_price (%f)", *filter.MinPrice, *filter.MaxPrice)
	}

	// Проверяем диапазон дат
	if filter.DateFrom != nil && filter.DateTo != nil && filter.DateFrom.After(*filter.DateTo) {
		return fmt.Errorf("date_from (%s) cannot be after date_to (%s)",
			filter.DateFrom.Format("2006-01-02"),
			filter.DateTo.Format("2006-01-02"))
	}

	// Проверяем разумные лимиты
	if filter.Limit != nil && *filter.Limit > 1000 {
		return fmt.Errorf("limit too large, maximum allowed: 1000, got: %d", *filter.Limit)
	}

	return nil
}
