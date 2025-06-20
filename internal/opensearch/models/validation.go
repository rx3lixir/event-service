package models

import (
	"fmt"
	"strings"
)

// Validate проверяет корректность EventDocument
func (e *EventDocument) Validate() error {
	var errors []string

	if e.ID <= 0 {
		errors = append(errors, "id must be positive")
	}

	if strings.TrimSpace(e.Name) == "" {
		errors = append(errors, "name is required")
	}

	if e.CategoryID <= 0 {
		errors = append(errors, "category_id must be positive")
	}

	if e.Price < 0 {
		errors = append(errors, "price cannot be negative")
	}

	if len(errors) > 0 {
		return fmt.Errorf("validation errors: %s", strings.Join(errors, ", "))
	}

	return nil
}

// ValidateForIndexing проверяет готовность документа к индексации
func (e *EventDocument) ValidateForIndexing() error {
	if err := e.Validate(); err != nil {
		return err
	}

	// Дополнительные проверки для индексации
	if e.CreatedAt.IsZero() {
		return fmt.Errorf("created_at is required for indexing")
	}

	return nil
}
