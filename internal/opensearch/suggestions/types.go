package suggestions

import (
	"fmt"
	"strings"
)

type Request struct {
	Query      string   `json:"query"`
	MaxResults int      `json:"max_results"`
	Fields     []string `json:"fields"`
}

func (r *Request) Validate() error {
	if strings.TrimSpace(r.Query) == "" {
		return fmt.Errorf("query is required")
	}

	if len(r.Query) < 2 {
		return fmt.Errorf("query must be at least 2 characters long")
	}

	if r.MaxResults <= 0 {
		r.MaxResults = 10
	}

	if r.MaxResults > 50 {
		return fmt.Errorf("max_results cannot exceed 50")
	}

	if len(r.Fields) == 0 {
		r.Fields = []string{"name", "location"}
	}

	// Валидируем поля
	validFields := map[string]bool{
		"name":     true,
		"location": true,
	}

	for _, field := range r.Fields {
		if !validFields[field] {
			return fmt.Errorf("invalid field: %s", field)
		}
	}

	return nil
}

type Response struct {
	Suggestions []Suggestion `json:"suggestions"`
	Query       string       `json:"query"`
	Total       int          `json:"total"`
}

type Suggestion struct {
	Text     string  `json:"text"`
	Score    float64 `json:"score"`
	Type     string  `json:"type"`
	Category string  `json:"category,omitempty"`
	EventID  *int64  `json:"event_id,omitempty"`
}

type SuggestionType string

const (
	SuggestionTypeEvent    SuggestionType = "event"
	SuggestionTypeLocation SuggestionType = "location"
	SuggestionTypeGeneral  SuggestionType = "general"
)

func (s *Suggestion) SetType(field string) {
	switch field {
	case "name":
		s.Type = string(SuggestionTypeEvent)
	case "location":
		s.Type = string(SuggestionTypeLocation)
	default:
		s.Type = string(SuggestionTypeGeneral)
	}
}
