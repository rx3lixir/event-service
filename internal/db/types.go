package db

import "time"

// Event представляет событие в системе
type Event struct {
	Id          int64      `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	CategoryID  int64      `json:"category_id"`
	Date        string     `json:"date"` // Рассмотри возможность использования time.Time или отдельных полей для большей строгости
	Time        string     `json:"time"` // Аналогично, можно использовать более строгий тип или валидацию
	Location    string     `json:"location"`
	Price       float32    `json:"price"`
	Image       string     `json:"image"`  // URL или путь к файлу
	Source      string     `json:"source"` // Источник события
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   *time.Time `json:"updated_at"` // Указатель, чтобы отразить NULL из БД, если не обновлялось
}

// CreateEventParams содержит параметры для создания нового события.
// Используется вместо передачи всей структуры Event, чтобы избежать путаницы с Id, CreatedAt, UpdatedAt.
type CreateEventParams struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	CategoryID  int64   `json:"category_id"`
	Date        string  `json:"date"`
	Time        string  `json:"time"`
	Location    string  `json:"location"`
	Price       float32 `json:"price"`
	Image       string  `json:"image"`
	Source      string  `json:"source"`
}

// UpdateEventParams содержит параметры для обновления существующего события.
// Используй указатели для полей, которые могут быть опционально обновлены (для PATCH),
// или все поля, если это всегда полное обновление (PUT).
// Для простоты, здесь предполагается полное обновление полей, кроме Id, CreatedAt.
type UpdateEventParams struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	CategoryID  int64   `json:"category_id"`
	Date        string  `json:"date"`
	Time        string  `json:"time"`
	Location    string  `json:"location"`
	Price       float32 `json:"price"`
	Image       string  `json:"image"`
	Source      string  `json:"source"`
}

// NewEventFromCreateRequest создает новый экземпляр Event на основе параметров создания.
// CreatedAt устанавливается текущим временем, UpdatedAt остается nil.
func NewEventFromCreateRequest(params CreateEventParams) *Event {
	return &Event{
		Name:        params.Name,
		Description: params.Description,
		CategoryID:  params.CategoryID,
		Date:        params.Date,
		Time:        params.Time,
		Location:    params.Location,
		Price:       params.Price,
		Image:       params.Image,
		Source:      params.Source,
		// CreatedAt будет установлено БД или в методе CreateEvent
		// UpdatedAt остается nil или будет установлено БД/методом CreateEvent
	}
}

// ApplyUpdate применяет изменения из UpdateEventParams к существующему событию.
// Этот метод полезен, если ты хочешь обновить объект Event в памяти перед отправкой в БД,
// или если UpdateEventParams содержит опциональные поля (с указателями).
func (e *Event) ApplyUpdate(params UpdateEventParams) {
	e.Name = params.Name
	e.Description = params.Description
	e.CategoryID = params.CategoryID
	e.Date = params.Date
	e.Time = params.Time
	e.Location = params.Location
	e.Price = params.Price
	e.Image = params.Image
	e.Source = params.Source
	// ID и CreatedAt не должны меняться здесь.
	// UpdatedAt будет обновлен базой данных или методом хранилища.
}
