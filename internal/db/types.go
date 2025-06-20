package db

import "time"

// Event представляет событие в системе
type Event struct {
	Id          int64
	Name        string
	Description string
	CategoryID  int64
	Date        string
	Time        string
	Location    string
	Price       float32
	Image       string
	Source      string
	CreatedAt   time.Time
	UpdatedAt   *time.Time
}

// CreateEventParams содержит параметры для создания нового события
type CreateEventParams struct {
	Name        string
	Description string
	CategoryID  int64
	Date        string
	Time        string
	Location    string
	Price       float32
	Image       string
	Source      string
}

// UpdateEventParams содержит параметры для обновления существующего события
type UpdateEventParams struct {
	Name        string
	Description string
	CategoryID  int64
	Date        string
	Time        string
	Location    string
	Price       float32
	Image       string
	Source      string
}

// Category представляет категорию событий
type Category struct {
	Id        int
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CreateCategoryReq представляет запрос на создание новой категории
type CreateCategoryReq struct {
	Name string
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

// NewCategory создает новую категорию из запроса
func NewCategory(req *CreateCategoryReq) *Category {
	return &Category{
		Name:      req.Name,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}
