package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *PostgresStore) CreateCategory(parentCtx context.Context, category *Category) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*3)
	defer cancel()

	query := `INSERT INTO categories (name) VALUES ($1) RETURNING id, created_at, updated_at`

	err := s.db.QueryRow(ctx, query, category.Name).Scan(&category.Id, &category.CreatedAt, &category.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to create category: %w", err)
	}

	return nil
}

func (s *PostgresStore) ListCategories(parentCtx context.Context) ([]*Category, error) {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*3)
	defer cancel()

	rows, err := s.db.Query(ctx,
		"SELECT id, name, created_at, updated_at FROM categories")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	categories := []*Category{}

	for rows.Next() {
		category := new(Category)
		if err := rows.Scan(&category.Id, &category.Name, &category.CreatedAt, &category.UpdatedAt); err != nil {
			return nil, err
		}

		categories = append(categories, category)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating category rows: %w", err)
	}

	return categories, nil
}

func (s *PostgresStore) GetCategoryByID(parentCtx context.Context, id int) (*Category, error) {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*3)
	defer cancel()

	row := s.db.QueryRow(ctx, "SELECT id, name, created_at, updated_at FROM categories WHERE id = $1", id)

	category := new(Category)
	err := row.Scan(
		&category.Id,
		&category.Name,
		&category.CreatedAt,
		&category.UpdatedAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("category %d not found", id)
		}
		return nil, fmt.Errorf("failed to get category by id %d: %w", id, err)
	}

	return category, nil
}

func (s *PostgresStore) UpdateCategory(parentCtx context.Context, category *Category) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*3)
	defer cancel()

	var exists bool

	// Проверка существования категории
	err := s.db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM categories WHERE id = $1)", category.Id).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("category with ID %d not found", category.Id)
	}

	query := `
		UPDATE categories
		SET name = $1, updated_at = NOW()
		WHERE id = $2
		RETURNING updated_at
	`
	err = s.db.QueryRow(
		ctx,
		query,
		category.Name,
		category.Id).Scan(&category.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to update category %d: %w", category.Id, err)
	}

	return nil
}

func (s *PostgresStore) DeleteCategory(parentCtx context.Context, id int) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*3)
	defer cancel()

	cmdTag, err := s.db.Exec(ctx, "DELETE FROM categories WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to delete category %d: %w", id, err)
	}

	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("category with ID %d not found for deletion", id)
	}

	return nil
}
