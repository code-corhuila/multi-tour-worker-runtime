package httpclient

import "context"

// Meta is the pagination block of a list answer.
type Meta struct {
	Page       int `json:"page"`
	Limit      int `json:"limit"`
	Total      int `json:"total"`
	TotalPages int `json:"totalPages"`
}

// Page is a paginated list answer: {data, meta}.
type Page[T any] struct {
	Data []T  `json:"data"`
	Meta Meta `json:"meta"`
}

// List reads one page of a collection; path already carries page and limit.
func List[T any](ctx context.Context, c *Client, path string) (Page[T], error) {
	var p Page[T]
	err := c.Get(ctx, path, &p)
	return p, err
}
