package tests

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/datastax/astra-db-go/filter"
	"github.com/datastax/astra-db-go/internal/integrationtests/harness"
	"github.com/datastax/astra-db-go/options"
	"github.com/datastax/astra-db-go/results"
	"github.com/datastax/astra-db-go/table"
)

func init() {
	// Register table tests
	t := []harness.IntegrationTest{
		{Name: "TableCreate", Run: TableCreate},
		{Name: "TableInsertOne", Run: TableInsertOne},
		{Name: "TableInsertMany", Run: TableInsertMany},
		{Name: "TableFindOne", Run: TableFindOne},
		{Name: "TableFind", Run: TableFind},
		{Name: "TableFindWithCursor", Run: TableFindWithCursor},
		{Name: "TableFindWithSort", Run: TableFindWithSort},
		{Name: "TableFindWithProjection", Run: TableFindWithProjection},
		{Name: "TableDrop", Run: TableDrop},
	}
	harness.Register(t...)
}

const tableName = "go_test_books"

// TestBook represents a book for table tests
type TestBook struct {
	Title         string   `json:"title"`
	Author        string   `json:"author"`
	NumberOfPages int      `json:"number_of_pages"`
	Rating        float32  `json:"rating"`
	IsCheckedOut  bool     `json:"is_checked_out"`
	Genres        []string `json:"genres"`
}

func TableCreate(e *harness.TestEnv) error {
	ctx := context.Background()
	db := e.DefaultDb()

	definition := table.Definition{
		Columns: map[string]table.Column{
			"title":           table.Text(),
			"author":          table.Text(),
			"number_of_pages": table.Int(),
			"rating":          table.Float(),
			"is_checked_out":  table.Boolean(),
			"genres":          table.List(table.Text()),
		},
		PrimaryKey: table.PrimaryKey{
			PartitionBy: []string{"title"},
		},
	}

	_, err := db.CreateTable(ctx, tableName, definition, options.WithIfNotExists(true))
	return err
}

func TableInsertOne(e *harness.TestEnv) error {
	ctx := context.Background()
	db := e.DefaultDb()
	table := db.Table(tableName)

	book := TestBook{
		Title:         "The Great Gatsby",
		Author:        "F. Scott Fitzgerald",
		NumberOfPages: 180,
		Rating:        4.5,
		IsCheckedOut:  false,
		Genres:        []string{"Fiction", "Classic"},
	}

	resp, err := table.InsertOne(ctx, book)
	if err != nil {
		return err
	}

	if len(resp.Status.InsertedIds) != 1 {
		return fmt.Errorf("expected 1 inserted ID, got %d", len(resp.Status.InsertedIds))
	}

	// The API returns insertedIds as an array of arrays - each ID is an array of primary key values
	// For a single-column primary key like "title", it returns [["The Great Gatsby"]]
	pkValues, ok := resp.Status.InsertedIds[0].([]any)
	if !ok {
		return fmt.Errorf("expected inserted ID to be []any, got %T", resp.Status.InsertedIds[0])
	}
	if len(pkValues) != 1 {
		return fmt.Errorf("expected 1 primary key value, got %d", len(pkValues))
	}
	insertedTitle, ok := pkValues[0].(string)
	if !ok {
		return fmt.Errorf("expected primary key value to be string, got %T", pkValues[0])
	}
	if insertedTitle != book.Title {
		return fmt.Errorf("expected inserted ID %q, got %q", book.Title, insertedTitle)
	}

	return nil
}

func TableInsertMany(e *harness.TestEnv) error {
	ctx := context.Background()
	db := e.DefaultDb()
	table := db.Table(tableName)

	books := []TestBook{
		{
			Title:         "1984",
			Author:        "George Orwell",
			NumberOfPages: 328,
			Rating:        4.7,
			IsCheckedOut:  true,
			Genres:        []string{"Dystopian", "Science Fiction"},
		},
		{
			Title:         "To Kill a Mockingbird",
			Author:        "Harper Lee",
			NumberOfPages: 281,
			Rating:        4.8,
			IsCheckedOut:  false,
			Genres:        []string{"Fiction", "Classic"},
		},
		{
			Title:         "Pride and Prejudice",
			Author:        "Jane Austen",
			NumberOfPages: 279,
			Rating:        4.6,
			IsCheckedOut:  false,
			Genres:        []string{"Romance", "Classic"},
		},
		{
			Title:         "The Catcher in the Rye",
			Author:        "J.D. Salinger",
			NumberOfPages: 234,
			Rating:        4.0,
			IsCheckedOut:  true,
			Genres:        []string{"Fiction", "Coming-of-age"},
		},
		{
			Title:         "Brave New World",
			Author:        "Aldous Huxley",
			NumberOfPages: 311,
			Rating:        4.5,
			IsCheckedOut:  false,
			Genres:        []string{"Dystopian", "Science Fiction"},
		},
	}

	resp, err := table.InsertMany(ctx, books)
	if err != nil {
		return err
	}

	if len(resp.Status.InsertedIds) != len(books) {
		return fmt.Errorf("expected %d inserted IDs, got %d", len(books), len(resp.Status.InsertedIds))
	}

	return nil
}

func TableFindOne(e *harness.TestEnv) error {
	ctx := context.Background()
	db := e.DefaultDb()
	table := db.Table(tableName)

	// Find the book we inserted in TableInsertOne
	var book TestBook
	err := table.FindOne(ctx, filter.Eq("title", "The Great Gatsby")).Decode(&book)
	if err != nil {
		return err
	}

	if book.Title != "The Great Gatsby" {
		return fmt.Errorf("expected title 'The Great Gatsby', got %q", book.Title)
	}
	if book.Author != "F. Scott Fitzgerald" {
		return fmt.Errorf("expected author 'F. Scott Fitzgerald', got %q", book.Author)
	}
	if book.NumberOfPages != 180 {
		return fmt.Errorf("expected 180 pages, got %d", book.NumberOfPages)
	}

	return nil
}

func TableFind(e *harness.TestEnv) error {
	ctx := context.Background()
	db := e.DefaultDb()
	warningHandlerRun := false
	tbl := db.Table(tableName, options.WithWarningHandler(func(w results.Warning) {
		warningHandlerRun = true
	}))

	// Find all books that are not checked out using cursor.All()
	cursor := tbl.Find(ctx, filter.Eq("is_checked_out", false))
	defer cursor.Close(ctx)

	var books []TestBook
	if err := cursor.All(ctx, &books); err != nil {
		return err
	}

	if len(books) == 0 {
		return errors.New("expected to find at least one book")
	}

	// Verify all returned books have is_checked_out = false
	for _, book := range books {
		if book.IsCheckedOut {
			return fmt.Errorf("expected is_checked_out to be false for book %q", book.Title)
		}
	}

	if !warningHandlerRun {
		return errors.New("expected warning handler to run but it did not")
	}

	// We should have a MISSING_INDEX warning because we filtered by a non-indexed column.
	// TODO: We could be more specific and check for the exact warning code/message. For now,
	// just ensure we got some warnings. It's unclear if the code might change in the future.
	if len(cursor.Warnings()) == 0 {
		return errors.New("expected warnings for filtering on non-indexed column but got none")
	}

	// Next, create index and verify warnings go away
	if err := tbl.CreateIndex(ctx, "is_checked_out_idx", "is_checked_out", options.WithIndexIfNotExists(true)); err != nil {
		return err
	}

	// Verify warnings go away after creating the index
	// Find all books that are not checked out using cursor.All()
	idxCursor := tbl.Find(ctx, filter.Eq("is_checked_out", false))
	defer idxCursor.Close(ctx)

	if err := idxCursor.All(ctx, &books); err != nil {
		return err
	}

	if len(idxCursor.Warnings()) > 0 {
		return fmt.Errorf("expected no warnings after index creation. Got: %v", idxCursor.Warnings())
	}

	// Let's double-create that index and make sure it doesn't error out
	if err := tbl.CreateIndex(ctx, "is_checked_out_idx", "is_checked_out", options.WithIndexIfNotExists(true)); err != nil {
		return err
	}

	// Finally - drop index
	if err := db.DropTableIndex(ctx, "is_checked_out_idx"); err != nil {
		return err
	}

	return nil
}

// TableFindWithCursor demonstrates iterating with Next/Decode pattern
func TableFindWithCursor(e *harness.TestEnv) error {
	ctx := context.Background()
	db := e.DefaultDb()
	tbl := db.Table(tableName)

	// Find all books using cursor iteration
	cursor := tbl.Find(ctx, filter.F{})
	defer cursor.Close(ctx)

	var books []TestBook
	for cursor.Next(ctx) {
		var book TestBook
		if err := cursor.Decode(&book); err != nil {
			return fmt.Errorf("decode error: %w", err)
		}
		books = append(books, book)
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	if len(books) == 0 {
		return errors.New("expected to find at least one book using cursor iteration")
	}

	return nil
}

func TableFindWithSort(e *harness.TestEnv) error {
	ctx := context.Background()
	db := e.DefaultDb()
	tbl := db.Table(tableName)

	// Find books sorted by rating descending using cursor.All()
	cursor := tbl.Find(ctx, filter.F{},
		options.WithSort(map[string]any{"rating": options.SortDescending}),
		options.WithLimit(3),
	)
	defer cursor.Close(ctx)

	var books []TestBook
	if err := cursor.All(ctx, &books); err != nil {
		return err
	}

	if len(books) == 0 {
		return errors.New("expected to find at least one book")
	}

	// Verify books are sorted by rating descending
	for i := 1; i < len(books); i++ {
		if books[i].Rating > books[i-1].Rating {
			return fmt.Errorf("expected books to be sorted by rating descending, but book %q (%.1f) comes after %q (%.1f)",
				books[i].Title, books[i].Rating, books[i-1].Title, books[i-1].Rating)
		}
	}

	return nil
}

func TableFindWithProjection(e *harness.TestEnv) error {
	ctx := context.Background()
	db := e.DefaultDb()
	tbl := db.Table(tableName)

	// Find books with only title and author using cursor.All()
	cursor := tbl.Find(ctx, filter.F{},
		options.WithProjection(map[string]bool{"title": true, "author": true}),
		options.WithLimit(1),
	)
	defer cursor.Close(ctx)

	var books []map[string]any
	if err := cursor.All(ctx, &books); err != nil {
		return err
	}

	if len(books) == 0 {
		return errors.New("expected to find at least one book")
	}

	book := books[0]

	// Verify only title and author are present
	if _, ok := book["title"]; !ok {
		return errors.New("expected title to be in projection")
	}
	if _, ok := book["author"]; !ok {
		return errors.New("expected author to be in projection")
	}

	// Check that other fields are not present (or are zero/nil)
	if rating, ok := book["rating"]; ok && rating != nil && !reflect.ValueOf(rating).IsZero() {
		return fmt.Errorf("expected rating to be excluded from projection, got %v", rating)
	}

	return nil
}

func TableDrop(e *harness.TestEnv) error {
	ctx := context.Background()
	db := e.DefaultDb()
	return db.DropTable(ctx, tableName)
}
