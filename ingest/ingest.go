package ingest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/AshrafAaref21/go-rag/llm"
	"github.com/AshrafAaref21/go-rag/vector"
)

const (
	defaultChunkSize    = 1000
	defaultChunkOverlap = 200
)

type Options struct {
	SourceDir    string
	ProcessedDir string
	ChunkSize    int
	ChunkOverlap int
}

func processOne(ctx context.Context, path string, opts Options, embedder llm.Embedder, store vector.Store) error {
	if !supportedFormat(path) {
		return fmt.Errorf("unsupported file format: %s", path)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", path, err)
	}

	_, err = processContent(ctx, path, raw, opts, embedder, store)
	return err
}

func processContent(ctx context.Context, source string, content []byte, opts Options, embedder llm.Embedder, store vector.Store) (int, error) {
	if embedder == nil {
		return 0, errors.New("embedder is nil")
	}
	if store == nil {
		return 0, errors.New("store is nil")
	}

	base := filepath.Base(source)
	if !supportedFormat(base) {
		return 0, fmt.Errorf("unsupported file format: %s", base)
	}

	size := opts.ChunkSize
	if size <= 0 {
		size = defaultChunkSize
	}

	overlap := opts.ChunkOverlap
	if overlap < 0 {
		overlap = defaultChunkOverlap
	}

	text := strings.TrimSpace(string(content))

	if text == "" {
		return 0, fmt.Errorf("file %s is empty", source)
	}

	chunks := chunk(text, size, overlap)
	if len(chunks) == 0 {
		return 0, fmt.Errorf("no chunks generated for file %s", source)
	}

	embeddings, err := embedder.Embed(ctx, chunks)
	if err != nil {
		return 0, fmt.Errorf("embed: failed to generate embeddings for file %s: %w", source, err)
	}

	if len(embeddings) != len(chunks) {
		return 0, fmt.Errorf(
			"embed: number of embeddings (%d) does not match number of chunks (%d) for file %s", len(embeddings), len(chunks), source,
		)
	}

	if err := store.DeleteBySource(ctx, source); err != nil {
		return 0, fmt.Errorf("store: failed to delete existing entries for file %s: %w", source, err)
	}

	ingestedAt := time.Now().UTC().Format(time.RFC3339)

	docs := make([]vector.Document, len(chunks))

	for i, chunk := range chunks {
		docs[i] = vector.Document{
			ID:        fmt.Sprintf("%s#%d", base, i),
			Content:   chunk,
			Embedding: embeddings[i],
			Metadata: map[string]string{
				"source":      source,
				"chunk_index": fmt.Sprintf("%d", i),
				"chunks":      fmt.Sprintf("%d", len(chunks)),
				"ingested_at": ingestedAt,
			},
		}
	}

	if err := store.Upsert(ctx, docs); err != nil {
		return 0, err
	}

	return len(docs), nil
}

func supportedFormat(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".txt", ".md", ".markdown":
		return true
	default:
		return false
	}
}
