package app

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/AshrafAaref21/go-rag/chat"
	"github.com/AshrafAaref21/go-rag/config"
	"github.com/AshrafAaref21/go-rag/ingest"
	"github.com/AshrafAaref21/go-rag/llm"
	"github.com/AshrafAaref21/go-rag/vector"
	"github.com/AshrafAaref21/go-rag/vector/pgvector"
)

func Run(parent context.Context, cfg config.Config) error {
	client := llm.New(cfg)
	logger := log.New(os.Stderr, "[rag] ", log.LstdFlags)

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	embedder := llm.NewEmbedder(cfg)

	store, err := openStore(ctx, cfg)
	if err != nil {
		logger.Printf("vector store disabled: %v", err)
	}

	var wg sync.WaitGroup
	if store != nil {
		wg.Go(func() {

			opts := ingest.Options{
				SourceDir:    cfg.IngestDir,
				ProcessedDir: cfg.ProcessedDir,
			}

			if err := ingest.Watch(ctx, opts, embedder, store, logger); err != nil {
				logger.Printf("ingest watcher error: %v", err)
			}

			defer store.Close()
		})
		logger.Printf("Watching directory %s for new files to ingest...", cfg.IngestDir)
	}

	if store != nil {
		defer store.Close()
		logger.Printf("vector store ready")
	}

	repl := chat.RunREPL(ctx, client, chat.Options{
		SystemPromptFile: cfg.SystemPromptFile,
	})

	cancel()  // Cancel the context to stop the watcher when REPL exits
	wg.Wait() // Wait for the watcher goroutine to finish

	return repl
}

func openStore(ctx context.Context, cfg config.Config) (vector.Store, error) {
	if cfg.DatabaseURL == "" {
		return nil, nil
	}
	s, err := pgvector.New(ctx, pgvector.Options{
		DSN:          cfg.DatabaseURL,
		EmbeddingDim: cfg.EmbeddingDim,
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}
