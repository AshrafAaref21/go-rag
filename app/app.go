package app

import (
	"context"
	"log"
	"os"

	"github.com/AshrafAaref21/go-rag/chat"
	"github.com/AshrafAaref21/go-rag/config"
	"github.com/AshrafAaref21/go-rag/llm"
	"github.com/AshrafAaref21/go-rag/vector"
	"github.com/AshrafAaref21/go-rag/vector/pgvector"
)

func Run(ctx context.Context, cfg config.Config) error {
	client := llm.New(cfg)
	logger := log.New(os.Stderr, "[rag] ", log.LstdFlags)

	store, err := openStore(ctx, cfg)
	if err != nil {
		logger.Printf("vector store disabled: %v", err)
	}

	if store != nil {
		defer store.Close()
		logger.Printf("vector store ready")
	}

	return chat.RunREPL(ctx, client, chat.Options{
		SystemPromptFile: cfg.SystemPromptFile,
	})
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
