package app

import (
	"context"

	"github.com/AshrafAaref21/go-rag/chat"
	"github.com/AshrafAaref21/go-rag/config"
	"github.com/AshrafAaref21/go-rag/llm"
)

func Run(ctx context.Context, cfg config.Config) error {
	client := llm.New(cfg)
	return chat.RunREPL(ctx, client, chat.Options{
		SystemPromptFile: cfg.SystemPromptFile,
	})
}
