package llm

import (
	"context"
	"fmt"

	"github.com/openai/openai-go/v3"
)

type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	resp, err := c.sdk.Embeddings.New(ctx, openai.EmbeddingNewParams{
		Model: c.cfg.Model,
		Input: openai.EmbeddingNewParamsInputUnion{
			OfArrayOfStrings: texts,
		},
	})
	if err != nil {
		return nil, err
	}

	if len(resp.Data) != len(texts) {
		return nil, fmt.Errorf("embeddings: unexpected number of embeddings: got %d, want %d", len(resp.Data), len(texts))
	}

	vecs := make([][]float32, len(resp.Data))
	for _, d := range resp.Data {
		idx := int(d.Index)
		if idx < 0 || idx >= len(vecs) {
			return nil, fmt.Errorf("embeddings: invalid embedding index: %d", idx)
		}
		vec := make([]float32, len(d.Embedding))
		for i, v := range d.Embedding {
			vec[i] = float32(v)
		}
		vecs[idx] = vec
	}

	return vecs, nil

}
