package ingest

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AshrafAaref21/go-rag/llm"
	"github.com/AshrafAaref21/go-rag/vector"
	"github.com/fsnotify/fsnotify"
)

const debounceDelay = 500 * time.Millisecond

func Watch(ctx context.Context, opts Options, embedder llm.Embedder, store vector.Store, logger *log.Logger) error {
	if filepath.Clean(opts.SourceDir) == filepath.Clean(opts.ProcessedDir) {
		return errors.New("source and processed directories must differ")
	}
	if err := os.MkdirAll(opts.SourceDir, 0o755); err != nil {
		return fmt.Errorf("create source dir: %w", err)
	}
	if err := os.MkdirAll(opts.ProcessedDir, 0o755); err != nil {
		return fmt.Errorf("create processed dir: %w", err)
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	defer w.Close()

	if err := w.Add(opts.SourceDir); err != nil {
		return fmt.Errorf("watch source dir: %w", err)
	}

	processedAbs, err := filepath.Abs(opts.ProcessedDir)
	if err != nil {
		return fmt.Errorf("resolve processed dir: %w", err)
	}

	handle := func(path string) {
		if err := processOne(ctx, path, opts, embedder, store); err != nil {
			logger.Printf("process %s: %v", filepath.Base(path), err)
			return
		}
		dst := filepath.Join(opts.ProcessedDir, filepath.Base(path))
		if err := os.Rename(path, dst); err != nil {
			logger.Printf("move %s to processed: %v", filepath.Base(path), err)
			return
		}
		logger.Printf("ingested %s", filepath.Base(path))
	}

	entries, err := os.ReadDir(opts.SourceDir)
	if err != nil {
		return fmt.Errorf("read source dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		// Process existing files in the source directory
		// Bad idea to process them all at once, better to schedule them with debounce
		go handle(filepath.Join(opts.SourceDir, e.Name()))
	}

	var (
		timersMu sync.Mutex
		timers   = make(map[string]*time.Timer)
	)

	schedule := func(path string) {
		timersMu.Lock()
		defer timersMu.Unlock()
		if t, ok := timers[path]; ok {
			t.Reset(debounceDelay)
			return
		}

		timers[path] = time.AfterFunc(debounceDelay, func() {
			timersMu.Lock()
			delete(timers, path)
			timersMu.Unlock()
			handle(path)
		})
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-w.Events:
			if !ok {
				return nil
			}
			if ev.Op&(fsnotify.Create|fsnotify.Write) == 0 {
				continue
			}
			if !shouldProcess(ev.Name, processedAbs) {
				continue
			}
			schedule(ev.Name)
		case err, ok := <-w.Errors:
			if !ok {
				return nil
			}
			logger.Printf("watcher error: %v", err)
		}
	}
}

func shouldProcess(path, processedAbs string) bool {
	if strings.HasPrefix(filepath.Base(path), ".") {
		return false
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	if processedAbs != "" && strings.HasPrefix(abs, processedAbs+string(filepath.Separator)) {
		return false
	}
	return true
}

// Better Concurrent Processing with Worker Pool and Retry Mechanism.

// package ingest

// import (
// 	"context"
// 	"errors"
// 	"fmt"
// 	"log"
// 	"os"
// 	"path/filepath"
// 	"strings"
// 	"sync"
// 	"time"

// 	"github.com/AshrafAaref21/go-rag/llm"
// 	"github.com/AshrafAaref21/go-rag/vector"
// 	"github.com/fsnotify/fsnotify"
// )

// const debounceDelay = 500 * time.Millisecond

// const (
// 	maxWorkers = 8
// 	queueSize  = 256
// 	retryDelay = 200 * time.Millisecond
// 	maxRetry   = 20
// )

// func Watch(ctx context.Context, opts Options, embedder llm.Embedder, store vector.Store, logger *log.Logger) error {
// 	if filepath.Clean(opts.SourceDir) == filepath.Clean(opts.ProcessedDir) {
// 		return errors.New("source and processed directories must differ")
// 	}
// 	if err := os.MkdirAll(opts.SourceDir, 0o755); err != nil {
// 		return fmt.Errorf("create source dir: %w", err)
// 	}
// 	if err := os.MkdirAll(opts.ProcessedDir, 0o755); err != nil {
// 		return fmt.Errorf("create processed dir: %w", err)
// 	}

// 	w, err := fsnotify.NewWatcher()
// 	if err != nil {
// 		return fmt.Errorf("create watcher: %w", err)
// 	}
// 	defer w.Close()

// 	if err := w.Add(opts.SourceDir); err != nil {
// 		return fmt.Errorf("watch source dir: %w", err)
// 	}

// 	processedAbs, err := filepath.Abs(opts.ProcessedDir)
// 	if err != nil {
// 		return fmt.Errorf("resolve processed dir: %w", err)
// 	}

// 	handle := func(path string) {
// 		if err := processOne(ctx, path, opts, embedder, store); err != nil {
// 			logger.Printf("process %s: %v", filepath.Base(path), err)
// 			return
// 		}
// 		dst := filepath.Join(opts.ProcessedDir, filepath.Base(path))
// 		if err := os.Rename(path, dst); err != nil {
// 			logger.Printf("move %s to processed: %v", filepath.Base(path), err)
// 			return
// 		}
// 		logger.Printf("ingested %s", filepath.Base(path))
// 	}

// 	jobs := make(chan string, queueSize)

// 	var (
// 		retryMu     sync.Mutex
// 		retryingSet = make(map[string]struct{})
// 	)

// 	scheduleRetry := func(path string) {
// 		retryMu.Lock()
// 		if _, exists := retryingSet[path]; exists {
// 			retryMu.Unlock()
// 			return
// 		}
// 		retryingSet[path] = struct{}{}
// 		retryMu.Unlock()

// 		go func(p string) {
// 			defer func() {
// 				retryMu.Lock()
// 				delete(retryingSet, p)
// 				retryMu.Unlock()
// 			}()

// 			for i := 0; i < maxRetry; i++ {
// 				select {
// 				case <-ctx.Done():
// 					return
// 				case jobs <- p:
// 					return
// 				case <-time.After(retryDelay):
// 				}
// 			}

// 			logger.Printf("drop after retries: %s", filepath.Base(p))
// 		}(path)
// 	}

// 	enqueue := func(path string) {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case jobs <- path:
// 			return
// 		default:
// 			scheduleRetry(path)
// 		}
// 	}

// 	for i := 0; i < maxWorkers; i++ {
// 		go func() {
// 			for {
// 				select {
// 				case <-ctx.Done():
// 					return
// 				case path := <-jobs:
// 					handle(path)
// 				}
// 			}
// 		}()
// 	}

// 	handleOldFiles(ctx, opts.SourceDir, enqueue, logger)

// 	var (
// 		timersMu sync.Mutex
// 		timers   = make(map[string]*time.Timer)
// 	)

// 	schedule := func(path string) {
// 		timersMu.Lock()
// 		defer timersMu.Unlock()
// 		if t, ok := timers[path]; ok {
// 			t.Reset(debounceDelay)
// 			return
// 		}

// 		timers[path] = time.AfterFunc(debounceDelay, func() {
// 			timersMu.Lock()
// 			delete(timers, path)
// 			timersMu.Unlock()
// 			enqueue(path)
// 		})
// 	}

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return nil
// 		case ev, ok := <-w.Events:
// 			if !ok {
// 				return nil
// 			}
// 			if ev.Op&(fsnotify.Create|fsnotify.Write) == 0 {
// 				continue
// 			}
// 			if !shouldProcess(ev.Name, processedAbs) {
// 				continue
// 			}
// 			schedule(ev.Name)
// 		case err, ok := <-w.Errors:
// 			if !ok {
// 				return nil
// 			}
// 			logger.Printf("watcher error: %v", err)
// 		}
// 	}
// }

// func handleOldFiles(ctx context.Context, sourceDir string, enqueue func(string), logger *log.Logger) {
// 	go func() {
// 		entries, err := os.ReadDir(sourceDir)
// 		if err != nil {
// 			logger.Printf("read source dir: %v", err)
// 			return
// 		}

// 		for _, e := range entries {
// 			if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
// 				continue
// 			}
// 			select {
// 			case <-ctx.Done():
// 				return
// 			default:
// 				enqueue(filepath.Join(sourceDir, e.Name()))
// 			}
// 		}
// 	}()
// }

// func shouldProcess(path, processedAbs string) bool {
// 	if strings.HasPrefix(filepath.Base(path), ".") {
// 		return false
// 	}
// 	info, err := os.Stat(path)
// 	if err != nil || info.IsDir() {
// 		return false
// 	}
// 	abs, err := filepath.Abs(path)
// 	if err != nil {
// 		return false
// 	}
// 	if processedAbs != "" && strings.HasPrefix(abs, processedAbs+string(filepath.Separator)) {
// 		return false
// 	}
// 	return true
// }
