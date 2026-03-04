package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

const (
	numWorkers   = 1000
	maxVisits    = 5000
	requestDelay = 300 * time.Millisecond
)

// -----------------------------
// Crawler State
// -----------------------------

type Visited struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func NewVisited() *Visited {
	return &Visited{
		m: make(map[string]struct{}),
	}
}

func (v *Visited) Add(u string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	if _, exists := v.m[u]; exists {
		return false
	}
	v.m[u] = struct{}{}
	return true
}

func (v *Visited) Count() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.m)
}

// -----------------------------
// Worker
// -----------------------------

func worker(
	ctx context.Context,
	id int,
	jobs <-chan string,
	discovered chan<- string,
	wg *sync.WaitGroup,
	visited *Visited,
) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	for {
		select {
		case <-ctx.Done():
			return

		case u, ok := <-jobs:
			if !ok {
				return
			}

			fmt.Println("Worker", id, "crawling:", u)

			links, err := fetchAndParse(client, u)
			if err != nil {
				fmt.Println("Worker", id, "error:", err)
				wg.Done()
				continue
			}

			time.Sleep(requestDelay)

			for _, link := range links {
				if visited.Count() >= maxVisits {
					break
				}

				if visited.Add(link) {
					wg.Add(1)
					select {
					case discovered <- link:
					case <-ctx.Done():
						break
					}
				}
			}

			wg.Done()
		}
	}
}

// -----------------------------
// Dispatcher (kept per architecture)
// -----------------------------

func dispatcher(
	ctx context.Context,
	discovered <-chan string,
	jobs chan<- string,
) {
	for {
		select {
		case <-ctx.Done():
			return

		case u, ok := <-discovered:
			if !ok {
				return
			}
			// Block naturally instead of busy waiting
			jobs <- u
		}
	}
}

// -----------------------------
// Fetch + Parse
// -----------------------------

func fetchAndParse(client *http.Client, rawURL string) ([]string, error) {
	resp, err := client.Get(rawURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return parseLinks(body, rawURL), nil
}

func parseLinks(body []byte, base string) []string {
	doc, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil
	}

	baseURL, err := url.Parse(base)
	if err != nil {
		return nil
	}

	var links []string

	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					href := strings.TrimSpace(attr.Val)
					parsed, err := url.Parse(href)
					if err != nil {
						continue
					}
					abs := baseURL.ResolveReference(parsed)
					if abs.Scheme == "http" || abs.Scheme == "https" {
						links = append(links, abs.String())
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}

	f(doc)
	return links
}

// -----------------------------
// Main
// -----------------------------

func main() {
	start := "https://crawler-test.com/"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan string, numWorkers)
	discovered := make(chan string, 1000)

	var wg sync.WaitGroup
	visited := NewVisited()

	// Start workers
	for i := range(numWorkers) {
		go worker(ctx, i, jobs, discovered, &wg, visited)
	}

	// Start dispatcher
	go dispatcher(ctx, discovered, jobs)

	// Seed
	visited.Add(start)
	wg.Add(1)
	discovered <- start

	// Shutdown controller
	go func() {
		wg.Wait()
		cancel()
		close(discovered)
		close(jobs)
	}()

	wg.Wait()

	fmt.Println("Crawling complete")
	fmt.Println("Total visited:", visited.Count())
}