// vectra-cli is a Cypher REPL over the vectra-server HTTP API. This file
// holds the pure HTTP client half: request encoding, response decoding, and
// the server's error envelope.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// cypherSummary is the write-summary object of POST /v1/cypher (present only
// on WRITE responses). The JSON keys are pinned here because the engine-side
// type carries no tags.
type cypherSummary struct {
	CreatedNodes   int `json:"createdNodes"`
	CreatedEdges   int `json:"createdEdges"`
	DeletedNodes   int `json:"deletedNodes"`
	DeletedEdges   int `json:"deletedEdges"`
	PropertiesSet  int `json:"propertiesSet"`
	IndexesCreated int `json:"indexesCreated"`
}

// cypherResponse is the decoded /v1/cypher body. READ responses carry
// columns/rows/count; WRITE responses carry columns/rows/summary and no
// count key (which decodes as zero).
type cypherResponse struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Count   int              `json:"count"`
	Summary *cypherSummary   `json:"summary"`
}

// apiError is the server's error envelope: {"error":{"code":...,"message":...}}.
type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *apiError) Error() string { return e.Code + ": " + e.Message }

// cypherClient talks to one vectra-server node's HTTP API.
type cypherClient struct {
	base   *url.URL
	bearer string // "" = send no Authorization header
	http   *http.Client
}

// newCypherClient parses baseURL, which must carry a scheme (a bare
// "host:port" is rejected — url.Parse would read the host as a bogus
// scheme). The bearer token is attached to every request unless empty.
func newCypherClient(baseURL, bearer string, timeout time.Duration) (*cypherClient, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base URL %q: %w", baseURL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("base URL %q needs a scheme, e.g. http://%s", baseURL, baseURL)
	}
	return &cypherClient{
		base:   u,
		bearer: bearer,
		http:   &http.Client{Timeout: timeout},
	}, nil
}

// cypher POSTs {"query": query} to /v1/cypher and decodes the response.
// Any non-2xx status decodes the error envelope and returns it as an
// *apiError; ctx bounds the request.
func (c *cypherClient) cypher(ctx context.Context, query string) (*cypherResponse, error) {
	body, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base.JoinPath("v1", "cypher").String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearer)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var env struct {
			Error *apiError `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
			return nil, fmt.Errorf("status %d with undecodable error body: %w", resp.StatusCode, err)
		}
		if env.Error == nil {
			return nil, fmt.Errorf("status %d with no error envelope", resp.StatusCode)
		}
		return nil, env.Error
	}
	var out cypherResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &out, nil
}
