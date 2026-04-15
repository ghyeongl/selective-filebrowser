package ragflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Client wraps the RAGFlow REST API for a single dataset.
type Client struct {
	httpClient *http.Client
	apiBase    string
	apiKey     string
	datasetID  string
}

// NewClient creates a client for the given route.
func NewClient(apiBase, apiKey, datasetID string) *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 60 * time.Second},
		apiBase:    apiBase,
		apiKey:     apiKey,
		datasetID:  datasetID,
	}
}

// apiResp is the common envelope for RAGFlow API responses.
type apiResp struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

// doc represents a document in RAGFlow.
type doc struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"run"` // "UNSTART", "RUNNING", "CANCEL", "DONE", "FAIL"
}

// docListResp is the response for document list/search endpoints.
// Upload returns data as []doc directly, but list/search returns {"docs":[],"total":N}.
type docListResp struct {
	Docs  []doc `json:"docs"`
	Total int   `json:"total"`
}

// Upload uploads a file to the RAGFlow dataset. Returns the document ID.
func (c *Client) Upload(ctx context.Context, filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)

	go func() {
		part, err := writer.CreateFormFile("file", filepath.Base(filePath))
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(part, f); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.CloseWithError(writer.Close())
	}()

	u := fmt.Sprintf("%s/api/v1/datasets/%s/documents", c.apiBase, c.datasetID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, pr)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("upload request: %w", err)
	}
	defer resp.Body.Close()

	var r apiResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if r.Code != 0 {
		return "", &APIError{Code: r.Code, Message: r.Message, HTTPStatus: resp.StatusCode}
	}

	var docs []doc
	if err := json.Unmarshal(r.Data, &docs); err != nil {
		return "", fmt.Errorf("decode documents: %w", err)
	}
	if len(docs) == 0 {
		return "", fmt.Errorf("upload returned no documents")
	}
	return docs[0].ID, nil
}

// FindByName searches for a document by filename. Returns docID or "" if not found.
func (c *Client) FindByName(ctx context.Context, fileName string) (string, error) {
	u := fmt.Sprintf("%s/api/v1/datasets/%s/documents?name=%s",
		c.apiBase, c.datasetID, url.QueryEscape(fileName))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("find request: %w", err)
	}
	defer resp.Body.Close()

	var r apiResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if r.Code != 0 {
		return "", &APIError{Code: r.Code, Message: r.Message, HTTPStatus: resp.StatusCode}
	}

	var list docListResp
	if err := json.Unmarshal(r.Data, &list); err != nil {
		return "", fmt.Errorf("decode documents: %w", err)
	}
	if len(list.Docs) == 0 {
		return "", nil
	}
	return list.Docs[0].ID, nil
}

// Parse triggers document parsing and waits for completion (no timeout).
// Blocks until DONE/FAIL/CANCEL or ctx is cancelled, ensuring one parse at a time.
func (c *Client) Parse(ctx context.Context, docID string) error {
	u := fmt.Sprintf("%s/api/v1/datasets/%s/chunks", c.apiBase, c.datasetID)
	payload, _ := json.Marshal(map[string][]string{"document_ids": {docID}})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(string(payload)))
	if err != nil {
		return fmt.Errorf("create parse request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("parse request: %w", err)
	}
	defer resp.Body.Close()

	var r apiResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("decode parse response: %w", err)
	}
	if r.Code != 0 {
		return &APIError{Code: r.Code, Message: r.Message, HTTPStatus: resp.StatusCode}
	}

	return c.waitForParse(ctx, docID)
}

func (c *Client) waitForParse(ctx context.Context, docID string) error {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			status, err := c.getDocStatus(ctx, docID)
			if err != nil {
				return fmt.Errorf("poll parse status: %w", err)
			}
			switch status {
			case "DONE":
				return nil
			case "FAIL":
				return fmt.Errorf("parse failed for doc %s", docID)
			case "CANCEL":
				return fmt.Errorf("parse cancelled for doc %s", docID)
			}
		}
	}
}

func (c *Client) getDocStatus(ctx context.Context, docID string) (string, error) {
	u := fmt.Sprintf("%s/api/v1/datasets/%s/documents?id=%s",
		c.apiBase, c.datasetID, url.QueryEscape(docID))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var r apiResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", err
	}

	var list docListResp
	if err := json.Unmarshal(r.Data, &list); err != nil {
		return "", err
	}
	if len(list.Docs) == 0 {
		return "", fmt.Errorf("document %s not found", docID)
	}
	return list.Docs[0].Status, nil
}

// Delete removes a document from RAGFlow.
func (c *Client) Delete(ctx context.Context, docID string) error {
	u := fmt.Sprintf("%s/api/v1/datasets/%s/documents", c.apiBase, c.datasetID)
	payload, _ := json.Marshal(map[string][]string{"ids": {docID}})

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, strings.NewReader(string(payload)))
	if err != nil {
		return fmt.Errorf("create delete request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete request: %w", err)
	}
	defer resp.Body.Close()

	var r apiResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if r.Code != 0 {
		return &APIError{Code: r.Code, Message: r.Message, HTTPStatus: resp.StatusCode}
	}
	return nil
}

// APIError represents a non-zero code response from RAGFlow.
type APIError struct {
	Code       int
	Message    string
	HTTPStatus int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("ragflow API error %d (HTTP %d): %s", e.Code, e.HTTPStatus, e.Message)
}

// IsRetryable returns true for 429 and 5xx errors.
func (e *APIError) IsRetryable() bool {
	return e.HTTPStatus == 429 || e.HTTPStatus >= 500
}
