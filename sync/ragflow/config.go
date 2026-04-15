package ragflow

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Route maps a Spaces path prefix to a RAGFlow dataset.
type Route struct {
	PathPrefix string
	APIBase    string
	APIKey     string
	DatasetID  string
}

// Config holds RAGFlow integration settings.
type Config struct {
	Routes     []Route
	Extensions map[string]struct{}
}

// extensions is the set of file extensions eligible for RAGFlow indexing.
var extensions = map[string]struct{}{
	".pdf":  {},
	".md":   {},
	".mdx":  {},
	".txt":  {},
	".doc":  {},
	".docx": {},
	".csv":  {},
	".xlsx": {},
	".xls":  {},
	".ppt":  {},
	".pptx": {},
}

// ParseConfigFromEnv builds a Config from environment variables.
//
// Required:
//
//	RAGFLOW_API_BASE           — shared API base URL
//	RAGFLOW_ROUTE_0_API_KEY    — API key for route 0
//	RAGFLOW_ROUTE_0_DATASET_ID — dataset ID for route 0
//
// Optional per route:
//
//	RAGFLOW_ROUTE_0_PREFIX     — path prefix (default: "" = all of Spaces)
//	RAGFLOW_ROUTE_0_API_BASE   — override shared API base for this route
//
// Routes are numbered 0, 1, 2, ... and stop at the first missing API_KEY.
func ParseConfigFromEnv() (Config, error) {
	apiBase := strings.TrimRight(os.Getenv("RAGFLOW_API_BASE"), "/")
	if apiBase == "" {
		return Config{}, fmt.Errorf("RAGFLOW_API_BASE is required")
	}

	var routes []Route
	for i := 0; ; i++ {
		prefix := fmt.Sprintf("RAGFLOW_ROUTE_%d_", i)
		apiKey := os.Getenv(prefix + "API_KEY")
		if apiKey == "" {
			break
		}
		datasetID := os.Getenv(prefix + "DATASET_ID")
		if datasetID == "" {
			return Config{}, fmt.Errorf("%sDATASET_ID is required", prefix)
		}

		routeBase := os.Getenv(prefix + "API_BASE")
		if routeBase == "" {
			routeBase = apiBase
		} else {
			routeBase = strings.TrimRight(routeBase, "/")
		}

		routes = append(routes, Route{
			PathPrefix: os.Getenv(prefix + "PREFIX"),
			APIBase:    routeBase,
			APIKey:     apiKey,
			DatasetID:  datasetID,
		})
	}

	if len(routes) == 0 {
		return Config{}, fmt.Errorf("no ragflow routes configured (need RAGFLOW_ROUTE_0_API_KEY)")
	}

	return Config{
		Routes:     routes,
		Extensions: extensions,
	}, nil
}

// MatchRoutes returns indices of all routes matching the given relative path.
func (c *Config) MatchRoutes(relPath string) []int {
	var indices []int
	for i, r := range c.Routes {
		if r.PathPrefix == "" || relPath == r.PathPrefix || strings.HasPrefix(relPath, r.PathPrefix+"/") {
			indices = append(indices, i)
		}
	}
	return indices
}

// IsEligible returns true if the file extension is in the RAGFlow extension set.
func (c *Config) IsEligible(relPath string) bool {
	ext := strings.ToLower(filepath.Ext(relPath))
	_, ok := c.Extensions[ext]
	return ok
}
