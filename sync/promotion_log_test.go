package sync

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureInfoLog swaps the package logger for one writing to a buffer at INFO,
// which is the level the console handler sends to stdout (i.e. `docker logs`).
func captureInfoLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := logger
	logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	t.Cleanup(func() { logger = prev })
	return &buf
}

// A file that appeared in Spaces and Archives never had is the guardrail-G1
// event: it must be visible at the default log level.
func TestPromotionIsLoggedAtInfo(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeSpaces(t, "arrived.txt", []byte("from spaces"))

	buf := captureInfoLog(t)
	env.run(t, "arrived.txt")

	out := buf.String()
	assert.Contains(t, out, "promoted from Spaces", "the grep token must be present")
	assert.Contains(t, out, "arrived.txt", "the path must be present")
	require.True(t, env.fileExists(env.archivesRoot+"/arrived.txt"), "precondition: it really was promoted")
}

// Noise check: normal A→S propagation and deselect must not produce the token,
// or it is useless as an observation.
func TestNormalSyncDoesNotLogPromotion(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "report.txt", []byte("v1"))
	env.run(t, "report.txt")

	buf := captureInfoLog(t)

	// Select → A→S propagation.
	entries, err := env.store.ListChildren(0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NoError(t, env.store.SetSelected([]uint64{entries[0].Inode}, true))
	env.run(t, "report.txt")
	require.True(t, env.fileExists(env.spacesRoot+"/report.txt"), "select must really propagate A→S")

	// Deselect → Spaces copy removed.
	require.NoError(t, env.store.SetSelected([]uint64{entries[0].Inode}, false))
	env.run(t, "report.txt")
	require.False(t, env.fileExists(env.spacesRoot+"/report.txt"), "deselect must really remove the Spaces copy")

	assert.NotContains(t, buf.String(), "promoted from Spaces",
		"normal propagation and deselect must stay quiet")
	assert.False(t, strings.Contains(buf.String(), "promoted"), "no partial token either")
}

// Restoring a catalogued file that vanished from Archives is not accumulation.
// It gets its own token so the KR grep does not count it.
func TestArchivesRecoveryUsesADifferentToken(t *testing.T) {
	env := setupPipelineEnv(t)
	env.writeArchive(t, "kept.txt", []byte("v1"))
	env.writeSpaces(t, "kept.txt", []byte("v1"))
	env.run(t, "kept.txt")
	require.NotNil(t, mustEntry(t, env, "kept.txt"), "precondition: catalogued in the DB")

	// Archives loses the file; Spaces still has it.
	require.NoError(t, os.Remove(env.archivesRoot+"/kept.txt"))

	buf := captureInfoLog(t)
	env.run(t, "kept.txt")

	require.True(t, env.fileExists(env.archivesRoot+"/kept.txt"), "precondition: it really was restored")
	out := buf.String()
	assert.Contains(t, out, "restored to Archives from Spaces", "restores stay visible")
	assert.NotContains(t, out, "promoted from Spaces", "a restore must not count toward the guardrail")
}

func mustEntry(t *testing.T, env *pipelineEnv, name string) *Entry {
	t.Helper()
	e, err := env.store.GetEntryByPath(0, name)
	require.NoError(t, err)
	return e
}
