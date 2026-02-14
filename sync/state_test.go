package sync

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScenario_All34(t *testing.T) {
	tests := []struct {
		name     string
		state    State
		scenario int
		status   string
	}{
		// Group: A_db=0 (scenarios 1-4)
		{"#1 nonexistent", State{}, 1, ""},
		{"#2 untracked A_disk only", State{ADisk: true}, 2, "untracked"},
		{"#3 untracked S_disk only", State{SDisk: true}, 3, "untracked"},
		{"#4 untracked both disks", State{ADisk: true, SDisk: true}, 4, "untracked"},

		// Group: A_db=1, A_disk=0, S_disk=0 (scenarios 5-8 — lost)
		{"#5 lost sel=0 S_db=0", State{ADb: true}, 5, "lost"},
		{"#6 lost sel=1 S_db=0", State{ADb: true, Selected: true}, 6, "lost"},
		{"#7 lost sel=0 S_db=1", State{ADb: true, SDb: true}, 7, "lost"},
		{"#8 lost sel=1 S_db=1", State{ADb: true, SDb: true, Selected: true}, 8, "lost"},

		// Group: A_db=1, A_disk=0, S_disk=1 (scenarios 9-14 — recovering)
		{"#9 recovering S_db=0 sel=0", State{ADb: true, SDisk: true}, 9, "recovering"},
		{"#10 recovering S_db=0 sel=1", State{ADb: true, SDisk: true, Selected: true}, 10, "recovering"},
		{"#11 recovering S_db=1 sel=0 clean", State{ADb: true, SDisk: true, SDb: true}, 11, "recovering"},
		{"#12 recovering S_db=1 sel=0 S_dirty", State{ADb: true, SDisk: true, SDb: true, SDirty: true}, 12, "recovering"},
		{"#13 recovering S_db=1 sel=1 clean", State{ADb: true, SDisk: true, SDb: true, Selected: true}, 13, "recovering"},
		{"#14 recovering S_db=1 sel=1 S_dirty", State{ADb: true, SDisk: true, SDb: true, Selected: true, SDirty: true}, 14, "recovering"},

		// Group: A_disk=1, A_db=1, S_disk=0, S_db=0 (scenarios 15-18)
		{"#15 archived", State{ADisk: true, ADb: true}, 15, "archived"},
		{"#16 archived A_dirty", State{ADisk: true, ADb: true, ADirty: true}, 16, "archived"},
		{"#17 syncing sel=1", State{ADisk: true, ADb: true, Selected: true}, 17, "syncing"},
		{"#18 syncing sel=1 A_dirty", State{ADisk: true, ADb: true, Selected: true, ADirty: true}, 18, "syncing"},

		// Group: A_disk=1, A_db=1, S_disk=0, S_db=1 (scenarios 19-22 — repairing)
		{"#19 repairing sel=0 clean", State{ADisk: true, ADb: true, SDb: true}, 19, "repairing"},
		{"#20 repairing sel=0 A_dirty", State{ADisk: true, ADb: true, SDb: true, ADirty: true}, 20, "repairing"},
		{"#21 repairing sel=1 clean", State{ADisk: true, ADb: true, SDb: true, Selected: true}, 21, "repairing"},
		{"#22 repairing sel=1 A_dirty", State{ADisk: true, ADb: true, SDb: true, Selected: true, ADirty: true}, 22, "repairing"},

		// Group: A_disk=1, A_db=1, S_disk=1, S_db=0 (scenarios 23-26 — repairing)
		{"#23 repairing sel=0 clean", State{ADisk: true, ADb: true, SDisk: true}, 23, "repairing"},
		{"#24 conflict sel=0 A_dirty", State{ADisk: true, ADb: true, SDisk: true, ADirty: true}, 24, "conflict"},
		{"#25 repairing sel=1 clean", State{ADisk: true, ADb: true, SDisk: true, Selected: true}, 25, "repairing"},
		{"#26 repairing sel=1 A_dirty", State{ADisk: true, ADb: true, SDisk: true, Selected: true, ADirty: true}, 26, "repairing"},

		// Group: A_disk=1, A_db=1, S_disk=1, S_db=1 (scenarios 27-34)
		{"#27 removing clean", State{ADisk: true, ADb: true, SDisk: true, SDb: true}, 27, "removing"},
		{"#28 removing S_dirty", State{ADisk: true, ADb: true, SDisk: true, SDb: true, SDirty: true}, 28, "removing"},
		{"#29 removing A_dirty", State{ADisk: true, ADb: true, SDisk: true, SDb: true, ADirty: true}, 29, "removing"},
		{"#30 removing sel=0 both dirty", State{ADisk: true, ADb: true, SDisk: true, SDb: true, ADirty: true, SDirty: true}, 30, "removing"},
		{"#31 synced", State{ADisk: true, ADb: true, SDisk: true, SDb: true, Selected: true}, 31, "synced"},
		{"#32 updating S_dirty", State{ADisk: true, ADb: true, SDisk: true, SDb: true, Selected: true, SDirty: true}, 32, "updating"},
		{"#33 updating A_dirty", State{ADisk: true, ADb: true, SDisk: true, SDb: true, Selected: true, ADirty: true}, 33, "updating"},
		{"#34 conflict sel=1 both dirty", State{ADisk: true, ADb: true, SDisk: true, SDb: true, Selected: true, ADirty: true, SDirty: true}, 34, "conflict"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.scenario, tt.state.Scenario(), "scenario mismatch")
			assert.Equal(t, tt.status, tt.state.UIStatus(), "status mismatch")
		})
	}
}

func TestComputeState_NilInputs(t *testing.T) {
	// Everything nil → scenario 1
	st := ComputeState(nil, nil, nil, nil)
	assert.Equal(t, 1, st.Scenario())
}

func TestComputeState_ArchivedFile(t *testing.T) {
	entry := &Entry{Inode: 100, Mtime: 5000, Selected: false}
	archiveMtime := int64(5000)
	st := ComputeState(entry, nil, &archiveMtime, nil)
	assert.Equal(t, 15, st.Scenario())
	assert.Equal(t, "archived", st.UIStatus())
	assert.False(t, st.ADirty)
}

func TestComputeState_ADirty(t *testing.T) {
	entry := &Entry{Inode: 100, Mtime: 5000, Selected: false}
	archiveMtime := int64(6000) // different from entry.Mtime
	st := ComputeState(entry, nil, &archiveMtime, nil)
	assert.Equal(t, 16, st.Scenario())
	assert.True(t, st.ADirty)
}

func TestComputeState_Synced(t *testing.T) {
	entry := &Entry{Inode: 100, Mtime: 5000, Selected: true}
	sv := &SpacesView{EntryIno: 100, SyncedMtime: 5000}
	archiveMtime := int64(5000)
	spacesMtime := int64(5000)
	st := ComputeState(entry, sv, &archiveMtime, &spacesMtime)
	assert.Equal(t, 31, st.Scenario())
	assert.Equal(t, "synced", st.UIStatus())
}

func TestComputeState_SDirty(t *testing.T) {
	entry := &Entry{Inode: 100, Mtime: 5000, Selected: true}
	sv := &SpacesView{EntryIno: 100, SyncedMtime: 5000}
	archiveMtime := int64(5000)
	spacesMtime := int64(7000) // modified on Spaces
	st := ComputeState(entry, sv, &archiveMtime, &spacesMtime)
	assert.Equal(t, 32, st.Scenario())
	assert.Equal(t, "updating", st.UIStatus())
	assert.True(t, st.SDirty)
}

func TestComputeState_BothDirtySelected(t *testing.T) {
	entry := &Entry{Inode: 100, Mtime: 5000, Selected: true}
	sv := &SpacesView{EntryIno: 100, SyncedMtime: 5000}
	archiveMtime := int64(6000)
	spacesMtime := int64(7000)
	st := ComputeState(entry, sv, &archiveMtime, &spacesMtime)
	assert.Equal(t, 34, st.Scenario())
	assert.Equal(t, "conflict", st.UIStatus())
}
