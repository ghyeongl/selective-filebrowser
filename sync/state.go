package sync

// State holds the 7 variables used to determine an entry's scenario.
type State struct {
	ADisk    bool // file exists on Archives disk
	ADb      bool // entry exists in entries table
	SDisk    bool // file exists on Spaces disk
	SDb      bool // spaces_view record exists
	Selected bool
	ADirty   bool // Archives mtime differs from entries.mtime
	SDirty   bool // Spaces mtime differs from spaces_view.synced_mtime
}

// Scenario returns the scenario number (1-34) from the truth table.
func (s State) Scenario() int {
	// Group by (A_disk, A_db, S_disk, S_db, selected, A_dirty, S_dirty)
	if !s.ADb {
		// A_db=0: scenarios 1-4
		if !s.ADisk && !s.SDisk {
			return 1
		}
		if s.ADisk && !s.SDisk {
			return 2
		}
		if !s.ADisk && s.SDisk {
			return 3
		}
		return 4 // A_disk=1, S_disk=1
	}

	if !s.ADisk {
		// A_db=1, A_disk=0: scenarios 5-14
		if !s.SDisk {
			if !s.SDb {
				if !s.Selected {
					return 5
				}
				return 6
			}
			// S_db=1 but S_disk=0 (stale spaces_view)
			if !s.Selected {
				return 7
			}
			return 8
		}
		// S_disk=1
		if !s.SDb {
			if !s.Selected {
				return 9
			}
			return 10
		}
		// S_db=1
		if !s.Selected && !s.SDirty {
			return 11
		}
		if !s.Selected && s.SDirty {
			return 12
		}
		if s.Selected && !s.SDirty {
			return 13
		}
		return 14 // selected=1, S_dirty=1
	}

	// A_disk=1, A_db=1
	if !s.SDisk && !s.SDb {
		// Group C: S_disk=0, S_db=0
		if !s.Selected && !s.ADirty {
			return 15
		}
		if !s.Selected && s.ADirty {
			return 16
		}
		if s.Selected && !s.ADirty {
			return 17
		}
		return 18 // selected=1, A_dirty=1
	}

	if !s.SDisk && s.SDb {
		// Group D: S_disk=0, S_db=1
		if !s.Selected && !s.ADirty {
			return 19
		}
		if !s.Selected && s.ADirty {
			return 20
		}
		if s.Selected && !s.ADirty {
			return 21
		}
		return 22 // selected=1, A_dirty=1
	}

	if s.SDisk && !s.SDb {
		// Group E: S_disk=1, S_db=0
		if !s.Selected && !s.ADirty {
			return 23
		}
		if !s.Selected && s.ADirty {
			return 24
		}
		if s.Selected && !s.ADirty {
			return 25
		}
		return 26 // selected=1, A_dirty=1
	}

	// Group F: S_disk=1, S_db=1
	if !s.Selected {
		if !s.ADirty && !s.SDirty {
			return 27
		}
		if !s.ADirty && s.SDirty {
			return 28
		}
		if s.ADirty && !s.SDirty {
			return 29
		}
		return 30 // A_dirty=1, S_dirty=1
	}
	// selected=1
	if !s.ADirty && !s.SDirty {
		return 31
	}
	if !s.ADirty && s.SDirty {
		return 32
	}
	if s.ADirty && !s.SDirty {
		return 33
	}
	return 34 // A_dirty=1, S_dirty=1
}

// UIStatus returns the human-readable status label for this state.
func (s State) UIStatus() string {
	sc := s.Scenario()
	switch {
	case sc == 1:
		return "" // nonexistent
	case sc == 15:
		return "archived"
	case sc == 31:
		return "synced"
	case sc == 17 || sc == 18:
		return "syncing"
	case sc == 27 || sc == 28 || sc == 29:
		return "removing"
	case sc == 32 || sc == 33:
		return "updating"
	case sc == 30 || sc == 34:
		return "conflict"
	case sc >= 9 && sc <= 14:
		return "recovering"
	case sc >= 5 && sc <= 8:
		return "lost"
	case sc >= 2 && sc <= 4:
		return "untracked"
	case sc >= 19 && sc <= 26:
		return "repairing"
	case sc == 16:
		return "archived" // A_dirty, will be updated
	default:
		return "unknown"
	}
}

// ComputeState builds a State from the entry, spaces_view, and disk stat results.
// archiveStat and spacesStat are nil when the file doesn't exist on disk.
func ComputeState(entry *Entry, sv *SpacesView, archiveMtime *int64, spacesMtime *int64) State {
	st := State{}

	st.ADisk = archiveMtime != nil
	st.ADb = entry != nil
	st.SDisk = spacesMtime != nil
	st.SDb = sv != nil

	if entry != nil {
		st.Selected = entry.Selected
		if archiveMtime != nil {
			st.ADirty = *archiveMtime != entry.Mtime
		}
	}

	if sv != nil && spacesMtime != nil {
		st.SDirty = *spacesMtime != sv.SyncedMtime
	}

	return st
}
