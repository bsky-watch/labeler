package server

// locked_applyLabelCreation implements the logic of adding a label.
// Return value indicates if any change was actually made.
func (s *Server) locked_applyLabelCreation(entry Entry, dryRun bool) bool {
	src := entry.Src
	uri := entry.Uri
	val := entry.Val
	cid := ""
	if entry.Cid != nil {
		cid = *entry.Cid
	}
	if s.labels[src] == nil {
		s.labels[src] = map[string]map[string]map[string]Entry{}
	}
	if s.labels[src][uri] == nil {
		s.labels[src][uri] = map[string]map[string]Entry{}
	}
	if s.labels[src][uri][val] == nil {
		s.labels[src][uri][val] = map[string]Entry{}
	}
	if prev, found := s.labels[src][uri][val][cid]; found {
		if prev.Exp == nil && entry.Exp == nil {
			return false
		}
		if prev.Exp != nil && entry.Exp != nil && *prev.Exp == *entry.Exp {
			return false
		}
	}
	if !dryRun {
		s.labels[src][uri][val][cid] = entry
	}
	return true
}

// locked_applyLabelRemoval implements the logic of removing a label.
// Return value indicates if any change was actually made.
func (s *Server) locked_applyLabelRemoval(entry Entry, dryRun bool) bool {
	src := entry.Src
	uri := entry.Uri
	val := entry.Val
	cid := ""
	if entry.Cid != nil {
		cid = *entry.Cid
	}
	if len(s.labels[src]) == 0 {
		return false
	}
	if len(s.labels[src][uri]) == 0 {
		return false
	}
	if len(s.labels[src][uri][val]) == 0 {
		return false
	}
	// Doing the dumb thing only for now. That is, not touching labels
	// for specific CIDs upon negation w/o a CID.
	if _, found := s.labels[src][uri][val][cid]; found {
		if !dryRun {
			delete(s.labels[src][uri][val], cid)
		}
		return true
	}
	return false
}
