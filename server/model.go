package server

import comatproto "github.com/bluesky-social/indigo/api/atproto"

type Entry struct {
	Seq int64  `gorm:"type:INTEGER PRIMARY KEY;primaryKey"`
	Cts string `gorm:"not null"`

	Uri string `gorm:"not null;index:idx_lookups,priority:1"`
	Val string `gorm:"not null;index:idx_lookups,priority:2"`
	Src string `gorm:"not null;index:idx_lookups,priority:3"`
	Cid string `gorm:"index:idx_lookups,priority:4"`

	Exp string
	Neg bool `gorm:"default:false"`
}

func (Entry) TableName() string {
	return "log"
}

func (e *Entry) FromLabel(seq int64, other comatproto.LabelDefs_Label) *Entry {
	e.Seq = seq
	e.Cts = other.Cts
	e.Uri = other.Uri
	e.Val = other.Val
	e.Src = other.Src
	e.Cid = ""
	e.Exp = ""
	e.Neg = false

	if other.Cid != nil {
		e.Cid = *other.Cid
	}
	if other.Exp != nil {
		e.Exp = *other.Exp
	}
	if other.Neg != nil {
		e.Neg = *other.Neg
	}

	return e
}

func (e *Entry) ToLabel() comatproto.LabelDefs_Label {
	r := comatproto.LabelDefs_Label{
		Cts: e.Cts,
		Val: e.Val,
		Uri: e.Uri,
		Src: e.Src,
		Ver: ptr(int64(1)),
	}
	if e.Cid != "" {
		r.Cid = ptr(e.Cid)
	}
	if e.Neg {
		r.Neg = ptr(e.Neg)
	}
	if e.Exp != "" {
		r.Exp = ptr(e.Exp)
	}
	return r
}

func entriesToLabels(entries []Entry) []comatproto.LabelDefs_Label {
	r := make([]comatproto.LabelDefs_Label, len(entries))
	for i, e := range entries {
		r[i] = e.ToLabel()
	}
	return r
}
