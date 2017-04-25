package leveldb

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"testing"
	"time"
)

// this force L0 compaction with merge
func TestTTL_l0compaction(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB})
	defer h.close()
	log := h.db.s.stor.Log

	assert.Equal(t, 0, len(h.db.s.stVersion.levels))
	for i := 0; i < 4; i++ {
		h.put(string(tkey(i)), string(tval(0, 32)))
		h.put(string(tkey(i+100)), string(tval(0, 32)))
		h.compactMem()
	}
	assert.Equal(t, 1, len(h.db.s.stVersion.levels))
	assert.Equal(t, 4, len(h.db.s.stVersion.levels[0]))
	log("gdv: before table compaction")
	h.waitCompaction()
	log("gdv: after table compaction")
	levels := h.db.s.stVersion.levels
	assert.Equal(t, 2, len(levels))
	assert.Equal(t, 0, len(levels[0]))
	assert.Equal(t, 1, len(levels[1]))

}

// as key ranges not overlap between l0 tables, this lead to trivial compaction (move)
func TestTTL_l0compaction_move(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB})
	defer h.close()
	log := h.db.s.stor.Log
	for i := 0; i < 4; i++ {
		h.put(string(tkey(i)), string(tval(0, 32)))
		h.compactMem()
	}
	assert.Equal(t, 1, len(h.db.s.stVersion.levels))
	assert.Equal(t, 4, len(h.db.s.stVersion.levels[0]))
	log("gdv: before table compaction")
	h.waitCompaction()
	log("gdv: after table compaction")
	levels := h.db.s.stVersion.levels
	log(fmt.Sprintf("%v", levels))
	assert.Equal(t, 2, len(levels))
	assert.Equal(t, 3, len(levels[0]))
	assert.Equal(t, 1, len(levels[1]))
}

func (h *dbHarness) timedKey(i, t int) string {
	var key [12]byte
	binary.BigEndian.PutUint32(key[:4], uint32(i))
	binary.BigEndian.PutUint64(key[4:], uint64(t))
	return string(key[:])
}

func TestTTL_merge_compaction(t *testing.T) {
	nowFunc := func() time.Time { return time.Unix(20, 0) }
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB, TTL: 10, NowFunc: nowFunc})
	defer h.close()
	log := h.db.s.logf

	putPair := func(from, to, ts int) {
		log("saving pair %d:%d", from, to)
		h.put(h.timedKey(from, ts), string(tval(0, 32)))
		h.put(h.timedKey(to, ts), string(tval(0, 32)))
		h.compactMem()
		log("mem compaction done")
	}

	putPair(1, 100, 34)
	assert.Equal(t, int64(34), h.db.s.stVersion.levels[0][0].latest)

	putPair(2, 101, 30)
	putPair(3, 102, 36)
	putPair(4, 103, 8)
	h.waitCompaction()
	log("table compaction done")

	assert.Equal(t, 2, len(h.db.s.version().levels))
	assert.Equal(t, int64(36), h.db.s.version().levels[1][0].latest)

}

func TestTTL_trivial_compaction(t *testing.T) {
	nowFunc := func() time.Time { return time.Unix(20, 0) }
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB, TTL: 10, NowFunc: nowFunc})
	defer h.close()
	log := h.db.s.logf

	expected := make(map[int64]struct{})
	for i := 0; i < 4; i++ {
		h.put(h.timedKey(i, i+100), string(tval(0, 32)))
		expected[int64(i+100)] = struct{}{}
		h.compactMem()
	}
	h.waitCompaction()
	log("table compaction complete")
	levels := h.db.s.version().levels
	assert.Equal(t, 3, len(levels[0]))
	assert.Equal(t, 1, len(levels[1]))
	actual := make(map[int64]struct{})
	for i := range levels {
		for l := range levels[i] {
			actual[levels[i][l].latest] = struct{}{}
		}
	}
	assert.Equal(t, expected, actual)
}
