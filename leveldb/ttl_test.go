package leveldb

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"sort"
	"testing"
	"time"
)

func (h *dbHarness) timedKey(i, t int) string {
	var key [12]byte
	binary.BigEndian.PutUint32(key[:4], uint32(i))
	binary.BigEndian.PutUint64(key[4:], uint64(t))
	return string(key[:])
}

func (h *dbHarness) withKeys(dest *[]string, ts int, keys ...int) *dbHarness {
	for _, ik := range keys {
		key := h.timedKey(ik, ts)
		*dest = append(*dest, key)
		h.put(key, string(tval(0, 32)))
	}
	return h
}

func (h *dbHarness) flush() {
	h.compactMem()
}

func (h *dbHarness) assertKeys(keys []string, expect bool) {
	for _, k := range keys {
		ok, err := h.db.Has([]byte(k), h.ro)
		if err != nil {
			h.t.Error(err)
		}
		if ok != expect {
			h.t.Errorf("Error: key present: %v", ok)
		}
	}
}

func (h *dbHarness) assertLevels(levels ...int) {
	actual := h.db.s.version().levels
	if len(actual) != len(levels) {
		h.t.Errorf("Bad number of levels, expected %d, actual %d", len(levels), len(actual))
	}
	for l, llen := range levels {
		if len(actual[l]) != llen {
			h.t.Errorf("Bad number of tables on level %d, expected %d, actual %d", l, llen, len(actual[l]))
		}
	}
}

func (h *dbHarness) assertLatest(level int, expected ...int) {
	var actual []int
	for _, lvl := range h.db.s.version().levels[level] {
		actual = append(actual, int(lvl.latest))
	}
	if len(actual) != len(expected) {
		h.t.Errorf("Wrong number of records, expected %d, actual %d", len(expected), len(actual))
	}
	sort.Sort(sort.IntSlice(actual))
	sort.Sort(sort.IntSlice(expected))
	for i := range expected {
		if actual[i] != expected[i] {
			h.t.Errorf("Bad latest, expected %d actual %d", expected[i], actual[i])
		}
	}
}

// this force L0 compaction with merge
func TestTTL_l0compaction(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB})
	defer h.close()

	var keys []string
	h.assertLevels()
	assert.Equal(t, 0, len(h.db.s.stVersion.levels))
	for i := 0; i < 4; i++ {
		h.withKeys(&keys, 0, i, i+100).flush()
	}
	h.assertLevels(4)
	h.waitCompaction()
	h.assertLevels(0, 1)

}

// as key ranges not overlap between l0 tables, this lead to trivial compaction (move)
func TestTTL_l0compaction_move(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB})
	defer h.close()
	var keys []string
	for i := 0; i < 4; i++ {
		h.withKeys(&keys, 0, i).flush()
	}
	h.assertLevels(4)
	h.waitCompaction()
	h.assertLevels(3, 1)
}

func TestTTL_merge_compaction(t *testing.T) {
	nowFunc := func() time.Time { return time.Unix(20, 0) }
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB, TTL: 10, NowFunc: nowFunc})
	defer h.close()

	var keys, drop []string
	h.withKeys(&keys, 34, 1, 100).flush()
	h.assertLatest(0, 34)

	h.withKeys(&keys, 30, 2, 101).flush()
	h.withKeys(&keys, 36, 3, 102).flush()
	h.withKeys(&drop, 8, 4, 103).flush()

	h.waitCompaction()

	h.assertLevels(0, 1)
	h.assertKeys(keys, true)
	h.assertKeys(drop, false)

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

func TestTTL_records_elimination(t *testing.T) {
	nowFunc := func() time.Time { return time.Unix(20, 0) }
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB, TTL: 10, NowFunc: nowFunc})
	defer h.close()

	// this keys must persists after table compaction
	var expectPersist, expectDrop []string
	for i := 0; i < 4; i++ {
		h.withKeys(&expectPersist, 100, i).withKeys(&expectDrop, 5, i+100).flush()
	}
	h.waitCompaction()
	h.db.log("table compaction complete")
	h.assertKeys(expectPersist, true)
	h.assertKeys(expectDrop, false)
}

func TestTTL_table_elimination(t *testing.T) {
	var currentTime time.Time = time.Unix(1000, 0)
	nowFunc := func() time.Time { return currentTime }
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB, TTL: 200, NowFunc: nowFunc})
	defer h.close()

	var expectPersist, expectDrop []string
	// create 2 tables on level0 for elimination
	for i := 0; i < 2; i++ {
		h.withKeys(&expectDrop, 1010, i).flush()
	}
	h.assertKeys(expectDrop, true)

	// and 4 tables to persists
	for i := 2; i < 6; i++ {
		h.withKeys(&expectPersist, 1510, i).flush()
	}
	h.db.log("before table compaction")
	h.assertKeys(expectPersist, true)

	currentTime = time.Unix(1500, 0)
	// create additional table to initiate compaction
	h.withKeys(&expectPersist, 1600, 7).flush()
	h.db.log("table compaction starting")
	h.waitCompaction()
	h.db.log("table compaction finished")
	h.assertKeys(expectDrop, false)
	h.assertKeys(expectPersist, true)
}

func TestTTL_old_age(t *testing.T) {
	var currentTime time.Time = time.Unix(1000, 0)
	nowFunc := func() time.Time { return currentTime }
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB, TTL: 200, NowFunc: nowFunc})
	defer h.close()

	var expectPersist, expectDrop []string

	// create two tables with old records
	h.withKeys(&expectDrop, 1010, 1, 2, 3).flush()
	h.withKeys(&expectDrop, 1010, 4, 5, 6).flush()

	// create tables with recent records
	for i := 7; i < 10; i++ {
		h.withKeys(&expectPersist, 1510, i).flush()
	}
	currentTime = time.Unix(1600, 0)
	// trigger new version
	h.withKeys(&expectPersist, 1610, 10).flush()

	h.assertKeys(expectDrop, false)
}

func TestTTL_save_latest_on_close(t *testing.T) {
	var currentTime time.Time = time.Unix(1000, 0)
	nowFunc := func() time.Time { return currentTime }
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB, TTL: 200, NowFunc: nowFunc})
	defer h.close()

	var keys []string
	h.withKeys(&keys, 1010, 1, 2, 3).flush()
	h.withKeys(&keys, 1030, 4, 5, 6).withKeys(&keys, 1050, 7, 8).flush()
	h.reopenDB()
	h.assertLatest(0, 1010, 1050)
}
