// Copyright 2022 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package locate

import (
	"go.uber.org/atomic"
	"time"
)

// number of buckets stored in the stats
const bucketsCount int = 10

const counterBytes int = 3
const counterMusk uint64 = (1 << counterBytes) - 1


type latencyStats struct {
	// each bucket represent the statistics data of 1s, we only keep latest `bucketsCount` data.
	buckets [bucketsCount]bucket
}

func (s *latencyStats) observe(now time.Time, val uint64) {
	ts := now.Unix()
	idx := int(ts) % bucketsCount
	s.buckets[idx].observe(ts, val)
}

const numOfLastStats int = 5

func (s *latencyStats) getLatestStats(ts time.Time) Stats {
	res := make(Stats, 0, numOfLastStats)
	idx := int(ts.Unix()) % bucketsCount
	for i := 0; i < numOfLastStats; i++ {
		res = append(res, s.buckets[(idx + bucketsCount - i) % bucketsCount].load())
	}
	return res
}

type bucket struct {
	countAndTotal atomic.Uint64
	ts            atomic.Int64
}

func (b *bucket) observe(ts int64, ms uint64) {
	oldTs := b.ts.Load()
	// if the bucket is outdated, clean the stats first.
	if oldTs != ts {
		// there is a slight race condition here, but the worse case is that some routine's `Add` operation
		// is cleaned by the `Store`, but this is ok as we don't need to guarantee 100% precise statistics
		if b.ts.CAS(oldTs, ts) {
			b.countAndTotal.Store(0)
		}
	}
	composedValue := (ms << counterBytes) | 1
	b.countAndTotal.Add(composedValue)
}

func (b *bucket) load() SingleStat {
	composedValue := b.countAndTotal.Load()
	return SingleStat{
		Count: composedValue & counterMusk,
		Sum: composedValue >> counterBytes,
	}
}

type SingleStat struct {
	Count uint64
	Sum   uint64
}

func (s SingleStat) Avg() uint64 {
	return s.Sum / s.Count
}

type Stats []SingleStat

func (s Stats) TotalCount() uint64 {
	count := uint64(0)
	for _, st := range s {
		count += st.Count
	}
	return count
}

const attenuateFactor float64 = 0.9

func (s Stats) WeightedAvg() uint64 {
	totalSum := 0.0
	totalCount := 0.0
	factor := 1.0
	for _, st := range s {
		totalCount += float64(st.Count) * factor
		totalSum += float64(st.Sum) * factor
		factor *= attenuateFactor
	}
	if totalCount < 100.0 {
		return 0.0
	}

	return uint64(totalSum / totalCount)
}


