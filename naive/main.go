package main

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/profile"
)

type record struct {
	name string
	temp float32
}

type siteAgg struct {
	name string
	min  float32
	max  float32
	sum  float32
	num  int
}

func newSiteAggFromRecord(r *record) *siteAgg {
	return &siteAgg{
		name: r.name,
		min:  r.temp,
		max:  r.temp,
		sum:  r.temp,
		num:  1,
	}
}

func (sa *siteAgg) ingest(r *record) error {
	if sa.name != r.name {
		return fmt.Errorf("cannot ingest record with different name %v vs %v", sa.name, r.name)
	}
	if r.temp < sa.min {
		sa.min = r.temp
	}
	if r.temp > sa.max {
		sa.max = r.temp
	}
	sa.sum += r.temp
	sa.num += 1
	return nil
}

func (sa *siteAgg) merge(that *siteAgg) error {
	if sa.name != that.name {
		return fmt.Errorf("cannot merge aggregates with different name %v vs %v", sa.name, that.name)
	}
	if that.min < sa.min {
		sa.min = that.min
	}
	if that.max > sa.max {
		sa.max = that.max
	}
	sa.sum += that.sum
	sa.num += that.num
	return nil
}

func (sa *siteAgg) toLine() []byte {
	avg := (float64(sa.sum) / float64(sa.num)) + 0.05
	return []byte(fmt.Sprintf("%v;%.1f;%.1f;%.1f\n", sa.name, sa.min, sa.max, avg))
}

func main() {
	defer profile.Start(profile.ProfilePath(".")).Stop()

	maxLineBufs := 1000
	lineBufLen := 10000
	recBufLen := 10000
	lineParsers := 40
	recordAggregators := 40

	wg := &sync.WaitGroup{}

	f, err := os.Open("/tmp/measurements.txt")
	if err != nil {
		fmt.Printf("error opening measurements file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	output, err := os.OpenFile("results.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("failed to open output file: %v\n", err)
		os.Exit(1)
	}
	defer output.Close()

	lineBufs := make(chan []string, maxLineBufs)
	recBufs := make(chan []*record, lineParsers*maxLineBufs)

	scanner := bufio.NewScanner(f)

	wg.Add(1)
	go func() {
		defer wg.Done()
		lineBuf := make([]string, 0, lineBufLen)
		for scanner.Scan() {
			l := scanner.Text()
			if strings.HasPrefix(l, "#") {
				continue
			}
			lineBuf = append(lineBuf, l)
			if len(lineBuf) >= lineBufLen {
				lineBufs <- lineBuf
				lineBuf = make([]string, 0, lineBufLen)
			}
		}
		if len(lineBuf) > 0 {
			lineBufs <- lineBuf
		}
		close(lineBufs)
	}()

	lpwg := &sync.WaitGroup{}

	lpwg.Add(lineParsers)
	for i := 0; i < lineParsers; i++ {
		go func() {
			defer lpwg.Done()
			recBuf := make([]*record, 0, recBufLen)
			for lb := range lineBufs {
				for _, l := range lb {
					rec, err := parseLine(l)
					if err != nil {
						fmt.Printf("error parsing line %v: %v\n", l, err)
						continue
					}
					recBuf = append(recBuf, rec)
					if len(recBuf) >= recBufLen {
						recBufs <- recBuf
						recBuf = make([]*record, 0, recBufLen)
					}
				}
				if len(recBuf) > 0 {
					recBufs <- recBuf
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		lpwg.Wait()
		close(recBufs)
	}()

	aggChunks := make([]map[string]*siteAgg, recordAggregators)

	wg.Add(recordAggregators)
	for i := 0; i < recordAggregators; i++ {
		agg := make(map[string]*siteAgg)
		go func() {
			defer wg.Done()
			for rb := range recBufs {
				for _, r := range rb {
					sa, ok := agg[r.name]
					if !ok {
						agg[r.name] = newSiteAggFromRecord(r)
						continue
					}
					sa.ingest(r)
				}
			}
		}()

		//CHECK: do we have to block before setting this?
		aggChunks[i] = agg
	}

	wg.Wait()

	aggResults := make(map[string]*siteAgg)
	for _, ac := range aggChunks {
		for k, v := range ac {
			sa, ok := aggResults[k]
			if !ok {
				aggResults[k] = ac[k]
				continue
			}
			sa.merge(v)
		}
	}

	aggArray := make([]*siteAgg, len(aggResults))
	i := 0
	for k := range aggResults {
		aggArray[i] = aggResults[k]
		i++
	}

	slices.SortFunc(aggArray, func(a, b *siteAgg) int {
		if a.name < b.name {
			return -1
		}
		if a.name > b.name {
			return 1
		}
		return 0
	})

	for _, agg := range aggArray {
		l := agg.toLine()
		fmt.Printf("%v;%v\n", agg.name, agg.num)
		_, err := output.Write(l)
		if err != nil {
			fmt.Printf("error while writing line %v to output file: %v\n", string(l), err)
			os.Exit(1)
		}
	}
}

func parseLine(line string) (*record, error) {
	parts := strings.Split(line, ";")
	if len(parts) != 2 {
		return nil, fmt.Errorf("malformed line: %v", line)
	}

	val, err := strconv.ParseFloat(parts[1], 32)
	if err != nil {
		return nil, fmt.Errorf("error parsing temp on line %v: %v", line, err)
	}

	return &record{
		name: parts[0],
		temp: float32(val),
	}, nil
}
