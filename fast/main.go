package main

import (
	"fmt"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/pkg/profile"
)

const (
	nameBytes = 27
	splitSize = 32 * 1048576
	readers   = 48
	//readers = 1
)

type siteAgg struct {
	min  int
	max  int
	sum  int
	num  int
	name [nameBytes]byte
}

func newSiteAgg(name [nameBytes]byte, tenths int) *siteAgg {
	return &siteAgg{
		name: name,
		min:  tenths,
		max:  tenths,
		sum:  tenths,
		num:  1,
	}
}

func (sa *siteAgg) ingest(name [nameBytes]byte, tenths int) error {
	if tenths < sa.min {
		sa.min = tenths
	}
	if tenths > sa.max {
		sa.max = tenths
	}
	sa.sum += tenths
	sa.num += 1
	return nil
}

func (sa *siteAgg) merge(that *siteAgg) error {
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
	avg := float32(float32(sa.sum)/float32(sa.num)) + 0.5
	return []byte(fmt.Sprintf("%s;%.1f;%.1f;%.1f\n", baToString(sa.name), float32(sa.min)/10.0, float32(sa.max)/10.0, float32(avg)/10.0))
}

func main() {
	defer profile.Start(profile.ProfilePath(".")).Stop()

	f, err := os.Open("/tmp/measurements.txt")
	if err != nil {
		fmt.Printf("error opening measurements file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("error getting measurements file stats: %v\n", err)
		os.Exit(1)
	}

	splits := (int(fi.Size()) / splitSize)
	if int(fi.Size())%splitSize > 0 {
		splits += 1
	}
	fmt.Printf("splits: %v\n", splits)

	output, err := os.OpenFile("results.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("failed to open output file: %v\n", err)
		os.Exit(1)
	}
	defer output.Close()

	scraps := make([][]byte, splits)
	aggChan := make(chan map[[nameBytes]byte]*siteAgg, splits)

	arwg := &sync.WaitGroup{}

	aggResults := make(map[[nameBytes]byte]*siteAgg)
	arwg.Add(1)
	go func() {
		defer arwg.Done()
		for aggMap := range aggChan {
			for k, v := range aggMap {
				sa, ok := aggResults[k]
				if !ok {
					aggResults[k] = aggMap[k]
					continue
				}
				err := sa.merge(v)
				if err != nil {
					fmt.Printf("error merging agg %#v into sa %#v: %v", v, sa, err)
				}
			}
		}
	}()

	rwg := &sync.WaitGroup{}
	sem := make(chan struct{}, readers)

	rwg.Add(splits)
	go func() {
		for i := 0; i < splits; i++ {
			sem <- struct{}{}
			go func(idx int) {
				defer rwg.Done()
				defer func() { <-sem }()

				buf := make([]byte, splitSize)
				n, err := f.ReadAt(buf, int64(idx)*int64(splitSize))
				if err != nil {
					if err != io.EOF || n == 0 {
						fmt.Printf("error reading file split %v: %v\n", idx, err)
						os.Exit(1)
					}
				}

				split, scrap := bytesToSplit(buf, n)
				scraps[idx] = scrap

				splitToAggs(split, aggChan)
			}(i)
		}
	}()

	rwg.Wait()

	//process the table scraps
	scrapBuf := slices.Concat(scraps...)
	splitToAggs(scrapBuf, aggChan)
	close(aggChan)

	arwg.Wait()

	aggArray := make([]*siteAgg, len(aggResults))
	i := 0
	for k := range aggResults {
		aggArray[i] = aggResults[k]
		i++
	}

	slices.SortFunc(aggArray, func(a, b *siteAgg) int {
		if a.name == b.name {
			return 0
		}
		if baToString(a.name) < baToString(b.name) {
			return -1
		}
		return 1
	})

	total := 0
	for _, agg := range aggArray {
		total += agg.num
		l := agg.toLine()
		_, err := output.Write(l)
		if err != nil {
			fmt.Printf("error while writing line %v to output file: %v\n", string(l), err)
			os.Exit(1)
		}
	}
	fmt.Printf("records: %v\n", total)
}

func bytesToSplit(buf []byte, n int) ([]byte, []byte) {
	s, e := 0, n-1
	for {
		if buf[s] != '\n' {
			s += 1
		}
		if buf[e] != '\n' {
			e -= 1
		}
		if buf[s] == '\n' && buf[e] == '\n' {
			break
		}
		if s > e {
			fmt.Printf("never found a newline in file split")
			os.Exit(1)
		}
	}

	head := buf[:s+1]
	tail := buf[e+1 : n]

	scrapLen := len(head) + len(tail)
	scrap := append(make([]byte, 0, scrapLen), head...)
	scrap = append(scrap, tail...)

	return buf[s+1 : e+1], scrap
}

func splitToAggs(bb []byte, aggChan chan map[[nameBytes]byte]*siteAgg) {
	tenths := 0
	mag := 0
	i := 0
	aggs := make(map[[nameBytes]byte]*siteAgg, 500)
	s := len(bb) - 2
	var e int
	for {
		tenths = 0
		mag = 1
		i = s
		for {
			if bb[i] == '.' {
				i--
				continue
			}
			if bb[i] == '-' {
				tenths = -tenths
				e = i - 1
				s = e
				break
			}
			if bb[i] == ';' {
				e = i
				s = e
				break
			}
			tenths += (int(bb[i]) - 48) * mag
			mag = 10 * mag
			i--
		}
		var n [nameBytes]byte
		i = s - 1
		for {
			if bb[i] == '\n' {
				copy(n[:], bb[i+1:e])
				s = i - 1
				e = s
				break
			}
			if i == 0 {
				copy(n[:], bb[:e])
				s = 0
				break
			}
			i--
		}
		sa, ok := aggs[n]
		if !ok {
			aggs[n] = newSiteAgg(n, tenths)
		} else {
			err := sa.ingest(n, tenths)
			if err != nil {
				fmt.Printf("failed to ingest data to agg: %v\n", err)
				os.Exit(1)
			}
		}
		if s == 0 {
			break
		}
	}
	aggChan <- aggs
}

func baToString(nameArray [nameBytes]byte) string {
	i := len(nameArray) - 1
	for i >= 0 {
		if nameArray[i] != 0 {
			break
		}
		i--
	}
	return string(nameArray[:i+1])
}
