package oxla

import (
	"bufio"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
	"go.uber.org/atomic"
)

const dbType = "clickhouse"

// String values of tags and fields to insert - string representation
type insertData struct {
	tags   string // hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
	fields string // 1451606400000000000,58,2,24,61,22,63,6,44,80,38
}

var tagColumnTypes []string

// Point is a single row of data keyed by which table it belongs
// Ex.:
// tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
// cpu,1451606400000000000,58,2,24,61,22,63,6,44,80,38
type point struct {
	table string
	row   *insertData
}

// scan.Batch interface implementation
type tableArr struct {
	m   map[string][]*insertData
	cnt uint
}

// scan.Batch interface implementation
func (ta *tableArr) Len() uint {
	return ta.cnt
}

// scan.Batch interface implementation
func (ta *tableArr) Append(item data.LoadedPoint) {
	that := item.Data.(*point)
	k := that.table
	ta.m[k] = append(ta.m[k], that.row)
	ta.cnt++
}

// scan.BatchFactory interface implementation
type factory struct{}

// scan.BatchFactory interface implementation
func (f *factory) New() targets.Batch {
	return &tableArr{
		m:   map[string][]*insertData{},
		cnt: 0,
	}
}

const tagsPrefix = "tags"

func NewBenchmark(file string, hashWorkers bool, conf *LoadingOptions) targets.Benchmark {
	return &benchmark{
		ds: &fileDataSource{
			scanner: bufio.NewScanner(load.GetBufferedReader(file)),
		},
		hashWorkers: hashWorkers,
		conf:        conf,
	}
}

// targets.Benchmark interface implementation
type benchmark struct {
	ds          targets.DataSource
	hashWorkers bool
	conf        *LoadingOptions
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.ds
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

type hostnameIndexer struct {
	val        atomic.Int32
	partitions uint
}

func (i *hostnameIndexer) GetIndex(item data.LoadedPoint) uint {
	val := i.val.Add(1)
	return uint(val) % i.partitions
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if b.hashWorkers {
		return &hostnameIndexer{
			partitions: maxPartitions,
		}
	}
	return &targets.ConstantIndexer{}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{opts: b.conf, driver: "pgx", dbName: b.conf.DbName}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{ds: b.GetDataSource(), opts: b.conf, driver: "pgx"}
}
