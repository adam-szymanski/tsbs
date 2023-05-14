// tsbs_load_clickhouse loads a ClickHouse instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"fmt"
	"log"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/oxla"
)

// Global vars
var (
	target targets.ImplementedTarget
)

var loader load.BenchmarkRunner
var loaderConf load.BenchmarkRunnerConfig
var conf *oxla.LoadingOptions

// Parse args:
func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	loaderConf = load.BenchmarkRunnerConfig{}
	target := oxla.NewTarget()
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	conf = &oxla.LoadingOptions{
		Host:               viper.GetString("host"),
		DbName:             loaderConf.DBName,
		CreateMetricsTable: true,
	}

	loader = load.GetBenchmarkRunner(loaderConf)
}

func main() {
	loader.RunBenchmark(oxla.NewBenchmark(loaderConf.FileName, loaderConf.HashWorkers, conf))
}
