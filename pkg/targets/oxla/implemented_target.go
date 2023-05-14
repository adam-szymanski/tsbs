package oxla

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
)

func NewTarget() targets.ImplementedTarget {
	return &oxlaTarget{}
}

type oxlaTarget struct{}

func (c oxlaTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("implement me")
}

func (c oxlaTarget) Serializer() serialize.PointSerializer {
	return &timescaledb.Serializer{}
}

func (c oxlaTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"host", "localhost", "Hostname of Oxla instance")
}

func (c oxlaTarget) TargetName() string {
	return constants.FormatOxla
}
