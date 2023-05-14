package oxla

import (
	"fmt"
)

// Loading option vars:
type LoadingOptions struct {
	Host               string `yaml:"host"`
	Port               string
	connectString      string
	CreateMetricsTable bool     `yaml:"create-metrics-table" mapstructure:"create-metrics-table"`
	TagColumnTypes     []string `yaml:",omitempty" mapstructure:",omitempty"`

	TimeIndex          bool `yaml:"time-index" mapstructure:"time-index"`
	TimePartitionIndex bool `yaml:"time-partition-index" mapstructure:"time-partition-index"`
	PartitionIndex     bool `yaml:"partition-index" mapstructure:"partition-index"`

	FieldIndex string `yaml:"field-index" mapstructure:"field-index"`
	UseJSON    bool   `yaml:"use-jsonb-tags" mapstructure:"use-jsonb-tags"`
	InTableTag bool   `yaml:"in-table-partition-tag" mapstructure:"in-table-partition-tag"`
	DbName     string
}

func (o *LoadingOptions) GetConnectString() string {
	// User might be passing in host=hostname the connect string out of habit which may override the
	// multi host configuration. Same for dbname= and user=. This sanitizes that.
	connectString := fmt.Sprintf("host=%s", o.Host)

	// For optional parameters, ensure they exist then interpolate them into the connectString
	if len(o.Port) > 0 {
		connectString = fmt.Sprintf("%s port=%s", connectString, o.Port)
	}

	return connectString
}
