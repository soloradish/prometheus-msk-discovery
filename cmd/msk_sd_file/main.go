package main

import (
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/statsbomb/prometheus-msk-discovery/internal/lib"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

type FileSDOptions struct {
	OutFile   string        `short:"o" long:"output" description:"path of the file to write MSK discovery information to" default:"msk_file_sd.yml"`
	Interval  time.Duration `short:"i" long:"scrape-interval" description:"interval at which to scrape the AWS API for MSK cluster information" default:"5m"`
	JobPrefix string        `short:"n" long:"job-prefix" description:"string with which to prefix each job label" default:"msk"`
	Regions   string        `short:"r" long:"regions" description:"the aws region in which to scan for MSK clusters, comma split eg. 'ap-southeast-1,ap-southeast-2'" required:"true"`
	Verbose   bool          `short:"v" description:"show verbose debug information"`
}

var opts FileSDOptions
var parser = flags.NewParser(&opts, flags.Default)

func main() {
	_, err := parser.Parse()
	if err != nil {
		switch flagsErr := err.(type) {
		case flags.ErrorType:
			if flagsErr == flags.ErrHelp {
				os.Exit(0)
			}
			fmt.Printf("%v", err)
			os.Exit(1)
		default:
			fmt.Printf("%v", err)
			os.Exit(1)
		}
	}
	regions := strings.Split(opts.Regions, ",")

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	if opts.Verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	log.Info().
		Str("outfile", opts.OutFile).
		Dur("interval", opts.Interval).
		Str("job_prefix", opts.JobPrefix).
		Strs("regions", regions).Msg("MSK SD Starting...")

	work := func() {
		client := lib.SDClient{
			JobPrefix:  opts.JobPrefix,
			AwsRegions: regions,
		}

		content, err := client.DiscoverAllRegions()
		if err != nil {
			panic(err)
		}
		m, err := yaml.Marshal(content)
		if err != nil {
			fmt.Println(err)
			return
		}

		log.Info().
			Str("outfile", opts.OutFile).
			Msgf("Writing %d discovered exporters to output file", len(content))
		err = ioutil.WriteFile(opts.OutFile, m, 0644)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	s := time.NewTimer(1 * time.Millisecond)
	t := time.NewTicker(opts.Interval)
	for {
		select {
		case <-s.C:
			{
				log.Debug().Msg("Init timer ticked, start first scrape.")
			}
		case <-t.C:
			{
				log.Debug().
					Dur("interval", opts.Interval).
					Msg("Interval ticker ticked, start scrape.")
			}
		}
		work()
	}
}
