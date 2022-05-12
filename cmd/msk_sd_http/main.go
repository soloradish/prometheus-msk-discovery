package main

import (
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/statsbomb/prometheus-msk-discovery/internal/lib"
	"gopkg.in/yaml.v2"
	"net/http"
	"os"
	"strings"
)

type HttpSDOptions struct {
	JobPrefix string `short:"n" long:"job-prefix" description:"string with which to prefix each job label" default:"msk"`
	Regions   string `short:"r" long:"regions" description:"the aws region in which to scan for MSK clusters, comma split eg. 'ap-southeast-1,ap-southeast-2'" required:"true"`
	Verbose   bool   `short:"v" description:"show verbose debug information"`
	Port      uint   `short:"p" description:"http server listen port" default:"8000"`
}

var opts HttpSDOptions
var parser = flags.NewParser(&opts, flags.Default)

func discoverHandler(sdc lib.SDClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content, err := sdc.DiscoverAllRegions()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Warn().Err(err).Msg("Error when DiscoverAllRegions")
			return
		}

		m, err := yaml.Marshal(content)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Warn().Err(err).Msg("Error when Marshal to yaml")
			return
		}

		w.Write(m)
		return
	})
}

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
		Uint("port", opts.Port).
		Str("job_prefix", opts.JobPrefix).
		Strs("regions", regions).Msg("MSK Http SD Starting...")

	client := lib.SDClient{
		JobPrefix:  opts.JobPrefix,
		AwsRegions: regions,
	}

	handler := discoverHandler(client)
	http.Handle("/", handler)
	http.ListenAndServe(fmt.Sprintf(":%v", opts.Port), nil)
}
