package main

import (
	"io"
	"log"
	"net/http"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func getMetrics(hostAndPort string) (map[string]*dto.MetricFamily, error) {
	resp, err := http.Get("http://" + hostAndPort + "/metrics")
	if err != nil {
		log.Printf("error getting metrics: %v", err)
		return nil, err
	}

	return processMetrics(resp.Body)
}

func processMetrics(metricsReader io.Reader) (map[string]*dto.MetricFamily, error) {
	var parser expfmt.TextParser
	mf, err := parser.TextToMetricFamilies(metricsReader)
	if err != nil {
		return nil, err
	}
	return mf, nil
}
