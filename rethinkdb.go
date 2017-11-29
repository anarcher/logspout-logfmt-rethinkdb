package lr

import (
	"github.com/gliderlabs/logspout/router"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-logfmt/logfmt"
	r "gopkg.in/gorethink/gorethink.v3"

	"os"
	"strings"
)

const (
	FIELD_TIME = "_time"
)

func init() {
	router.AdapterFactories.Register(NewLogfmtRethinkdbAdapter, "logfmt-rethinkdb")
}

type LogfmtRethinkdbAdapter struct {
	route          *router.Route
	session        *r.Session
	rethinkdbDB    string
	rethinkdbTable string
	logger         log.Logger
	debug          bool
}

func NewLogfmtRethinkdbAdapter(route *router.Route) (router.LogAdapter, error) {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	logger = level.NewFilter(logger, level.AllowAll())
	logger = log.With(logger, "caller", log.DefaultCaller)

	//Rethinkdb session
	//TODO(anarcher) session ConnectOpts.InitialCap,MaxOpen,Cluster address
	session, err := r.Connect(r.ConnectOpts{
		Address: route.Address,
	})
	if err != nil {
		level.Error(logger).Log("err", err)
		return nil, err
	}

	db := getopt(route.Options, "rethinkdb_db", "RETHINKDB_DB", "logs")
	table := getopt(route.Options, "rethinkdb_table", "RETHINKDB_TABLE", "logs")
	debug := getopt(route.Options, "debug", "DEBUG", "false")

	a := &LogfmtRethinkdbAdapter{
		session:        session,
		route:          route,
		rethinkdbDB:    db,
		rethinkdbTable: table,
		logger:         logger,
	}
	if debug == "true" {
		a.debug = true
	}

	level.Info(logger).Log("adapter", "logfmr-rethinkgb", "addr", route.Address, "db", db, "table", table, "debug", debug, "start", true)

	return a, nil

}

func (a *LogfmtRethinkdbAdapter) Stream(logstream chan *router.Message) {
	level.Debug(a.logger).Log("stream", true)

	for m := range logstream {
		log := a.transformLog(m)
		log[FIELD_TIME] = m.Time
		a.insertLog(log)
	}

}

func (a LogfmtRethinkdbAdapter) transformLog(m *router.Message) map[string]interface{} {
	log := make(map[string]interface{})

	d := logfmt.NewDecoder(strings.NewReader(m.Data))
	for d.ScanRecord() {
		for d.ScanKeyval() {
			log[string(d.Key())] = string(d.Value())
		}
	}
	if d.Err() != nil {
		level.Error(a.logger).Log("err", d.Err())
	}

	return log
}

func (a LogfmtRethinkdbAdapter) insertLog(log map[string]interface{}) {
	_, err := r.DB(a.rethinkdbDB).Table(a.rethinkdbTable).Insert(log).RunWrite(a.session)
	if err != nil {
		level.Error(a.logger).Log("err", err)
	}
}

func getopt(options map[string]string, optkey string, envkey string, default_value string) (value string) {
	value = options[optkey]
	if value == "" {
		value = os.Getenv(envkey)
		if value == "" {
			value = default_value
		}
	}

	return
}
