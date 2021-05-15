package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/buger/jsonparser"
	"github.com/rs/zerolog/log"
)

type record struct {
	t     string
	keys  []string
	value []float64
}

func (r *record) set(data []byte, path string) error {
	t, e := jsonparser.GetString(data, "t")
	if e != nil {
		r.t = time.Now().Format(time.RFC3339)
	} else {
		r.t = t
	}
	if e := jsonparser.ObjectEach(data, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		v, e := jsonparser.GetFloat(value)
		if e != nil {
			log.Error().Str("path", path).Str("key", string(key)).Msg("cannot decode float value")
			return errors.New("decode error")
		}
		r.keys = append(r.keys, string(key))
		r.value = append(r.value, v)
		return nil
	}); e != nil {
		log.Error().Str("path", path).Msg("json decode error")
		return e
	}
	return nil
}

type cache struct {
	path    string
	records []record
}

func (c *cache) set(data []byte, path string) error {
	r := record{}
	if e := r.set(data, path); e != nil {
		return e
	}
	c.records = append(c.records, r)
	return nil
}

type server struct {
	lock         sync.Mutex
	clientID     string
	clientSecret string
	caches       []*cache
	output       string
}

func (s *server) setRecord(data []byte, path string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	newPath := true
	var ca *cache
	for _, c := range s.caches {
		if strings.Compare(path, c.path) == 0 {
			newPath = false
			ca = c
			break
		}
	}
	if newPath {
		c := &cache{
			path:    path,
			records: []record{},
		}
		ca = c
		s.caches = append(s.caches, c)
	}
	if err := ca.set(data, path); err != nil {
		return err
	}
	return nil
}

func flushCache(c *cache) error {
	if len(c.records) == 0 {
		return nil
	}
	keys := []string{}
	for _, r := range c.records {
		for _, k := range r.keys {
			if len(keys) == 0 {
				keys = append(keys, k)
			} else {
				i := sort.SearchStrings(keys, string(k))
				if strings.Compare(keys[i], k) != 0 {
					keys = append(keys, k)
					sort.Strings(keys)
				}
			}
		}
	}
	if err := os.MkdirAll(c.path[1:], 0766); err != nil {
		log.Error().Msgf("cannot create directory %s: %s", c.path[1:], err.Error())
	}
	filePath := path.Join("./", c.path[1:], c.records[0].t+".csv")
	f, err := os.Create(filePath)
	if err != nil {
		log.Error().Msgf("cannot create csv %s: %s", filePath, err.Error())
		return err
	}
	log.Info().Str("path", filePath).Str("t", c.records[0].t).Msgf("write file %s", filePath)
	defer f.Close()
	writer := csv.NewWriter(f)
	if err = writer.Write(keys); err != nil {
		log.Error().Msgf("cannot write csv %s: %s", filePath, err.Error())
		return err
	}
	values := make([]string, len(keys))
	for _, r := range c.records {

		for i := range values {
			values[i] = ""
		}

		for i, k := range r.keys {
			n := sort.SearchStrings(keys, k)
			values[n] = fmt.Sprintf("%f", r.value[i])
		}

		if err = writer.Write(values); err != nil {
			log.Error().Msgf("cannot write csv: %s", err.Error())
			return err
		}
	}
	writer.Flush()

	return nil
}

func (s *server) flush() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	hasError := false
	for _, c := range s.caches {
		if err := flushCache(c); err != nil {
			hasError = true
		}

	}
	s.caches = []*cache{}
	if hasError {
		return errors.New("flush has error")
	}
	return nil
}

type ErrorMessage struct {
	Message string `json:"message"`
}

func (s *server) ServeHTTP(res http.ResponseWriter, req *http.Request) {

	clientID, clientSecret, ok := req.BasicAuth()
	if !ok {
		log.Info().Str("uri", req.RequestURI).Msgf("401 unauthorized %s", req.RequestURI)
		res.WriteHeader(401)
		return
	}
	if clientID != s.clientID || clientSecret != s.clientSecret {
		log.Info().Str("uri", req.RequestURI).Msgf("401 unauthorized %s", req.RequestURI)
		res.WriteHeader(401)
		return
	}

	log.Info().Str("uri", req.RequestURI).Msgf("receive %s", req.RequestURI)

	if req.RequestURI == "/flush" {
		if err := s.flush(); err != nil {
			res.WriteHeader(400)
			b, _ := json.Marshal(ErrorMessage{
				Message: err.Error(),
			})
			_, _ = res.Write(b)
			log.Info().Str("uri", req.RequestURI).Msgf("400 invalid request: %s", req.RequestURI)
			return
		}
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error().Str("uri", req.RequestURI).Msg("err body reading")
		res.WriteHeader(400)
		b, _ := json.Marshal(ErrorMessage{
			Message: "error body reading",
		})
		_, _ = res.Write(b)
		log.Info().Str("uri", req.RequestURI).Msgf("400 invalid request: %s", req.RequestURI)
		return
	}
	if err := s.setRecord(body, req.RequestURI); err != nil {
		res.WriteHeader(400)
		b, _ := json.Marshal(ErrorMessage{
			Message: err.Error(),
		})
		_, _ = res.Write(b)
		log.Info().Str("uri", req.RequestURI).Msgf("400 invalid request: %s", req.RequestURI)
		return
	}
	res.WriteHeader(200)
	_, _ = res.Write([]byte("{}"))
	log.Info().Str("uri", req.RequestURI).Msgf("200 accept: %s", req.RequestURI)
}

func main() {
	s := new(server)
	var flushDurationSec int
	flag.StringVar(&s.clientID, "client-id", "", "basic auth client id")
	flag.StringVar(&s.clientSecret, "client-secret", "", "basic auth client secret")
	flag.StringVar(&s.output, "output", "./", "output dir")
	flag.IntVar(&flushDurationSec, "duration", 24*60*60, "output dir")
	flag.Parse()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Info().Msg("Startup")

	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:8080")
	if err != nil {
		panic(err.Error())
	}
	conn, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Error().Msgf("cannot listen 0.0.0.0:8080 %s", err.Error())
		return
	}

	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(flushDurationSec))
		for {
			<-ticker.C
			s.flush()
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-signalChan
		_ = s.flush()
		conn.Close()
	}()

	if err := http.Serve(conn, s); err != net.ErrClosed {
		log.Error().Msgf("connection error %s", err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}
