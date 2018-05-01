package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tylerb/graceful"

	billing "github.com/weaveworks/billing-client"
	"github.com/weaveworks/common/aws"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/network"
	"github.com/weaveworks/go-checkpoint"
	"github.com/weaveworks/scope/app"
	"github.com/weaveworks/scope/app/multitenant"
	"github.com/weaveworks/scope/common/weave"
	"github.com/weaveworks/scope/common/xfer"
	"github.com/weaveworks/scope/probe/docker"
	"github.com/weaveworks/weave/common"
)

const (
	memcacheUpdateInterval = 1 * time.Minute
	httpTimeout            = 90 * time.Second
)

var (
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "scope",
		Name:      "request_duration_seconds",
		Help:      "Time in seconds spent serving HTTP requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route", "status_code", "ws"})
)

func init() {
	prometheus.MustRegister(requestDuration)
	billing.MustRegisterMetrics()
}

// Router creates the mux for all the various app components.
func router(collector app.Collector, controlRouter app.ControlRouter, pipeRouter app.PipeRouter, externalUI bool, capabilities map[string]bool, metricsGraphURL string) http.Handler {
	router := mux.NewRouter().SkipClean(true)

	// We pull in the http.DefaultServeMux to get the pprof routes
	router.PathPrefix("/debug/pprof").Handler(http.DefaultServeMux)
	router.Path("/metrics").Handler(prometheus.Handler())

	app.RegisterReportPostHandler(collector, router)
	app.RegisterControlRoutes(router, controlRouter)
	app.RegisterPipeRoutes(router, pipeRouter)
	app.RegisterTopologyRoutes(router, app.WebReporter{Reporter: collector, MetricsGraphURL: metricsGraphURL}, capabilities)

	uiHandler := http.FileServer(GetFS(externalUI))
	router.PathPrefix("/ui").Name("static").Handler(
		middleware.PathRewrite(regexp.MustCompile("^/ui"), "").Wrap(
			uiHandler))
	router.PathPrefix("/").Name("static").Handler(uiHandler)

	instrument := middleware.Instrument{
		RouteMatcher: router,
		Duration:     requestDuration,
	}
	return instrument.Wrap(router)
}

func collectorFactory(userIDer multitenant.UserIDer, collectorURL, s3URL, natsHostname string,
	memcacheConfig multitenant.MemcacheConfig, window time.Duration, createTables bool) (app.Collector, error) {
	if collectorURL == "local" {
		return app.NewCollector(window), nil
	}

	parsed, err := url.Parse(collectorURL)
	if err != nil {
		return nil, err
	}

	switch parsed.Scheme {
	case "file":
		return app.NewFileCollector(parsed.Path, window)
	case "dynamodb":
		s3, err := url.Parse(s3URL)
		if err != nil {
			return nil, fmt.Errorf("Valid URL for s3 required: %v", err)
		}
		dynamoDBConfig, err := aws.ConfigFromURL(parsed)
		if err != nil {
			return nil, err
		}
		s3Config, err := aws.ConfigFromURL(s3)
		if err != nil {
			return nil, err
		}
		bucketName := strings.TrimPrefix(s3.Path, "/")
		tableName := strings.TrimPrefix(parsed.Path, "/")
		s3Store := multitenant.NewS3Client(s3Config, bucketName)
		var memcacheClient *multitenant.MemcacheClient
		if memcacheConfig.Host != "" {
			memcacheClient = multitenant.NewMemcacheClient(memcacheConfig)
		}
		awsCollector, err := multitenant.NewAWSCollector(
			multitenant.AWSCollectorConfig{
				UserIDer:       userIDer,
				DynamoDBConfig: dynamoDBConfig,
				DynamoTable:    tableName,
				S3Store:        &s3Store,
				NatsHost:       natsHostname,
				MemcacheClient: memcacheClient,
				Window:         window,
			},
		)
		if err != nil {
			return nil, err
		}
		if createTables {
			if err := awsCollector.CreateTables(); err != nil {
				return nil, err
			}
		}
		return awsCollector, nil
	}

	return nil, fmt.Errorf("Invalid collector '%s'", collectorURL)
}

func emitterFactory(collector app.Collector, clientCfg billing.Config, userIDer multitenant.UserIDer, emitterCfg multitenant.BillingEmitterConfig) (*multitenant.BillingEmitter, error) {
	billingClient, err := billing.NewClient(clientCfg)
	if err != nil {
		return nil, err
	}
	emitterCfg.UserIDer = userIDer
	return multitenant.NewBillingEmitter(
		collector,
		billingClient,
		emitterCfg,
	)
}

func controlRouterFactory(userIDer multitenant.UserIDer, controlRouterURL string) (app.ControlRouter, error) {
	if controlRouterURL == "local" {
		return app.NewLocalControlRouter(), nil
	}

	parsed, err := url.Parse(controlRouterURL)
	if err != nil {
		return nil, err
	}

	if parsed.Scheme == "sqs" {
		prefix := strings.TrimPrefix(parsed.Path, "/")
		sqsConfig, err := aws.ConfigFromURL(parsed)
		if err != nil {
			return nil, err
		}
		return multitenant.NewSQSControlRouter(sqsConfig, userIDer, prefix), nil
	}

	return nil, fmt.Errorf("Invalid control router '%s'", controlRouterURL)
}

func pipeRouterFactory(userIDer multitenant.UserIDer, pipeRouterURL, consulInf string) (app.PipeRouter, error) {
	if pipeRouterURL == "local" {
		return app.NewLocalPipeRouter(), nil
	}

	parsed, err := url.Parse(pipeRouterURL)
	if err != nil {
		return nil, err
	}

	if parsed.Scheme == "consul" {
		consulClient, err := multitenant.NewConsulClient(parsed.Host)
		if err != nil {
			return nil, err
		}
		advertise, err := network.GetFirstAddressOf(consulInf)
		if err != nil {
			return nil, err
		}
		addr := fmt.Sprintf("%s:4444", advertise)
		return multitenant.NewConsulPipeRouter(consulClient, strings.TrimPrefix(parsed.Path, "/"), addr, userIDer), nil
	}

	return nil, fmt.Errorf("Invalid pipe router '%s'", pipeRouterURL)
}

// Main runs the app
func appMain(flags appFlags) {
	setLogLevel(flags.logLevel)
	setLogFormatter(flags.logPrefix)
	runtime.SetBlockProfileRate(flags.blockProfileRate)

	defer log.Info("app exiting")
	rand.Seed(time.Now().UnixNano())
	app.UniqueID = strconv.FormatInt(rand.Int63(), 16)
	app.Version = version
	log.Infof("app starting, version %s, ID %s", app.Version, app.UniqueID)
	logCensoredArgs()

	userIDer := multitenant.NoopUserIDer
	if flags.userIDHeader != "" {
		userIDer = multitenant.UserIDHeader(flags.userIDHeader)
	}

	collector, err := collectorFactory(
		userIDer, flags.collectorURL, flags.s3URL, flags.natsHostname,
		multitenant.MemcacheConfig{
			Host:             flags.memcachedHostname,
			Timeout:          flags.memcachedTimeout,
			Expiration:       flags.memcachedExpiration,
			UpdateInterval:   memcacheUpdateInterval,
			Service:          flags.memcachedService,
			CompressionLevel: flags.memcachedCompressionLevel,
		},
		flags.window, flags.awsCreateTables)
	if err != nil {
		log.Fatalf("Error creating collector: %v", err)
		return
	}

	if flags.BillingEmitterConfig.Enabled {
		billingEmitter, err := emitterFactory(collector, flags.BillingClientConfig, userIDer, flags.BillingEmitterConfig)
		if err != nil {
			log.Fatalf("Error creating emitter: %v", err)
			return
		}
		defer billingEmitter.Close()
		collector = billingEmitter
	}

	controlRouter, err := controlRouterFactory(userIDer, flags.controlRouterURL)
	if err != nil {
		log.Fatalf("Error creating control router: %v", err)
		return
	}

	pipeRouter, err := pipeRouterFactory(userIDer, flags.pipeRouterURL, flags.consulInf)
	if err != nil {
		log.Fatalf("Error creating pipe router: %v", err)
		return
	}

	// Start background version checking
	checkpoint.CheckInterval(&checkpoint.CheckParams{
		Product: "scope-app",
		Version: app.Version,
		Flags:   makeBaseCheckpointFlags(),
	}, versionCheckPeriod, func(r *checkpoint.CheckResponse, err error) {
		if err != nil {
			log.Errorf("Error checking version: %v", err)
		} else if r.Outdated {
			log.Infof("Scope version %s is available; please update at %s",
				r.CurrentVersion, r.CurrentDownloadURL)
			app.NewVersion(r.CurrentVersion, r.CurrentDownloadURL)
		}
	})

	// Periodically try and register our IP address in WeaveDNS.
	if flags.weaveEnabled && flags.weaveHostname != "" {
		weave, err := newWeavePublisher(
			flags.dockerEndpoint, flags.weaveAddr,
			flags.weaveHostname, flags.containerName)
		if err != nil {
			log.Println("Failed to start weave integration:", err)
		} else {
			defer weave.Stop()
		}
	}

	capabilities := map[string]bool{
		xfer.HistoricReportsCapability: collector.HasHistoricReports(),
	}
	handler := router(collector, controlRouter, pipeRouter, flags.externalUI, capabilities, flags.metricsGraphURL)
	if flags.logHTTP {
		handler = middleware.Log{
			LogRequestHeaders: flags.logHTTPHeaders,
		}.Wrap(handler)
	}

	httpServer := &http.Server{
		Addr:           flags.listen,
		Handler:        handler,
		ReadTimeout:    httpTimeout,
		WriteTimeout:   httpTimeout,
		MaxHeaderBytes: 1 << 20,
	}

	if flags.tlsEnabled {
		log.Debugf("TLS enabled")
		if flags.tlsCert == "" {
			log.Fatalf("no server certificate supplied for TLS")
			return
		} else if flags.tlsKey == "" {
			log.Fatalf("no private key supplied for TLS")
			return
		}
		log.Debugf("using server certificate: %s", flags.tlsCert)
		log.Debugf("using private key: %s", flags.tlsKey)
		var minVersion uint16 = tls.VersionTLS10
		if flags.tlsMinVersion != "" {
			log.Debugf("setting minimum TLS version: %s", flags.tlsMinVersion)
			switch flags.tlsMinVersion {
			case "1.0":
				minVersion = tls.VersionTLS10
			case "1.1":
				minVersion = tls.VersionTLS11
			case "1.2":
				minVersion = tls.VersionTLS12
			default:
				base := 10
				if strings.HasPrefix(flags.tlsMinVersion, "0x") {
					base = 16
				}
				val, err := strconv.ParseInt(flags.tlsMinVersion, base, 16)
				if err != nil {
					log.Fatalf("parsing value of app.tls.minVersion: %s", err)
					return
				}
				minVersion = uint16(val)
			}
		}
		var cipherSuites []uint16
		if flags.tlsCipherSuites != "" {
			log.Debugf("setting TLS cipher suites: %s", flags.tlsCipherSuites)
			values := strings.Split(flags.tlsCipherSuites, ",")
			for _, strVal := range values {
				strVal = strings.TrimSpace(strVal)
				if strVal == "" {
					continue
				}
				var cipher uint16
				switch strings.ToUpper(strVal) {
				case "TLS_RSA_WITH_RC4_128_SHA":
					cipher = tls.TLS_RSA_WITH_RC4_128_SHA
				case "TLS_RSA_WITH_3DES_EDE_CBC_SHA":
					cipher = tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA
				case "TLS_RSA_WITH_AES_128_CBC_SHA":
					cipher = tls.TLS_RSA_WITH_AES_128_CBC_SHA
				case "TLS_RSA_WITH_AES_256_CBC_SHA":
					cipher = tls.TLS_RSA_WITH_AES_256_CBC_SHA
				case "TLS_RSA_WITH_AES_128_CBC_SHA256":
					cipher = tls.TLS_RSA_WITH_AES_128_CBC_SHA256
				case "TLS_RSA_WITH_AES_128_GCM_SHA256":
					cipher = tls.TLS_RSA_WITH_AES_128_GCM_SHA256
				case "TLS_RSA_WITH_AES_256_GCM_SHA384":
					cipher = tls.TLS_RSA_WITH_AES_256_GCM_SHA384
				case "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA":
					cipher = tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA
				case "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA":
					cipher = tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA
				case "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA":
					cipher = tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA
				case "TLS_ECDHE_RSA_WITH_RC4_128_SHA":
					cipher = tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA
				case "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA":
					cipher = tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA
				case "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA":
					cipher = tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA
				case "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA":
					cipher = tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
				case "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256":
					cipher = tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256
				case "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256":
					cipher = tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256
				case "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":
					cipher = tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
				case "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256":
					cipher = tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
				case "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":
					cipher = tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
				case "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384":
					cipher = tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
				case "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305":
					cipher = tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305
				case "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305":
					cipher = tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305
				default:
					base := 10
					if strings.HasPrefix(strVal, "0x") {
						base = 16
					}
					val, err := strconv.ParseUint(strVal, base, 16)
					if err != nil {
						log.Fatalf("parsing value of app.tls.cipherSuites: %s", err)
						return
					}
					cipher = uint16(val)
				}
				cipherSuites = append(cipherSuites, cipher)
			}
		}
		var curvePrefs []tls.CurveID
		if flags.tlsCurvePrefs != "" {
			log.Debugf("setting TLS curve preferences: %s", flags.tlsCurvePrefs)
			values := strings.Split(flags.tlsCurvePrefs, ",")
			for _, strVal := range values {
				strVal = strings.TrimSpace(strVal)
				if strVal == "" {
					continue
				}
				var curve tls.CurveID
				switch strings.ToUpper(strVal) {
				case "CURVEP256":
					curve = tls.CurveP256
				case "CURVEP384":
					curve = tls.CurveP384
				case "CURVEP512":
					curve = tls.CurveP521
				case "X25519":
					curve = tls.X25519
				default:
					base := 10
					if strings.HasPrefix(strVal, "0x") {
						base = 16
					}
					val, err := strconv.ParseUint(strVal, base, 16)
					if err != nil {
						log.Fatalf("parsing value of app.tls.curvePrefs: %s", err)
						return
					}
					curve = tls.CurveID(val)
				}
				curvePrefs = append(curvePrefs, curve)

			}
		}
		tlsConfig := &tls.Config{
			MinVersion:       minVersion,
			CipherSuites:     cipherSuites,
			CurvePreferences: curvePrefs,
		}
		if flags.tlsClientAuth {
			log.Debug("TLS client certificate authentication enabled")
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
			if flags.tlsDNRegexp != "" {
				log.Debugf("allowing only clients with cert DNs matching regexp: %s", flags.tlsDNRegexp)
				dnRegexp, err := regexp.Compile(flags.tlsDNRegexp)
				if err != nil {
					log.Fatalf("parsing client auth DN regular expression: %s", err)
					return
				}
				tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
					peerDNs := make([]string, len(verifiedChains))
					for i, certChain := range verifiedChains {
						peerCert := certChain[0]
						peerDN := peerCert.Subject.String()
						peerDNs[i] = peerDN
					}
					log.Debugf("received peer DNs: %s", peerDNs)
					for _, peerDN := range peerDNs {
						if dnRegexp.MatchString(peerDN) {
							log.Infof("allowed client certificate DN: %s", peerDN)
							return nil
						}
					}
					return fmt.Errorf("peer DNs do not match allowed regexp: %s", peerDNs)
				}
			}
			if flags.tlsCACerts != "" {
				log.Debugf("reading CA certificates from file: %s", flags.tlsCACerts)
				cacerts, err := ioutil.ReadFile(flags.tlsCACerts)
				if err != nil {
					log.Fatalf("reading CA certs file: %s", err)
					return
				}
				certPool := x509.NewCertPool()
				if ok := certPool.AppendCertsFromPEM(cacerts); !ok {
					log.Fatalf("no PEM certificates found in CA certs file")
					return
				}
				tlsConfig.ClientCAs = certPool
			}
		}
		httpServer.TLSConfig = tlsConfig
	}

	server := &graceful.Server{
		// we want to manage the stop condition ourselves below
		NoSignalHandling: true,
		Server:           httpServer,
	}
	if flags.tlsEnabled {
		go func() {
			log.Infof("listening for TLS connections on %s", flags.listen)
			if err := server.ListenAndServeTLS(flags.tlsCert, flags.tlsKey); err != nil {
				log.Error(err)
			}
		}()
	} else {
		go func() {
			log.Infof("listening on %s", flags.listen)
			if err := server.ListenAndServe(); err != nil {
				log.Error(err)
			}
		}()
	}

	// block until INT/TERM
	common.SignalHandlerLoop()
	// stop listening, wait for any active connections to finish
	server.Stop(flags.stopTimeout)
	<-server.StopChan()
}

func newWeavePublisher(dockerEndpoint, weaveAddr, weaveHostname, containerName string) (*app.WeavePublisher, error) {
	dockerClient, err := docker.NewDockerClientStub(dockerEndpoint)
	if err != nil {
		return nil, err
	}
	weaveClient := weave.NewClient(weaveAddr)
	return app.NewWeavePublisher(
		weaveClient,
		dockerClient,
		app.Interfaces,
		weaveHostname,
		containerName,
	), nil
}
