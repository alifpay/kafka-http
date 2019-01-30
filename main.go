package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alifpay/kafka"
)

func handler(w http.ResponseWriter, r *http.Request) {
	queryValues := r.URL.Query()
	req := queryValues.Get("req")
	kafka.Produce("8x35zqny-alif.pro", req)

	//How to get message from kafka.Consume
	msg := "Yes, there is. What do you want!"
	fmt.Fprintf(w, "Hi there! Is there %s?, response from kafka: %s!", req, msg)
}
func main() {
	var wg sync.WaitGroup
	err := kafka.Connect("ark-01.srvs.cloudkafka.com:9094,ark-02.srvs.cloudkafka.com:9094,ark-03.srvs.cloudkafka.com:9094",
		"8x35zqny",
		"0u8GzFXvW3G-R7nCCY22q7Je1y9x-WRY", []string{"8x35zqny-alif.pro"})
	if err != nil {
		fmt.Println(err)
		return
	}
	http.HandleFunc("/", handler)
	srv := &http.Server{
		Addr:         ":8182",
		ReadTimeout:  20 * time.Second,
		WriteTimeout: 40 * time.Second,
	}

	ctx, cancelFun := context.WithCancel(context.Background())
	go kafka.Consume(ctx, &wg)
	go func() {
		sigint := make(chan os.Signal)
		signal.Notify(sigint, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGINT)
		s := <-sigint
		fmt.Println(s)
		cancelFun()
		kafka.Close()
		err = srv.Shutdown(ctx)
		if err != nil {
			log.Println("server: couldn't shutdown because of " + err.Error())
		}
	}()

	log.Fatal(srv.ListenAndServe())
	wg.Wait()
}
