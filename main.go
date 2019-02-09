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

	"github.com/alifpay/cache"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/alifpay/kafka"
)

//var Msgs chan string

func handler(w http.ResponseWriter, r *http.Request) {
	queryValues := r.URL.Query()
	val := queryValues.Get("req")

	err := kafka.Produce("8x35zqny-alif.pro", kafka.StrObj{ID: "1234", Val: val})
	if err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	//How to get message from kafka.Consume
	//1.s very simple solution
	// pros: very simple
	// cons: is not stable. sometimes response can be late
	msg := "No, there isn't. try later!"
	i := 0
	for i < 20 {
		time.Sleep(50 * time.Millisecond)
		val, err := cache.Get("1234")
		if err == nil {
			msg = "Yes, there is. What do you want! \n" + string(val)
			break
		}
		i++
	}

	fmt.Fprintf(w, "Hi there! Is there %s?,\nresponse from kafka: %s!", val, msg)
}
func websocket(w http.ResponseWriter, r *http.Request) {
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		// handle error
	}
	go func() {
		defer conn.Close()

		for {
			msg, op, err := wsutil.ReadClientData(conn)
			if err != nil {
				// handle error
			}
			err = wsutil.WriteServerMessage(conn, op, msg)
			if err != nil {
				// handle error
			}

			time.Sleep(500 * time.Millisecond)
			err = wsutil.WriteServerMessage(conn, op, []byte("Message from kafka"))
			if err != nil {
				// handle error
			}
		}
	}()
}

func main() {
	var wg sync.WaitGroup
	//Msgs = make(chan string, 100)
	err := kafka.Connect("ark-01.srvs.cloudkafka.com:9094,ark-02.srvs.cloudkafka.com:9094,ark-03.srvs.cloudkafka.com:9094",
		"8x35zqny",
		"0u8GzFXvW3G-R7nCCY22q7Je1y9x-WRY", []string{"8x35zqny-alif.pro"})
	if err != nil {
		fmt.Println(err)
		return
	}
	http.HandleFunc("/api", handler)
	http.HandleFunc("/websocket", websocket)
	http.Handle("/", http.FileServer(http.Dir("./files")))
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
