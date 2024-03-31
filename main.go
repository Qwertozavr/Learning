package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"io"
	//"os"
	"sync"
	"time"

	//natsd "github.com/nats-io/nats-server/v2/server"
	//stand "github.com/nats-io/nats-streaming-server/server"

	"text/template"
	//"go.uber.org/zap"
	"github.com/jackc/pgx/v5"
	//"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

type model struct {
	Order_uid    string `json: "order_uid"`
	Track_number string `json: "track_number"`
	Entry        string `json: "entry"`
	Delivery     struct {
		Name    string `json: "name"`
		Phone   string `json: "phone"`
		Zip     string `json: "zip"`
		City    string `json: "city"`
		Address string `json: "address"`
		Region  string `json: "region"`
		Email   string `json: "email"`
	} `json: "delivery"`
	Payment struct {
		Transaction   string `json: "transaction"`
		Request_id    string `json: "request_id"`
		Currency      string `json: "currency"`
		Provider      string `json: "provider"`
		Amount        int    `json: "amount"`
		Payment_dt    int    `json: "payment_dt"`
		Bank          string `json: "bank"`
		Delivery_cost int    `json: "delivery_cost"`
		Goods_total   int    `json: "goods_total"`
		Custom_fee    int    `json: "custom_fee"`
	} `json: "payment"`
	Items []struct {
		Chrt_id      int    `json: "chrt_id"`
		Track_number string `json: "track_number"`
		Price        int    `json: "price"`
		Rid          string `json: "rid"`
		Name         string `json: "name"`
		Sale         int    `json: "sale"`
		Size         string `json: "size"`
		Total_price  int    `json: "total_price"`
		Nm_id        int    `json: "nm_id"`
		Brand        string `json: "brand"`
		Status       int    `json: "status"`
	} `json: "items"`
	Locale            string `json: "locale"`
	Interal_signature string `json: "interal_signature"`
	Customer_id       string `json: "customer_id"`
	Delivery_service  string `json: "delivery_service"`
	Shardkey          string `json: "shardkey"`
	Sm_id             int    `json: "sm_id"`
	Date_created      string `json: "date_created"`
	Oof_shard         string `json: "oof_shard"`
}

// Область хранения
var cach map[string]model

// nats connect
func natsConnect(conn *pgx.Conn, mutex *sync.Mutex) (stan.Conn, stan.Subscription) {
	// Подключение к nats server
	stream_chan, err := stan.Connect("test-cluster", "client-1", stan.NatsURL("nats://localhost:4444"))
	if err != nil {
		log.Fatal("Connection to nats channel failed, error: ", err)
		return nil, nil
	}

	subscribe, err := stream_chan.Subscribe("test-cluster", func(msg *stan.Msg) {
		var new_order model
		// Парсинг закодированного json
		err := json.Unmarshal(msg.Data, &new_order)
		if err != nil {
			log.Fatal("Parsing JSON failed: ", err)
		} else {
			// Блокирование go-рутин, для надёжности
			mutex.Lock()
			cach[new_order.Order_uid] = new_order
			mutex.Unlock()

			_, err = conn.Exec(context.Background(), "insert into wb.order values ($1, $2)", "@"+new_order.Order_uid, string(msg.Data))
			if err != nil {
				log.Fatal("Insert failed: ", err)
			}
		}
	})
	if err != nil {
		log.Fatal("Subscribe failed: ", err)
		return nil, nil
	}
	return stream_chan, subscribe
}

func psg_connect() *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), "postgres://wb_learning:123@localhost:5432/wb_l0")
	if err != nil {
		log.Fatal("Connect to BD failed: ", err)
	}
	return conn
}

func http_server_start() *http.Server {
	http_server := &http.Server{
		Addr: "localhost:8111",
	}
	server_run := make(chan bool)

	go func(server_run chan bool) {
		server_run <- true
		err := http_server.ListenAndServe()
		if err != nil {
			log.Fatal("Server is down.\n")
		}
	}(server_run)

	<-server_run
	close(server_run)
	if http_server != nil {
		log.Printf("Server started\n")
	}
	return http_server
}

func Handler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Welcome\n"))
}

func server_handler(conn *pgx.Conn, sc stan.Conn, mutex *sync.Mutex) {
	http.HandleFunc("/", main_page(conn, sc, mutex))
	http.Handle("/web/", http.StripPrefix("/web/", http.FileServer(http.Dir("web"))))
}

func main_page(conn *pgx.Conn, sc stan.Conn, mutex *sync.Mutex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var input_uid string
		output_order := model{}

		if r.Method == http.MethodPost {
			new_file, _, err := r.FormFile("file")
			if err != nil {
				log.Fatal("Reading file failed(1): ", err)
			} else {
				data, err := io.ReadAll(new_file)
				if err != nil {
					log.Fatal("Reading file failed(2): ", err)
				} else {
					var new_order model
					err := json.Unmarshal(data, &new_order)
					if err != nil {
						log.Fatal("Parsing JSON failed: ", err)
					}

					err = sc.Publish("test-cluster", data)
					if err != nil {
						log.Fatal("Massege to NATS failed: ", err)
					}

				}
				new_file.Close()
			}
		} else if r.Method == http.MethodGet {

			var input_uid string
			output_order := model{}

			input_uid = "@" + r.FormValue("order_uid")
			if cach[input_uid].Order_uid == "" {
				restore_cach(conn, mutex)
			}
			output_order = cach[input_uid]
			var out string = "Order_uid: " + output_order.Order_uid + "\nTrack_number: " + output_order.Track_number
			log.Println(out)
			//fmt.Fprintf(w, out)
		} // b563feb7b2b84b6test

		data := map[string]interface{}{
			"JSONs": cach,
			"UID":   input_uid,
			"order": output_order,
		}

		t, _ := template.ParseFiles("main.html")
		err := t.Execute(w, data)
		if err != nil {
			log.Fatal("Error")
		}
	}
}

func restore_cach(conn *pgx.Conn, mutex *sync.Mutex) {
	rows, err := conn.Query(context.Background(), "select * from wb.order")
	if err != nil {
		log.Fatal("Query fail ", err)
	}
	for rows.Next() {
		var new_order_data []byte
		var id string
		var new_order model
		err := rows.Scan(&id, &new_order_data)
		if err != nil {
			log.Fatal("Scan failed: ", err)
		}
		err = json.Unmarshal(new_order_data, &new_order)
		if err != nil {
			log.Fatal("Parsing JSON failed: ", err)
		}
		mutex.Lock()
		cach[id] = new_order
		mutex.Unlock()
	}
}

func main() {
	cach = make(map[string]model)
	var mu sync.Mutex

	conn := psg_connect()
	defer conn.Close(context.Background())
	if conn == nil {
		log.Fatal("Connection to Psg failed!")
		return
	}

	restore_cach(conn, &mu)

	sc, sub := natsConnect(conn, &mu)
	defer sub.Unsubscribe()
	defer sc.Close()
	if sc == nil || sub == nil {
		log.Fatal("Connection to NATS failed!")
		return
	}

	server := http_server_start()
	defer server.Shutdown(context.Background())
	if server == nil {
		log.Fatal("Start server failed")
		return
	}

	go server_handler(conn, sc, &mu)

	var stop string
	fmt.Scan(&stop)

	log.Fatal("Bye!")

	time.Sleep(1 * time.Second)
}
