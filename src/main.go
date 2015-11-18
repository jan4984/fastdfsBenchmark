package main

import(
	"log"
	_ "github.com/weilaihui/fdfs_client"
	"flag"
	"encoding/json"
)

type testDesc struct{
	TotalCount int
	Write struct{
		Pct int
		Cfg []struct{
			Avg int
			Max int
			Min int
			Pct int
		}
	}
	Read struct{
		Pct int
    }
}

func main(){
	defaultTestDesc := `
{
	"TotalCount":100000,
	"Write":{
		"Pct":99,
		"Cfg":[
			{"Avg":7, "Max":15, "Min":3, "Pct":95},
			{"Avg":200, "Max":300, "Min":100, "Pct":3},
			{"Avg":500, "Max":800, "Min":300, "Pct":2}
		]
	},
	"Read":{
		"Pct":10
	}
}
`
	fdfsAddress := flag.String("fa", "", "(must)fastDFS connection string(like 10.0.0.5:22313)")
	origUsage := flag.Usage
	flag.Usage = func(){
		log.Println("the default test description is")
		log.Println(defaultTestDesc)
		origUsage()
	}
	testDescStr := flag.String("td", "see default test description", "(opt)test description")

	flag.Parse()
	if(*fdfsAddress==""){
		flag.Usage()
		return
	}
	log.Println("to use fastDFS address", *fdfsAddress)
	if(*testDescStr=="see default test description"){
		log.Println("to use default test description")
		*testDescStr = defaultTestDesc
	}

	td := testDesc{}

	if err:=json.Unmarshal([]byte(*testDescStr), &td);err != nil{
		log.Println(err)
		flag.Usage()
		return
	}
}
