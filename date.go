package main

import (
	"fmt"
	rnd "github.com/Pallinder/go-randomdata"
	"log"
	"strings"
	"time"
)

const (
	DateFormat = "2006-Jan-02"
)

func (d DateFragment) Process() {
	f := func(t string) bool {
		return strings.HasPrefix(strings.ToUpper(d.Datatype), t)
	}
	switch {
	case f("DATE"):
		{
			ODateFormat := DateFormat
			d.processDate(ODateFormat)
		}
	case f("TIMESTAMP"):
		{
			ODateFormat := "2006-Jan-02 15:04:05"
			d.processDate(ODateFormat)
		}
	default:
		log.Fatalf("Unsupported Date type\n")
	}
}
func (d DateFragment) Print() {
	fmt.Printf("%+v %+v\n", d.Fragment, d.Dd)
}

func (d DateFragment) processDate(ODateFormat string) {
	fd, f := d.GetOutputFile()
	defer fd.Close()
	var val string

	startDate, err := time.Parse(DateFormat, d.Dd.DateStart)
	if err != nil {
		log.Fatalf("Cannot parse start date\n")
	}
	endDate, err := time.Parse(DateFormat, d.Dd.DateEnd)
	if err != nil {
		log.Fatalf("Cannot parse end date\n")
	}

	if !endDate.After(startDate) {
		log.Fatalf("End date has to be later than start date\n")
	}

	hours := int(endDate.Sub(startDate).Hours())

	for i := 0; i < d.NumRows; i++ {
		offset := rnd.Number(hours) * -1
		val = endDate.Add(time.Duration(offset) * time.Hour).Format(ODateFormat)
		_, err := f.WriteString(fmt.Sprintf("%v\n", val))
		if err != nil {
			log.Fatalf("Error generating data for Fragment %v\n", d.Fragment)
		}
		f.Flush()
	}

}
