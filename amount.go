package main

import (
	"fmt"
	rnd "github.com/Pallinder/go-randomdata"
	"log"
	"strings"
)

func (a AmountFragment) Process() {
	f := func(t string) bool {
		return strings.HasPrefix(strings.ToUpper(a.Datatype), t)
	}
	switch {
	case f("INT"):
		a.processInt()
	case f("FLOAT"):
		a.processFloat()
	default:
		log.Fatalf("Unsupported Amount type\n")
	}
}
func (a AmountFragment) Print() {
	fmt.Printf("%+v %+v\n", a.Fragment, a.Ad)
}

func (a AmountFragment) processInt() {
	fd, f := a.GetOutputFile()
	defer fd.Close()
	var val int

	for i := 0; i < a.NumRows; i++ {
		val = rnd.Number(a.Ad.AmtStart, a.Ad.AmtEnd)
		_, err := f.WriteString(fmt.Sprintf("%v\n", val))
		if err != nil {
			log.Fatalf("Error generating data for Fragment %v\n", a.Fragment)
		}
		f.Flush()
	}

}

func (a AmountFragment) processFloat() {
	fd, f := a.GetOutputFile()
	defer fd.Close()
	var val float64

	for i := 0; i < a.NumRows; i++ {
		val = rnd.Decimal(a.Ad.AmtStart, a.Ad.AmtEnd)
		_, err := f.WriteString(fmt.Sprintf("%v\n", val))
		if err != nil {
			log.Fatalf("Error generating data for Fragment %v\n", a.Fragment)
		}
		f.Flush()
	}

}
