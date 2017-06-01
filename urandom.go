package main

import (
	"fmt"
	rnd "github.com/Pallinder/go-randomdata"
	"log"
	"strconv"
	"strings"
)

func (u URandomFragment) Process() {
	f := func(t string) bool {
		return strings.HasPrefix(strings.ToUpper(u.Datatype), t)
	}
	switch {
	case f("INT"):
		u.processInt()
	case f("FLOAT"):
		u.processFloat()
	case f("VARCHAR"): // random runes
		{
			var len int
			len, err := strconv.Atoi(strings.TrimPrefix(strings.ToUpper(u.Datatype), "VARCHAR"))
			if err != nil || len > 62 {
				len = 32
			}
			u.processVarchar(len)
		}
	case f("CHAR"): // VARCHAR data but from specific pools
		u.processChar()
	default:
		log.Fatalf("Unsupported random type\n")
	}
}
func (u URandomFragment) Print() {
	fmt.Printf("%+v %+v\n", u.Fragment, u.Ud)
}

func (u URandomFragment) processInt() {
	fd, f := u.GetOutputFile()
	defer fd.Close()
	var val int

	for i := 0; i < u.NumRows; i++ {
		switch {
		case u.Ud.Ndv == 0:
			val = rnd.Number(u.RangeStart, u.RangeEnd)
		case u.Ud.Ndv > 0 && u.Ud.Ndv < cardinality:
			val = rnd.Number(0, cardinality-1) % u.Ud.Ndv
		default:
			val = rnd.Number(0, cardinality-1)
		}

		_, err := f.WriteString(fmt.Sprintf("%v\n", val))
		if err != nil {
			log.Fatalf("Error generating data for Fragment %v\n", u.Fragment)
		}
		f.Flush()
	}

}

func (u URandomFragment) processFloat() {
	fd, f := u.GetOutputFile()
	defer fd.Close()
	var val float64

	for i := 0; i < u.NumRows; i++ {
		switch {
		case u.Ud.Ndv == 0:
			val = rnd.Decimal(0, u.NumRows)
		default:
			val = rnd.Decimal(0, cardinality-1)
		}

		_, err := f.WriteString(fmt.Sprintf("%v\n", val))
		if err != nil {
			log.Fatalf("Error generating data for Fragment %v\n", u.Fragment)
		}
		f.Flush()
	}

}

func (u URandomFragment) processVarchar(len int) {
	fd, f := u.GetOutputFile()
	defer fd.Close()
	var val string

	for i := 0; i < u.NumRows; i++ {
		val = rnd.RandStringRunes(len)
		_, err := f.WriteString(fmt.Sprintf("%v\n", val))
		if err != nil {
			log.Fatalf("Error generating data for Fragment %v\n", u.Fragment)
		}
		f.Flush()
	}

}

func (u URandomFragment) processChar() {
	fd, f := u.GetOutputFile()
	defer fd.Close()
	var val string
	var fn func() string

	pool := strings.ToUpper(u.Ud.Pool)
	switch pool {
	case "FIRSTNAME":
		fn = func() string {
			return rnd.FirstName(rnd.RandomGender)
		}
	case "LASTNAME":
		fn = rnd.LastName
	case "EMAIL":
		fn = rnd.Email
	case "CITY":
		fn = rnd.City
	case "STATE":
		fn = func() string {
			return rnd.State(rnd.Large)
		}
	case "ADDRESS":
		fn = func() string {
			s := rnd.Address()
			return strings.Join(strings.Split(s, "\n"), " ")
		}
	case "PARAGRAPH":
		fn = rnd.Paragraph
	case "IPV4":
		fn = rnd.IpV4Address
	case "IPV6":
		fn = rnd.IpV6Address
	case "MAC":
		fn = rnd.MacAddress
	default:
		fn = rnd.SillyName

	}

	for i := 0; i < u.NumRows; i++ {
		val = fn()
		_, err := f.WriteString(fmt.Sprintf("%v\n", val))
		if err != nil {
			log.Fatalf("Error generating data for Fragment %v\n", u.Fragment)
		}
		f.Flush()
	}

}
