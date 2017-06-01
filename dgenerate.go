package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
)

type Distname string

const (
	SERIAL  Distname = "SERIAL"  // values incremented by 1
	URANDOM Distname = "URANDOM" // uniform random distribution from predefined pools
	AMOUNT  Distname = "AMOUNT"  // bounded uniform distribution
	ZIPF    Distname = "ZIPF"    // Zipfian (power law) distribution
	DATE    Distname = "DATE"    // timestamp/date range uniform distribution
)

var (
	cardinality int    // total rows for the whole dataset
	nodes       int    // number of nodes for/on which to generate the data
	nodeid      int    // the 0 based index for the node for which to generate
	schemaFile  string // json schema description, one row per column (hmm, db humor)
	rowsPerNode int    // share of the cardinality per node
	fragments   int    // number of actual data files per column per node

	// environment variables
	maxRowsPerFile int // maximum number of rows per output file
	maxGenerators  int // number of go routines to run in parallel for generating data
)

// Distribution
type Dist struct {
	DistName Distname        `json:"Distname"`
	DistProp json.RawMessage `json:"Distprop"`
}

// DistProp can be any of the following specialized structs that capture the
// parameters of the specific distribution
type SerialDist struct {
	NumStart int `json:"NumStart"` // starting number
}

type URandomDist struct {
	Pool         string `json:"Pool"`         // pool of values to pick from https://github.com/Pallinder/go-randomdata
	Ndv          int    `json:"Ndv"`          // Number of distinct values (best effort)
	MinBatchSize int    `json:"MinBatchSize"` // This will be a thing in the future
}

type AmountDist struct {
	AmtStart int `json:"AmtStart"`
	AmtEnd   int `json:"AmtEnd"`
}
type DateDist struct {
	DateStart string `json:"DateStart"`
	DateEnd   string `json:"DateEnd"`
}

type ZipfDist struct {
	S    float64 `json:"S"`    // Math stuff
	V    float64 `json:"V"`    // More Math stuff
	Imax uint64  `json:"Imax"` // Range of values between 0 and this number
}

// Column Definitions read from the schema file
type ColumnDef struct {
	ColName  string `json:"Name"`
	Datatype string `json:"Datatype"`
	Dist     Dist   `json:"Distribution"`
}

// Fragments for generating data
type Fragment struct {
	ColName    string
	Datatype   string
	NodeId     int
	Fragment   int
	NumRows    int
	RangeStart int
	RangeEnd   int
}

type Processor interface {
	Process()
	// For Debugging
	Print()
}

// Processor interface implementers
type SerialFragment struct {
	Fragment
	Sd *SerialDist
}

type URandomFragment struct {
	Fragment
	Ud *URandomDist
}
type AmountFragment struct {
	Fragment
	Ad *AmountDist
}
type DateFragment struct {
	Fragment
	Dd *DateDist
}
type ZipfFragment struct {
	Fragment
	Zd *ZipfDist
}

func main() {

	// Parse the arguments and initialize variables
	initialize()

	// Read the schema file and populate the column definitions
	colDefs := processSchemaFile(schemaFile)

	rowsPerNode = cardinality / nodes
	if rowsPerNode < maxRowsPerFile {
		fragments = 1
	} else {
		fragments = int(math.Ceil(float64(rowsPerNode) / float64(maxRowsPerFile)))
	}

	// Create requests that are fragments, or units of work that can be executed
	// concurrently by maxGenerators go routines to generate data files
	requests := createRequests(colDefs)
	fmt.Printf("Total requests = %v\n", len(requests))
	var wg sync.WaitGroup
	sem := make(chan int, maxGenerators)
	for i, r := range requests {
		sem <- 1
		wg.Add(1)
		fmt.Printf("Request #%d:\n", i)
		go func(r Processor) {
			r.Print()
			r.Process()
			wg.Done()
			<-sem
		}(r)
	}
	wg.Wait()
}

func initialize() {

	// All dash arguments get declared here before the call to Parse
	flag.IntVar(&cardinality, "card", 1000000, "cardinality")
	flag.IntVar(&nodes, "nodes", 2, "number of nodes")
	flag.IntVar(&nodeid, "nodeid", -1, "0 based node index")
	flag.StringVar(&schemaFile, "schemafile", "", "schema file")

	flag.Parse()

	if schemaFile == "" {
		log.Fatalf("Usage: dgenerate -card=<cardinality> -nodes=<total nodes> -nodeid=<generate for nodeid> -schemafile=<schemafilename>\n")
	}
	if nodeid >= nodes {
		log.Fatalf("Cannot generate for node id = %v when total nodes (0 based) are %v\n", nodeid, nodes)
	}

	// Now get the environment variables
	mrp_val, set := os.LookupEnv("MAXROWSPERFILE")
	if !set {
		maxRowsPerFile = 100
	} else {
		var err error
		maxRowsPerFile, err = strconv.Atoi(mrp_val)
		if err != nil {
			maxRowsPerFile = 100
		}
	}

	mgr_val, set := os.LookupEnv("MAXGENERATORS")
	if !set {
		maxGenerators = 1
	} else {
		var err error
		maxGenerators, err = strconv.Atoi(mgr_val)
		if err != nil {
			maxGenerators = 1
		}
	}
}

func processSchemaFile(schemaFile string) []ColumnDef {
	colDefs := make([]ColumnDef, 0)
	f, err := os.Open(schemaFile)
	if err != nil {
		log.Fatalf("Unable to open schema file %v\n", schemaFile)
	}

	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Erorr reading schema file\n")
		}
		// Process line
		var cdef ColumnDef
		err = json.Unmarshal(line, &cdef)
		if err != nil {
			log.Fatalf("Error unmarshaling the record in schema file\n")
		}
		colDefs = append(colDefs, cdef)
	}

	err = f.Close()
	if err != nil {
		log.Fatalf("Erorr closing schema file\n")
	}

	return colDefs
}

func createRequests(colDefs []ColumnDef) []Processor {
	requests := make([]Processor, 0)
	for _, c := range colDefs {
		var di = c.Dist.GetDistInfo()
		for n := 0; n < nodes; n++ {
			offset := n * rowsPerNode
			var f, start, end int
			var r Fragment
			for f = 0; f < fragments-1; f++ {
				start = offset + f*maxRowsPerFile
				end = start + maxRowsPerFile - 1
				r = Fragment{
					ColName:    c.ColName,
					Datatype:   c.Datatype,
					NodeId:     n,
					Fragment:   f,
					NumRows:    maxRowsPerFile,
					RangeStart: start,
					RangeEnd:   end,
				}
				fi := c.Dist.GetFragment(r, di)
				if nodeid == n || nodeid == -1 {
					requests = append(requests, fi)
				}
			}
			// The last fragment needs special attention
			start = offset + f*maxRowsPerFile
			end = offset + rowsPerNode - 1
			r = Fragment{
				ColName:    c.ColName,
				Datatype:   c.Datatype,
				NodeId:     n,
				Fragment:   f,
				NumRows:    end - start + 1,
				RangeStart: start,
				RangeEnd:   end,
			}
			fi := c.Dist.GetFragment(r, di)
			if nodeid == n || nodeid == -1 {
				requests = append(requests, fi)
			}

		}

	}

	return requests
}

func (d Dist) GetDistInfo() interface{} {
	var dst interface{}
	switch d.DistName {

	case SERIAL:
		dst = new(SerialDist)
	case URANDOM:
		dst = new(URandomDist)
	case AMOUNT:
		dst = new(AmountDist)
	case DATE:
		dst = new(DateDist)
	case ZIPF:
		dst = new(ZipfDist)

	}
	err := json.Unmarshal(d.DistProp, dst)
	if err != nil {
		log.Fatalf("Unable to unmarshal Distprop\n")
	}
	return dst

}

func (d Dist) GetFragment(fragment Fragment, di interface{}) Processor {
	switch d.DistName {
	case SERIAL:
		return SerialFragment{
			Fragment: fragment,
			Sd:       di.(*SerialDist),
		}
	case URANDOM:
		return URandomFragment{
			Fragment: fragment,
			Ud:       di.(*URandomDist),
		}
	case AMOUNT:
		return AmountFragment{
			Fragment: fragment,
			Ad:       di.(*AmountDist),
		}
	case DATE:
		return DateFragment{
			Fragment: fragment,
			Dd:       di.(*DateDist),
		}
	case ZIPF:
		return ZipfFragment{
			Fragment: fragment,
			Zd:       di.(*ZipfDist),
		}

	}
	return nil
}

func (f Fragment) GetOutputFile() (*os.File, *bufio.Writer) {
	filename := fmt.Sprintf("Node%02d_%v_%03d.txt", f.NodeId, f.ColName, f.Fragment)
	fp, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Unable to open file %v for writing\n", filename)
	}
	w := bufio.NewWriter(fp)
	return fp, w
}
