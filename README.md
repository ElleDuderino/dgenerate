# dGenerate
Parallel Data Generator for Distributed Databases

## Introduction

There is often a need to generate large amounts of realistic data, preferably in parallel, so that it can be ingested in a database
for benchmarking or evaluation or testing of certain feature. In today's distributed world most databases have the ability 
ingest data in parallel from many chunks independently. _dGenerate_ is a data generator which takes a schema description and 
generates data files in parallel, and possibly data for different shards on different nodes so that it can be ingested locally.

## Description

dGenerate generates data for each column of the schema in one or more physical data files. The files can be generated on
one node or on multiple nodes by invoking an instance of this program on each node separately. This makes it very convenient 
when ingesting into columnar distributed databases. You can just `paste` the files for pertinent columns with a delimiter
right before ingesting data. This way you can generate the data upfront and for all columns that are likely to be used but don't 
have to be. That decision can be deferred until actual ingestion and schema definition, of the table to be populated, 
is decided.

## Input

The program reads a JSON file in which the each row specifies the column description that consists of 

* Name (Column name)
* Datatype (can be INT, INTEGER, INT64, FLOAT, FLOAT64, VARCHARnn, CHAR, DATE, TIMESTAMP)
* Distribution (conatins the DISTNAME and DISTPROP that specifies the distribution method and the parameters for 
that distribution). More on the details of distribution [later](#distribution-specification).

## Output

Each column will get its own physical data file, or multiple files if the number of rows to be generated exceeds the 
limit of maximum rows per file (set using environment variable `MAXROWSPERFILE`). The output files are of the form:

`Nodenn_<column_name>_nnn.txt`

e.g., `Node01_custid_012.txt` represents the 12th datafile on the second node (Nodes are 0 based) for column `custid`

## Dependency

This program calls a lot of functions from the package [randomdata](https://github.com/Pallinder/go-randomdata) when it 
comes to generating random numbers or picking meaningful values from a pool of categories like firstName, lastName, Address, 
etc.
Before you do `go install` you will need to do the following as a prerequisite:

```go get github.com/Pallinder/go-randomdata```



## CLI Options

The executable `dgenerate` can be invoked with the following flags:
* `card`: cardinality; the total number of rows that need to be generated across all nodes; default: 1000000
* `nodes`: Total nodes in the database cluster; default:2
* `nodeid`: Optional. If specified then generate data only for that specific node (0 based); Otherwise, generate for all nodes.
* `schemafile`: Complete path of the JSON formatted schema file with column definitions information.

## Environment Variables

There are two environment variables that dGenerate relies upon.
1. `MAXROWSPERFILE`:  The number of rows in each file. If unset, or set to garbage, the defualt value of 100 will be used
2. `MAXGENERATORS`:  The maximum generators withing a single invocation of dgenerate that can be running on a node. When unset,
or set to garbage the default value of 1 (i.e., serial) will be used.

## Distribution Specification
>Under Construction: WORK IN PROGRESS, but golang definitions listed below in the meanwhile for reference
```go
const (
	SERIAL  Distname = "SERIAL"  // values incremented by 1
	URANDOM Distname = "URANDOM" // uniform random distribution from predefined pools
	AMOUNT  Distname = "AMOUNT"  // bounded uniform distribution
	ZIPF    Distname = "ZIPF"    // Zipfian (power law) distribution
	DATE    Distname = "DATE"    // timestamp/date range uniform distribution
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
```

## Examples

### Example 1

```Shell
dgenerate -card=1000 -nodes=3 -schemafile=sample_schema_file
```
Assuming neither `MAXROWSPERFILE` nor `MAXGENERATORS` is set, default values (100, 1) will be assumed. 
This will generate 4 files per column (specified in the schema file `sample_schema_file`) per node serially. All files
will be generated in the directory from where the command was invoked. Each file will 
will conatin up to 100 rows and each node will get 333 files. The four files designated for each of the 3 nodes 
will have 100, 100, 100 and 33 rows each.

### Example 2

```Shell
export MAXROWSPERFILE=1000000
export MAXGENERATORS=64
dgenerate -card=490000000 -nodes=7 -nodeid=4 -schemafile=sample_schema_file
```
This will generate the share of data for the 5th (out of a total of 7 nodes) node. Total rows are 490 million. So, 
the number of rows per column per node (in this case Node04) will be 70 million. Since `MAXROWSPERFILE=1000000`, there will be 70 files generated per 
column (specified in the schema file `sample_schema_file`). Data will be generated 64 way in parallel.


```
**Note**: Since the data is generated independently for all columns  in parallel, dGenerate is not a 
good choice if you want corelation between columns.
```

