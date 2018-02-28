package dbf

// Mutator have Name and Params
type Mutator struct {
	Name   string                 `json:"name"`
	Params map[string]interface{} `json:"params"`
}

// Column represents columns of table
type Column struct {
	Name     string   `json:"name"`
	Ordinal  int      `json:"ordinal"`
	DataType string   `json:"type"`
	Mutator  *Mutator `json:"mutator"`
}

// Schema represents a table
type Schema struct {
	Columns []*Column `json:"columns"`
}
