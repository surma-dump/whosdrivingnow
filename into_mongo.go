package main

import (
	"github.com/voxelbrain/goptions"
	// "labix.org/v2/mgo"
	"./drivenow"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	options = struct {
		RawFolder string `goptions:"-r, --raw, obligatory, description='Path to the folder containing the raws'"`
		Dry       bool   `goptions:"-n, --dry-run, description='Dont actually work on the database'"`
	}{}
)

func init() {
	goptions.ParseAndFail(&options)
}

func main() {
	filepath.Walk(options.RawFolder, func(path string, fi os.FileInfo, err error) error {
		if fi.IsDir() {
			return nil
		}
		if err != nil {
			log.Printf("Walking %s failed: %s", path, err)
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			log.Printf("Could not open %s: %s", path, err)
			return nil
		}
		defer f.Close()
		filename := fi.Name()
		strts := filename[0 : len(filename)-len(filepath.Ext(filename))]
		ts, err := strconv.ParseInt(strts, 10, 64)
		if err != nil {
			log.Printf("Invalid filename %s (%s)", path, strts)
			return nil
		}
		timestamp := time.Unix(ts, 0)

		container := struct {
			Rec struct {
				Vehicles struct {
					Vehicles []drivenow.Vehicle `json:"vehicles"`
				} `json:"vehicles"`
			} `json:"rec"`
		}{}
		dec := json.NewDecoder(f)
		err = dec.Decode(&container)
		if err != nil {
			log.Printf("Invalid content in %s: %s", path, err)
			return nil
		}
		log.Printf("%s", timestamp)
		return nil
	})
}
