package main

import (
	"./drivenow"
	"encoding/json"
	"github.com/voxelbrain/goptions"
	"labix.org/v2/mgo"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	options = struct {
		RawFolder string        `goptions:"-r, --raw, obligatory, description='Path to the folder containing the raws'"`
		Dry       bool          `goptions:"-n, --dry-run, description='Dont actually work on the database'"`
		MongoURL  *url.URL      `goptions:"-m, --mongodb, description='URL pointing to MongoDB'"`
		Help      goptions.Help `goptions:"-h, --help, description='Show this help'"`
	}{
		MongoURL: MustURL(url.Parse("mongodb://localhost")),
	}
)

func init() {
	goptions.ParseAndFail(&options)
}

func main() {
	var collection *mgo.Collection
	if !options.Dry {
		session, err := mgo.Dial(options.MongoURL.String())
		if err != nil {
			log.Fatalf("Could not connect to %s: %s", options.MongoURL.String(), err)
		}
		defer session.Close()
		collection = session.DB("").C("raw")
	}
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
		log.Printf("Adding %4d entries of %s", len(container.Rec.Vehicles.Vehicles), timestamp)
		for i, v := range container.Rec.Vehicles.Vehicles {
			v.Timestamp = timestamp
			if !options.Dry {
				err := collection.Insert(v)
				if err != nil {
					log.Printf("Inserting %4d failed: %s", i, err)
					continue
				}
			}
		}
		return nil
	})
}

func MustURL(u *url.URL, err error) *url.URL {
	if err != nil {
		panic(err)
	}
	return u
}
