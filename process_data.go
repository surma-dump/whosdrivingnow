package main

import (
	"./drivenow"
	"encoding/json"
	"github.com/voxelbrain/goptions"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	options = struct {
		RawFolder   string        `goptions:"-r, --raw, obligatory, description='Path to the folder containing the raws'"`
		SkipImport  bool          `goptions:"--skip-import, description='Skip importing'"`
		SkipIndexes bool          `goptions:"--skip-indexes, description='Skip creating indexes'"`
		MongoURL    *url.URL      `goptions:"-m, --mongodb, description='URL pointing to MongoDB'"`
		Help        goptions.Help `goptions:"-h, --help, description='Show this help'"`
	}{
		MongoURL: MustURL(url.Parse("mongodb://localhost")),
	}
)

func init() {
	goptions.ParseAndFail(&options)
}

func main() {
	session, err := mgo.Dial(options.MongoURL.String())
	if err != nil {
		log.Fatalf("Could not connect to %s: %s", options.MongoURL.String(), err)
	}
	defer session.Close()
	collection := session.DB("").C("raw")
	if !options.SkipImport {
		c := readFiles(options.RawFolder)
		for v := range c {
			err := collection.Insert(v)
			if err != nil {
				log.Printf("Inserting of %s (%s) failed: %s", v.Name, v.Timestamp, err)
				continue
			}
		}
	}

	if !options.SkipIndexes {
		log.Printf("Creating indexes...")
		collection.EnsureIndex(mgo.Index{
			Key: []string{"timestamp"},
		})
		collection.EnsureIndex(mgo.Index{
			Key: []string{"name"},
		})
	}

	names := []string{}
	err = collection.Find(nil).Distinct("name", &names)
	if err != nil {
		log.Fatalf("Could not query car names: %s", err)
	}
	for _, name := range names {
		log.Printf("Extracting routes of %s...", name)
		iter := collection.Find(bson.M{
			"name": name,
		}).Sort("timestamp").Iter()
		var cur, last drivenow.Vehicle
		for iter.Next(&cur) {
			log.Printf("cur = %#v, last = %#v", cur, last)
			last = cur
		}
	}
}

func MustURL(u *url.URL, err error) *url.URL {
	if err != nil {
		panic(err)
	}
	return u
}

func readFiles(rawfolder string) chan *drivenow.Vehicle {
	c := make(chan *drivenow.Vehicle)
	go filepath.Walk(options.RawFolder, func(path string, fi os.FileInfo, err error) error {
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
		log.Printf("Decoding %s...", filename)
		dec := json.NewDecoder(f)
		err = dec.Decode(&container)
		if err != nil {
			log.Printf("Invalid content in %s: %s", path, err)
			return nil
		}
		for _, v := range container.Rec.Vehicles.Vehicles {
			v.Timestamp = timestamp
			c <- &v
		}
		close(c)
		return nil
	})
	return c
}
