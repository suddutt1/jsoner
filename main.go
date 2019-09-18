package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"sync"

	"path/filepath"

	log "github.com/sirupsen/logrus"
)

func main() {
	action := flag.String("a", "consolidate", "Tool action . Valid values are consolidate")
	filePattern := flag.String("p", "*.json", "File pattern to include in processing")
	verbose := flag.Bool("v", false, "Verbose output")
	threads := flag.Int("t", 4, "No of threads")
	flag.Parse()
	if *verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	switch *action {
	case "consolidate":
		fmt.Println("Starting consolidation")
		args := flag.Args()
		if len(args) < 3 {
			log.Fatalf("Invalid input : valid usage is jsoner -a=consolidate <path> <id field> <consfield>")
		}
		files := findFiles(args[0], *filePattern)
		consolidate(files, args[1], args[2], *threads)
	default:
		flag.Usage()
	}
}

func findFiles(rootPath, pattern string) []string {
	files, err := ioutil.ReadDir(rootPath)
	if err != nil {
		log.Fatalf("Unable to read the directory %v", err)
	}
	selectedFiles := make([]string, 0)
	for _, file := range files {
		if isOk, _ := filepath.Match(pattern, file.Name()); isOk {
			log.Debugf("Selecting %s", file.Name())
			selectedFiles = append(selectedFiles, file.Name())
		}
	}
	return selectedFiles
}
func consolidate(selectedFiles []string, idField, summaryField string, threads int) {
	for _, fileName := range selectedFiles {
		log.Info("Reading file ", fileName)
		jsonBytes, err := ioutil.ReadFile(fileName)
		if err != nil {
			log.Fatalf("File reading error %v", err)
		}
		records := make([]map[string]interface{}, 0)
		err = json.Unmarshal(jsonBytes, &records)
		if err != nil {
			log.Fatalf("File parsing error %v", err)
		}
		phoneNumberMap := new(sync.Map)
		totalRecords := len(records)
		log.Infof("File %s has %d records ", fileName, totalRecords)
		summaryMap := make(map[string]int)
		for index := 0; index < totalRecords; index++ {
			record := records[index]
			if idFieldValue, isExisting := record[idField]; isExisting {
				phoneNumberMap.Store(idFieldValue, 1)
			}
			if summaryFieldValue, isExisting := record[summaryField]; isExisting {
				summaryField := fmt.Sprintf("%v", summaryFieldValue)
				if value, isFound := summaryMap[summaryField]; isFound {
					summaryMap[summaryField] = (value + 1)
				} else {
					summaryMap[summaryField] = 1
				}
			}
		}
		log.Infof("Map %+v", summaryMap)
	}

}
