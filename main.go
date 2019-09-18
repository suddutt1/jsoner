package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

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
		if len(args) < 4 {
			log.Fatalf("Invalid input : valid usage is jsoner -a=consolidate <path> <id field> <consfield>")
		}
		files := findFiles(args[0], *filePattern)
		consolidate(files, args[1], args[2], args[3], *threads)
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

type fieldDetails struct {
	SummaryField string
	Resolver     string
}

func consolidate(selectedFiles []string, idField, summaryField, resolverField string, threads int) {
	summaryMap := make(map[string]int)
	idFieldMap := make(map[string]fieldDetails)
	startTime := time.Now()
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

		totalRecords := len(records)
		log.Infof("File %s has %d records ", fileName, totalRecords)

		for index := 0; index < totalRecords; index++ {
			record := records[index]
			if idFieldValue, isExisting := record[idField]; isExisting {
				idField := fmt.Sprintf("%v", idFieldValue)
				summaryFieldValue, _ := record[summaryField]
				summaryFieldValueStr := fmt.Sprintf("%v", summaryFieldValue)
				rslvField, _ := record[resolverField]
				rslvFieldValueStr := fmt.Sprintf("%v", rslvField)
				if existingSummary, isExisting := idFieldMap[idField]; isExisting {
					if existingSummary.SummaryField != summaryFieldValueStr {
						//We need to prioritize
						if strings.Compare(rslvFieldValueStr, existingSummary.Resolver) > 0 {
							idFieldMap[idField] = fieldDetails{SummaryField: summaryFieldValueStr, Resolver: rslvFieldValueStr}
						}
					}

				} else {
					idFieldMap[idField] = fieldDetails{SummaryField: summaryFieldValueStr, Resolver: rslvFieldValueStr}
				}

			}

		}
		log.Infof("Unique records so far %d", len(idFieldMap))

	}

	log.Infof("Final consolidation result")
	log.Infof("Total number of unique records in all the files %d", len(idFieldMap))
	log.Infof("Consolidation result based on %s", summaryField)

	for _, summaryFieldValue := range idFieldMap {
		if value, isFound := summaryMap[summaryFieldValue.SummaryField]; isFound {
			summaryMap[summaryFieldValue.SummaryField] = (value + 1)
		} else {
			summaryMap[summaryFieldValue.SummaryField] = 1
		}
	}
	for key, count := range summaryMap {
		log.Infof("%s=%d", key, count)
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	log.Infof("Tool excution completed %s", duration.String())
}
