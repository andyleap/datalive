package datalive

import (
	"strconv"
)

// JSONPath navigates a "path" of json keys/indexes
func JSONPath(data interface{}, path string) interface{} {
	return jsonPath(data, JSONPathSplit(path))
}

func JSONPathSplit(path string) []string {
	mode := 0
	cur := make([]rune, 0, 20)
	split := []string{}
	for _, v := range path {
		switch mode {
		case 0:
			if v == '.' {
				split = append(split, string(cur))
				cur = cur[:0]
				break
			}
			if v == '\\' {
				mode = 1
				break
			}
			cur = append(cur, v)
		case 1:
			cur = append(cur, v)
			mode = 0
		}
	}
	if len(cur) > 0 {
		split = append(split, string(cur))
	}
	return split
}

func jsonPath(data interface{}, path []string) interface{} {
	if len(path) == 0 {
		return data
	}
	switch data := data.(type) {
	case []interface{}:
		index, err := strconv.Atoi(path[0])
		if err != nil {
			return nil
		}
		if index < 0 || index > len(data) {
			return nil
		}
		return jsonPath(data[index], path[1:])
	case map[string]interface{}:
		return jsonPath(data[path[0]], path[1:])
	}
	return nil
}

type Set []string

func (s *Set) Set(key string) {
	for _, k := range *s {
		if k == key {
			return
		}
	}
	*s = append(*s, key)
}

func (s *Set) Clear(key string) {
	for i, k := range *s {
		if k == key {
			(*s)[i] = (*s)[len(*s)-1]
			*s = (*s)[:len(*s)-1]
		}
	}
}

func (s *Set) Check(key string) bool {
	for _, k := range *s {
		if k == key {
			return true
		}
	}
	return false
}
