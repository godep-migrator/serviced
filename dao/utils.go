package dao

import (
	"fmt"
	"os"
)

var urandomFilename = "/dev/urandom"

// Generate a new UUID
func NewUuid() (string, error) {
	f, err := os.Open(urandomFilename)
	if err != nil {
		return "", err
	}
	b := make([]byte, 16)
	defer f.Close()
	f.Read(b)
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid, err
}

type StringSet struct {
	set map[string]struct{}
}

func NewStringSet() *StringSet {
	return &StringSet{map[string]struct{}{}}
}

func (s *StringSet) Add(strs ...string) {
	for _, str := range strs {
		s.set[str] = struct{}{}
	}
}

func (s *StringSet) Values() []string {
	result := make([]string, len(s.set))
	for k := range s.set {
		result = append(result, k)
	}
	return result
}

func DockerImages(sds ...ServiceDefinition) []string {
	images := NewStringSet()
	for _, sd := range sds {
		images.Add(DockerImages(sd.Services...)...)
		if sd.ImageId != "" {
			images.Add(sd.ImageId)
		}
	}
	return images.Values()
}
