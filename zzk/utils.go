package zzk

import (
	"fmt"
	"os"
)

const urandomfilename = "/dev/urandom"

func newuuid() (string, error) {
	f, err := os.Open(urandomfilename)
	if err != nil {
		return "", err
	}
	b := make([]byte, 16)
	defer f.Close()
	f.Read(b)
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])

	return uuid, nil
}