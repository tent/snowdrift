package main

import (
	"github.com/codegangsta/martini"
	"github.com/cupcake/snowdrift"
)

func main() {
	m := snowdrift.New(&snowdrift.Config{
		Backend:      snowdrift.NewMemoryBackend(),
		URLPrefix:    "http://localhost:3000/",
		RootRedirect: "http://google.com",
	})
	m.Use(martini.Logger())
	m.Run()
}
