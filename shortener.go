package snowdrift

import (
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"

	"bitbucket.org/ww/goautoneg"
	"github.com/codegangsta/martini"
	"github.com/codegangsta/martini-contrib/binding"
	"github.com/codegangsta/martini-contrib/render"
	"github.com/speps/go-hashids"
)

type Config struct {
	Backend      Backend
	HashSalt     string
	URLPrefix    string
	RootRedirect string
	ReportErr    func(err error, req *http.Request)
}

func New(c *Config) *martini.Martini {
	r := martini.NewRouter()
	m := martini.New()
	m.Action(r.Handle)

	ctx := &context{
		Backend:   c.Backend,
		ShortHash: hashids.New(),
		LongHash:  hashids.New(),
		URLPrefix: c.URLPrefix,
		ReportErr: c.ReportErr,
	}
	if c.HashSalt == "" {
		c.HashSalt = "salt"
	}
	ctx.ShortHash.Salt = c.HashSalt
	ctx.LongHash.Salt = c.HashSalt
	ctx.LongHash.MinLength = 12
	m.Map(ctx)
	m.Use(render.Renderer())

	r.Get("/", func(r *http.Request, w http.ResponseWriter) {
		http.Redirect(w, r, c.RootRedirect, http.StatusFound)
	})
	r.Post("/", binding.Bind(link{}), createLink)
	r.Get("/:code", getLink)

	return m
}

var ErrNotFound = errors.New("snowdrift: not found")
var ErrURLExists = errors.New("snowdrift: url already exists")
var ErrCodeExists = errors.New("snowdrift: code already exists")

type Backend interface {
	Add(url, digest, code string) error
	GetCode(digest string) (string, error)
	GetURL(code string) (string, error)
	NextID() (int, error)
}

type link struct {
	LongURL  string `json:"long_url" riak:"long_url"`
	ShortURL string `json:"short_url" riak:"-"`
	Obscure  *bool  `json:"obscure,omitempty" riak:"-"`
}

type context struct {
	Backend
	ShortHash *hashids.HashID
	LongHash  *hashids.HashID
	URLPrefix string
	ReportErr func(err error, req *http.Request)
}

func urlDigest(url string) string {
	digest := sha512.Sum512([]byte(url))
	return hex.EncodeToString(digest[:32])
}

func createLink(c *context, link link, r render.Render, req *http.Request) {
	if len(link.LongURL) > 2000 {
		r.Error(400)
		return
	}
	u, err := url.Parse(link.LongURL)
	if err != nil || u.Scheme != "http" && u.Scheme != "https" {
		r.Error(400)
		return
	}

	digest := urlDigest(link.LongURL)

	code, err := c.GetCode(digest)
	if err == nil {
		link.ShortURL = c.URLPrefix + code
		link.Obscure = nil
		r.JSON(200, link)
		return
	}

	id, err := c.NextID()
	if err != nil {
		r.Error(500)
		if c.ReportErr != nil {
			c.ReportErr(err, req)
		}
		return
	}
	if link.Obscure != nil && *link.Obscure {
		code = c.LongHash.Encrypt([]int{id})
	} else {
		code = c.ShortHash.Encrypt([]int{id})
	}

	if err := c.Add(link.LongURL, digest, code); err != nil {
		r.Error(500)
		if c.ReportErr != nil {
			c.ReportErr(err, req)
		}
		return
	}
	link.Obscure = nil
	link.ShortURL = c.URLPrefix + code
	r.JSON(200, link)
}

func getLink(c *context, params martini.Params, r render.Render, req *http.Request, w http.ResponseWriter) {
	url, err := c.GetURL(params["code"])
	if err == ErrNotFound {
		http.NotFound(w, req)
		return
	}
	if err != nil {
		r.Error(500)
		if c.ReportErr != nil {
			c.ReportErr(err, req)
		}
		return
	}
	contentType := goautoneg.Negotiate(req.Header.Get("Accept"), []string{"text/html", "application/json"})
	if contentType == "application/json" {
		r.JSON(200, link{LongURL: url, ShortURL: c.URLPrefix + params["code"]})
		return
	}

	http.Redirect(w, req, url, http.StatusMovedPermanently)
}
