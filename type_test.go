package Redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Type(t *testing.T) {
	assert := assert.New(t)

	var a HashItem
	a.Key = "aaaa"
	assert.Equal(a.Rtype(), "hash")

	var b SetItem
	b.Key = "aaaa"
	assert.Equal(b.Rtype(), "set")

	var c StringItem
	c.Key = "aaaa"
	assert.Equal(c.Rtype(), "string")

	var d Item
	d.Key = "aaaa"
	d.Rtype = "ccc"

	assert.Equal(d.Rtype, "ccc")
	assert.Equal(d.Key, "aaaa")

}
