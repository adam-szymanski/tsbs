package oxla

import "testing"

func TestRunDBCreator(t *testing.T) {
	c := dbCreator{}
	c.Init()
	if err := c.CreateDB("test"); err != nil {
		t.Fatal(err.Error())
	}
	if err := c.PostCreateDB("test"); err != nil {
		t.Fatal(err.Error())
	}
}
