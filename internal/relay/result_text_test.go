package relay

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestResultTextSynchronousActiveLeg(t *testing.T) {
	g := connect(t, newRelay(t, 2))
	sender := g.session.(interface{ SendResultText(string) (bool, error) })
	attempted, err := sender.SendResultText("receipt text")
	if err != nil || !attempted {
		t.Fatal(attempted, err)
	}
	select {
	case frame := <-g.upstream.events:
		var outer struct {
			Type string `json:"type"`
			Data string `json:"data"`
		}
		if json.Unmarshal([]byte(frame), &outer) != nil || outer.Type != "data_message" || !strings.Contains(outer.Data, "receipt text") {
			t.Fatal(frame)
		}
	case <-time.After(testWait):
		t.Fatal("actual transport did not receive frame")
	}
}

func TestResultTextBeforeWrite(t *testing.T) {
	s := &pionSession{}
	attempted, err := s.SendResultText("hello")
	if attempted || err == nil {
		t.Fatal(attempted, err)
	}
}
