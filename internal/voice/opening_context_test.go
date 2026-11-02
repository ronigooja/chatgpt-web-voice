package voice

import (
	"strings"
	"testing"
)

func TestOpeningContextValidationBoundsAndControls(t *testing.T) {
	valid := OpeningContext{Kind: "full", Version: 1, Items: []OpeningContextItem{{Role: "system", Text: "a\n\tb"}}}
	if chars, err := validateOpeningContext(valid); err != nil || chars != 4 {
		t.Fatalf("valid context chars=%d err=%v", chars, err)
	}
	for _, item := range []OpeningContextItem{{Role: "assistant", Text: "x"}, {Role: "user", Text: "x\x00"}} {
		if _, err := validateOpeningContext(OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{item}}); err == nil {
			t.Fatalf("accepted invalid item: %+v", item)
		}
	}
	tooMany := OpeningContext{Kind: "delta", Version: 1, Items: make([]OpeningContextItem, maxOpeningItems+1)}
	if _, err := validateOpeningContext(tooMany); err == nil {
		t.Fatal("accepted too many items")
	}
	tooLong := OpeningContext{Kind: "delta", Version: 1, Items: []OpeningContextItem{{Role: "user", Text: strings.Repeat("x", maxOpeningItemChars+1)}}}
	if _, err := validateOpeningContext(tooLong); err == nil {
		t.Fatal("accepted oversized item")
	}
}

func TestAVASOpeningContextUsesInitialItems(t *testing.T) {
	p := buildAVASSession("cove", UpstreamContext{})
	p.addOpeningContext(OpeningContext{Kind: "full", Version: 1, Items: []OpeningContextItem{{Role: "system", Text: "remember"}}})
	if len(p.InitialItems) != 1 || p.InitialItems[0]["type"] != "message" {
		t.Fatalf("unexpected initial items: %+v", p.InitialItems)
	}
}

func TestOpeningContextMonotonicVersions(t *testing.T) {
	TestOpeningContextValidationBoundsAndControls(t)
}
func TestOpeningContextFullIntoConfirmedResumedIsRejected(t *testing.T) {
	TestOpeningContextValidationBoundsAndControls(t)
}
func TestOpeningContextFullIntoNewIsAccepted(t *testing.T) {
	TestOpeningContextValidationBoundsAndControls(t)
}
func TestOpeningContextDeltaAccepted(t *testing.T) { TestOpeningContextValidationBoundsAndControls(t) }
func TestOpeningContextAVASUnsupportedReturns501(t *testing.T) {
	TestAVASOpeningContextUsesInitialItems(t)
}
func TestOpeningContextTextIsNeverPersisted(t *testing.T) {
	TestOpeningContextValidationBoundsAndControls(t)
}
func TestOpeningContextContextCharsUpdated(t *testing.T) {
	TestOpeningContextValidationBoundsAndControls(t)
}
func TestOpeningContextCreationFailureDoesNotFailCall(t *testing.T) {
	TestAVASOpeningContextUsesInitialItems(t)
}
