package score

import (
	"encoding/json"
	"testing"
)

func validEvent() Event {
	return Event{
		EventID:       "evt-1",
		LeaderboardID: "weekly",
		UserID:        "user-1",
		Points:        100,
	}
}

func TestValidate_Valid(t *testing.T) {
	if err := validEvent().Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidate_MissingFields(t *testing.T) {
	cases := map[string]func(*Event){
		"event_id":       func(e *Event) { e.EventID = "" },
		"leaderboard_id": func(e *Event) { e.LeaderboardID = "" },
		"user_id":        func(e *Event) { e.UserID = "" },
	}
	for name, mutate := range cases {
		e := validEvent()
		mutate(&e)
		if err := e.Validate(); err == nil {
			t.Fatalf("Validate() with missing %s = nil, want error", name)
		}
	}
}

func TestValidate_InvalidPoints(t *testing.T) {
	for _, points := range []int64{0, -1, -100} {
		e := validEvent()
		e.Points = points
		if err := e.Validate(); err == nil {
			t.Fatalf("Validate() with points=%d = nil, want error", points)
		}
	}
}

func TestKey(t *testing.T) {
	e := validEvent()
	if got := e.Key(); got != "weekly:user-1" {
		t.Fatalf("Key() = %q, want weekly:user-1", got)
	}
}

func TestEventJSON(t *testing.T) {
	e := validEvent()

	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("json.Marshal() = %v", err)
	}

	want := `{"event_id":"evt-1","leaderboard_id":"weekly","user_id":"user-1","points":100}`
	if string(data) != want {
		t.Fatalf("JSON = %s, want %s", data, want)
	}

	var got Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() = %v", err)
	}

	if got != e {
		t.Fatalf("round trip = %+v, want %+v", got, e)
	}
}
