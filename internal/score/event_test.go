package score

import (
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

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	e := validEvent()
	data, err := e.Marshal()
	if err != nil {
		t.Fatalf("Marshal() = %v", err)
	}
	want := `{"event_id":"evt-1","leaderboard_id":"weekly","user_id":"user-1","points":100}`
	if string(data) != want {
		t.Fatalf("Marshal() = %s, want %s", data, want)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() = %v", err)
	}
	if got != e {
		t.Fatalf("round trip = %+v, want %+v", got, e)
	}
}

func TestUnmarshal_InvalidJSON(t *testing.T) {
	if _, err := Unmarshal([]byte("{bad")); err == nil {
		t.Fatal("Unmarshal() with invalid JSON = nil error, want error")
	}
}

func TestUnmarshal_PointsAsInt64(t *testing.T) {
	// points must deserialize into int64, not float64.
	data := []byte(`{"event_id":"e","leaderboard_id":"l","user_id":"u","points":9223372036854775807}`)
	e, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() = %v", err)
	}
	if e.Points != 9223372036854775807 {
		t.Fatalf("Points = %d, want MaxInt64", e.Points)
	}
}
