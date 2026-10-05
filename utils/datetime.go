package utils

import "time"

var dateTimeLayouts = []string{
	time.RFC3339,
	time.RFC3339Nano,
	time.RFC1123,
	time.RFC1123Z,
	time.RFC822,
	time.RFC822Z,
	time.RFC850,
	time.ANSIC,
	time.UnixDate,
	time.RubyDate,
	"2006-01-02T15:04:05Z",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999999",
	"01/02/2006 15:04:05",
	"02/01/2006 15:04:05",
	"2006-01-02T15:04:05-07:00",
	"Mon Jan 02 2006 15:04:05 -0700",
	"2006/01/02 15:04:05",
	"2006/01/02T15:04:05",
	"2006/01/02 15:04:05Z07:00",
	"2006/01/02 15:04:05 MST",
	"2006-01-02",
	"2006/01/02",
	"01/02/2006",
	"02/01/2006",
}

// timeLayouts lists every time-of-day-only layout a client-supplied
// time-typed filter value may arrive in.
var timeLayouts = []string{
	time.Kitchen,
	"15:04:05",
	"15:04",
	"15:04:05.999999999",
	"3:04:05 PM",
	"3:04 PM",
	"15:04:05Z07:00",
	"15:04:05 MST",
	"3:04:05 PM MST",
	"15:04:05-07:00",
}

// ParseDateTime tries every known date/time layout against s in order,
// returning the first successful parse. ok is false if none match.
func ParseDateTime(s string) (t time.Time, ok bool) {
	for _, layout := range dateTimeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// ParseTimeOfDay tries every known time-of-day layout against s in order,
// returning the first successful parse. ok is false if none match.
func ParseTimeOfDay(s string) (t time.Time, ok bool) {
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
