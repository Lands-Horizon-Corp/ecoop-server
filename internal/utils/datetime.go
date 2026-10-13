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

// ParseDateTime reads a date and time in any known layout. A value without a
// zone is read as UTC.
func ParseDateTime(s string) (t time.Time, ok bool) {
	return ParseDateTimeIn(s, time.UTC)
}

// ParseDateTimeIn is ParseDateTime for values without a zone: they are read in
// loc. A zone in the value itself always wins.
func ParseDateTimeIn(s string, loc *time.Location) (t time.Time, ok bool) {
	for _, layout := range dateTimeLayouts {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func ParseTimeOfDay(s string) (t time.Time, ok bool) {
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
