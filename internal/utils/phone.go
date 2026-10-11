package utils

import "regexp"

var e164 = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)

// IsValidPhoneNumber reports whether number is in E.164 form (+ then 8 to 15
// digits, no leading zero), the format Twilio requires.
func IsValidPhoneNumber(number string) bool {
	return e164.MatchString(number)
}
