package p2p

// truncateStr safely truncates a string to maxLen characters
// If the string is shorter than maxLen, returns the full string
func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}
