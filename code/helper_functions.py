def log_str_format(s, max_length = 50, short_append_chars = '...'):
	"""
	Cuts a string down to max_length and appends short_append_chars if string was originally larger
	"""
	if len(s) > max_length:
		s = s[:max_length]
		s = s + short_append_chars

	return s