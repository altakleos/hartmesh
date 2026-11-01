"""Token-shaped text recognition without importing a transport or SDK."""

TOKEN_BODY = "[0-9]{6,12}:[A-Za-z0-9_-]{30,}"
# A search in what a person typed or said: a longer run of digits is not a bot id.
TOKEN_IN_TEXT = r"(?<![0-9])" + TOKEN_BODY
