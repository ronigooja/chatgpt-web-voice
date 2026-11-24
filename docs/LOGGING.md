# Logging

Gateway logs carry metadata only, never conversation content, request or response
bodies, credentials, or tokens at any level. Conversation content is available
in the in-memory `/viewer` event viewer. Event-shape sampling records types,
allowlisted structural keys, lengths and counts, not payload values. Client
voice access logs omit caller-supplied IDs, paths and user agents.
