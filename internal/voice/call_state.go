package voice

import "sync"

// callState holds in-memory transcript, forwarding, and conversation metadata
// beside the session binding and is dropped with it.
type callState struct {
	owner                 string
	mu                    sync.Mutex
	voice                 string
	voiceMode             string
	language              string
	accountID             int64
	stopOnce              sync.Once
	transcript            *transcriptTail
	forward               *transcriptForward
	forwardOnce           sync.Once
	owned                 *ownedCall
	conversation          string
	conversationReason    string
	conversationConfirmed *bool
	contextChars          int
}

func (c *callState) halt() {
	c.stopOnce.Do(func() { c.forward.Close() })
}

func (c *callState) startForward() {
	if c.forward != nil {
		c.forwardOnce.Do(func() { go c.forward.run() })
	}
}
