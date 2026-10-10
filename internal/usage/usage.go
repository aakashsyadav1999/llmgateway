// Package usage observes provider usage without buffering or delaying delivery.
package usage

import (
	"bytes"
	"encoding/json"
)

const limit = 1 << 20

type Price struct {
	Input       float64  `json:"input_usd_per_million"`
	Output      float64  `json:"output_usd_per_million"`
	CachedInput *float64 `json:"cached_input_usd_per_million,omitempty"`
}

type Tokens struct{ Input, Output, CachedInput int64 }

func (p Price) Cost(t Tokens) float64 {
	cached := p.Input
	if p.CachedInput != nil {
		cached = *p.CachedInput
	}
	return (float64(t.Input-t.CachedInput)*p.Input + float64(t.CachedInput)*cached + float64(t.Output)*p.Output) / 1e6
}

type Observer struct {
	Stream                  bool
	buf, event              []byte
	overflow, eventOverflow bool
	Tokens                  Tokens
	Model                   string
	Found                   bool
}

func (o *Observer) Write(p []byte) (int, error) {
	if !o.Stream {
		if !o.overflow && len(o.buf)+len(p) <= limit {
			o.buf = append(o.buf, p...)
		} else {
			o.overflow = true
			o.buf = nil
		}
		return len(p), nil
	}
	for _, b := range p {
		if b == '\n' {
			o.line()
			o.buf = o.buf[:0]
			o.overflow = false
			continue
		}
		if !o.overflow {
			if len(o.buf) == limit {
				o.overflow = true
				o.buf = nil
			} else {
				o.buf = append(o.buf, b)
			}
		}
	}
	return len(p), nil
}

func (o *Observer) line() {
	if o.overflow {
		o.eventOverflow = true
		return
	}
	line := bytes.TrimSuffix(o.buf, []byte("\r"))
	if len(line) == 0 {
		if !o.eventOverflow {
			o.parse(o.event)
		}
		o.event = o.event[:0]
		o.eventOverflow = false
	} else if bytes.HasPrefix(line, []byte("data:")) && !o.eventOverflow {
		data := bytes.TrimPrefix(line[5:], []byte(" "))
		if len(o.event)+len(data)+1 > limit {
			o.eventOverflow = true
			o.event = nil
			return
		}
		o.event = append(o.event, data...)
		o.event = append(o.event, '\n')
	}
}

func (o *Observer) Finish() {
	if !o.Stream {
		if !o.overflow {
			o.parse(o.buf)
		}
		return
	}
	if len(o.buf) > 0 || o.overflow {
		o.line()
	}
	if !o.eventOverflow {
		o.parse(o.event)
	}
}

func (o *Observer) parse(b []byte) {
	var v struct {
		Model string `json:"model"`
		Usage *struct {
			Prompt     *int64 `json:"prompt_tokens"`
			Completion *int64 `json:"completion_tokens"`
			Input      *int64 `json:"input_tokens"`
			Output     *int64 `json:"output_tokens"`
			Details    struct {
				Cached int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
			InputDetails struct {
				Cached int64 `json:"cached_tokens"`
			} `json:"input_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(b, &v) != nil {
		return
	}
	if v.Model != "" {
		o.Model = v.Model
	}
	if v.Usage == nil {
		return
	}
	u := v.Usage
	input, output := u.Prompt, u.Completion
	cached := u.Details.Cached
	if input == nil {
		input = u.Input
		cached = u.InputDetails.Cached
	}
	if output == nil {
		output = u.Output
	}
	if input == nil {
		return
	}
	t := Tokens{Input: *input, CachedInput: cached}
	if output != nil {
		t.Output = *output
	}
	if t.Input < 0 || t.Output < 0 || cached < 0 || cached > t.Input {
		return
	}
	o.Tokens = t
	o.Found = true
}
