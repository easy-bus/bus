package bus

import "time"

type HandlerOpt func(h *Handler)

func HandlerDelay(delay time.Duration) HandlerOpt {
	return func(h *Handler) { h.Delay = delay }
}
