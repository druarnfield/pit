package trigger

import (
	"context"
	"fmt"

	"github.com/robfig/cron/v3"
)

// CronTrigger fires events on a cron schedule.
type CronTrigger struct {
	dagName  string
	schedule string
}

// NewCronTrigger creates a trigger that fires on the given cron schedule.
// Returns an error if the schedule expression is invalid.
func NewCronTrigger(dagName, schedule string) (*CronTrigger, error) {
	if _, err := cron.ParseStandard(schedule); err != nil {
		return nil, fmt.Errorf("invalid cron schedule %q: %w", schedule, err)
	}
	return &CronTrigger{dagName: dagName, schedule: schedule}, nil
}

// Name returns a human-readable identifier for this trigger.
func (ct *CronTrigger) Name() string {
	return fmt.Sprintf("cron(%s) → %s", ct.schedule, ct.dagName)
}

// Start begins the cron scheduler and sends events to the channel.
// Blocks until the context is cancelled.
func (ct *CronTrigger) Start(ctx context.Context, events chan<- Event) error {
	c := cron.New()

	_, err := c.AddFunc(ct.schedule, func() {
		select {
		case events <- Event{
			DAGName: ct.dagName,
			Source:  "cron",
		}:
		case <-ctx.Done():
		}
	})
	if err != nil {
		return fmt.Errorf("adding cron job: %w", err)
	}

	c.Start()
	<-ctx.Done()
	c.Stop()
	return nil
}
