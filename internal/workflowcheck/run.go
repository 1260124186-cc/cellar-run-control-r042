package workflowcheck

import (
	"fmt"
	"net/http/httptest"
	"os"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/httpapi"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

func Run(name string) error {
	switch name {
	case "observation-batch":
		// Ten minutes before UTC midnight: runs start before the boundary, then
		// the clock advances past midnight so the batch crosses the day.
		fixed := clock.NewFixed(midnightUTC(time.Now().UTC()).Add(-10 * time.Minute))
		return runWithClock(name, fixed)
	default:
		return runWithClock(name, clock.System{})
	}
}

func runWithClock(name string, timeSource clock.Clock) error {
	dataDir, err := os.MkdirTemp("", "cellar-run-check-*")
	if err != nil {
		return fmt.Errorf("create check directory: %w", err)
	}
	defer os.RemoveAll(dataDir)

	store, err := storage.Open(dataDir)
	if err != nil {
		return err
	}
	app := service.New(store, timeSource)
	server := httptest.NewServer(httpapi.NewHandler(app))
	defer server.Close()
	api := newClient(server.URL)

	switch name {
	case "formula-approval":
		if err := formulaApproval(api); err != nil {
			return err
		}
	case "vessel-run-lifecycle":
		if err := vesselRunLifecycle(api); err != nil {
			return err
		}
	case "observations-completion":
		if err := observationsCompletion(api); err != nil {
			return err
		}
	case "observation-batch":
		fixed, ok := timeSource.(*clock.Fixed)
		if !ok {
			return fmt.Errorf("observation-batch check requires a fixed clock")
		}
		if err := observationBatch(api, fixed); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown workflow check %q", name)
	}
	fmt.Printf("workflow check passed: %s\n", name)
	return nil
}

func midnightUTC(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}
