package workflowcheck

import (
	"fmt"
	"reflect"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

// observationBatch exercises the batch observation entry point. It proves that
// a batch is an all-or-nothing commit, that repeated imports and retries have a
// deterministic result, that observations may cross a day boundary under strict
// time ordering, and that a successful batch leaves the same run detail,
// version, and final metrics as the same records written one at a time.
func observationBatch(api *client, timeSource *clock.Fixed) error {
	formula, err := ensureApprovedFormula(api, "Batch Formula")
	if err != nil {
		return err
	}

	// All three runs start before the UTC midnight boundary. The clock then
	// advances past midnight by four minutes, which keeps every observation at
	// or after the run start and inside the one-minute future tolerance.
	startedAt := timeSource.Now()
	batchVessel, err := createCheckVessel(api, "VSL-301", 300)
	if err != nil {
		return err
	}
	singleVessel, err := createCheckVessel(api, "VSL-302", 300)
	if err != nil {
		return err
	}
	atomicVessel, err := createCheckVessel(api, "VSL-303", 300)
	if err != nil {
		return err
	}
	batchRun, err := createStartedRun(api, formula.ID, batchVessel.ID, "RUN-BATCH")
	if err != nil {
		return err
	}
	singleRun, err := createStartedRun(api, formula.ID, singleVessel.ID, "RUN-SINGLE")
	if err != nil {
		return err
	}
	atomicRun, err := createStartedRun(api, formula.ID, atomicVessel.ID, "RUN-BATCH-ATOMIC")
	if err != nil {
		return err
	}

	timeSource.Advance(14 * time.Minute)
	if startedAt.Day() == timeSource.Now().Day() {
		return errInvalidWorkflow("batch check clock did not cross a day boundary")
	}

	points := []batchPoint{
		{observedAt: startedAt.Add(30 * time.Second), gravity: 1.060, temperature: 20.0, note: "before midnight"},
		{observedAt: startedAt.Add(8 * time.Minute), gravity: 1.040, temperature: 20.5, note: "across midnight"},
		{observedAt: startedAt.Add(13 * time.Minute).Add(30 * time.Second), gravity: 1.016, temperature: 19.8, note: "after midnight"},
	}
	if points[0].observedAt.Day() == points[2].observedAt.Day() {
		return errInvalidWorkflow("batch points do not cross the midnight boundary")
	}

	if err := appendBatch(api, batchRun.Run.ID, points, ptrInt64(batchRun.Run.Version)); err != nil {
		return err
	}
	singleVersion := singleRun.Run.Version
	for index, point := range points {
		response, err := api.call("POST", "/v1/runs/"+singleRun.Run.ID+"/observations",
			singlePayload(index+1, point), ptrInt64(singleVersion))
		if err != nil {
			return err
		}
		if err := requireStatus(response, 200, "append single observation for parity"); err != nil {
			return err
		}
		var updated runView
		if err := response.decode(&updated); err != nil {
			return err
		}
		singleVersion = updated.Version
	}

	batched, err := readRun(api, batchRun.Run.ID)
	if err != nil {
		return err
	}
	singled, err := readRun(api, singleRun.Run.ID)
	if err != nil {
		return err
	}
	if batched.Version != singled.Version {
		return errInvalidWorkflow(fmt.Sprintf(
			"batch version %d does not match single-write version %d",
			batched.Version, singled.Version))
	}
	if err := requireSameObservations(batched, singled); err != nil {
		return err
	}

	if err := completeRun(api, &batched); err != nil {
		return err
	}
	if err := completeRun(api, &singled); err != nil {
		return err
	}
	if !reflect.DeepEqual(batched.Metrics, singled.Metrics) {
		return errInvalidWorkflow(fmt.Sprintf(
			"batch metrics %+v do not match single-write metrics %+v",
			batched.Metrics, singled.Metrics))
	}
	if batched.Metrics == nil || batched.Metrics.ObservationCount != 3 {
		return errInvalidWorkflow("batch completion did not use all three observations")
	}

	// A completed run rejects further batches without touching persisted state.
	closed, err := api.call("POST", "/v1/runs/"+batched.ID+"/observations/batch",
		batchPayload([]batchPoint{{observedAt: points[2].observedAt.Add(time.Second), gravity: 1.010, temperature: 20}}),
		ptrInt64(batched.Version))
	if err != nil {
		return err
	}
	if err := requireCode(closed, 409, string(domain.CodeRunState),
		"batch observations on a completed run"); err != nil {
		return err
	}

	// Advance once more so the atomicity scenarios use fresh observed times
	// that stay within the future tolerance; the run start is far in the past.
	timeSource.Advance(4 * time.Minute)
	return atomicityCheck(api, atomicRun.Run, timeSource.Now())
}

// atomicityCheck verifies that one bad record rejects the whole batch, that
// retries after a partial failure have a deterministic all-or-nothing result,
// and that importing the same file twice is rejected rather than duplicated.
func atomicityCheck(api *client, run runView, base time.Time) error {
	id := run.ID
	baseVersion := run.Version

	last := base
	after := func(offset int, gravity float64, temperature float64) batchPoint {
		return batchPoint{
			observedAt:  last.Add(time.Duration(offset) * time.Second),
			gravity:     gravity,
			temperature: temperature,
		}
	}
	// First record valid, third record out of gravity range: nothing lands.
	badGravity := []batchPoint{after(1, 1.010, 20), after(2, 1.008, 20), after(3, 1.5, 20)}
	if err := requireBatchRejected(api, id, badGravity, baseVersion,
		400, string(domain.CodeInvalidInput), "reject batch with out-of-range gravity"); err != nil {
		return err
	}
	// Third record out of temperature range: nothing lands.
	badTemperature := []batchPoint{after(1, 1.010, 20), after(2, 1.008, 20), after(3, 1.006, 70)}
	if err := requireBatchRejected(api, id, badTemperature, baseVersion,
		400, string(domain.CodeInvalidInput), "reject batch with out-of-range temperature"); err != nil {
		return err
	}
	// Second record repeats the first observed time: nothing lands.
	nonMonotonic := []batchPoint{after(1, 1.010, 20), after(1, 1.008, 20), after(3, 1.006, 20)}
	if err := requireBatchRejected(api, id, nonMonotonic, baseVersion,
		409, string(domain.CodeObservationConflict), "reject batch with equal times"); err != nil {
		return err
	}
	// Second record observes the times out of file order: nothing lands.
	outOfOrder := []batchPoint{after(1, 1.010, 20), after(3, 1.008, 20), after(2, 1.006, 20)}
	if err := requireBatchRejected(api, id, outOfOrder, baseVersion,
		409, string(domain.CodeObservationConflict), "reject batch with times out of file order"); err != nil {
		return err
	}
	// Third record lets gravity rise more than 0.010: nothing lands.
	risingGravity := []batchPoint{after(1, 1.010, 20), after(2, 1.008, 20), after(3, 1.030, 20)}
	if err := requireBatchRejected(api, id, risingGravity, baseVersion,
		409, string(domain.CodeObservationConflict), "reject batch with rising gravity"); err != nil {
		return err
	}
	// Sequence gap inside the file: nothing lands.
	gap := batchPayload([]batchPoint{after(1, 1.010, 20), after(2, 1.008, 20), after(3, 1.006, 20)})
	gap["observations"].([]map[string]any)[2]["sequence"] = 4
	gapResponse, err := api.call("POST", "/v1/runs/"+id+"/observations/batch",
		gap, ptrInt64(baseVersion))
	if err != nil {
		return err
	}
	if err := requireCode(gapResponse, 409, string(domain.CodeObservationConflict),
		"reject batch with a sequence gap"); err != nil {
		return err
	}
	// The batch starts at sequence 2 while the run still expects 1: nothing lands.
	wrongStart := batchPayload([]batchPoint{after(1, 1.010, 20), after(2, 1.008, 20)})
	wrongStart["observations"].([]map[string]any)[0]["sequence"] = 2
	wrongStart["observations"].([]map[string]any)[1]["sequence"] = 3
	wrongStartResponse, err := api.call("POST", "/v1/runs/"+id+"/observations/batch",
		wrongStart, ptrInt64(baseVersion))
	if err != nil {
		return err
	}
	if err := requireCode(wrongStartResponse, 409, string(domain.CodeObservationConflict),
		"reject batch that does not resume at the next sequence"); err != nil {
		return err
	}
	// Stale aggregate version: nothing lands.
	stale := []batchPoint{after(1, 1.010, 20), after(2, 1.008, 20), after(3, 1.006, 20)}
	staleResponse, err := api.call("POST", "/v1/runs/"+id+"/observations/batch",
		batchPayload(stale), ptrInt64(baseVersion-1))
	if err != nil {
		return err
	}
	if err := requireCode(staleResponse, 412, string(domain.CodeRevisionConflict),
		"reject batch with a stale version"); err != nil {
		return err
	}
	if unchanged, err := readRun(api, id); err != nil {
		return err
	} else if unchanged.Version != baseVersion {
		return errInvalidWorkflow("stale-version batch advanced the run version")
	}
	// Empty batch: nothing lands.
	emptyResponse, err := api.call("POST", "/v1/runs/"+id+"/observations/batch",
		map[string]any{"observations": []map[string]any{}}, ptrInt64(baseVersion))
	if err != nil {
		return err
	}
	if err := requireCode(emptyResponse, 400, string(domain.CodeInvalidInput),
		"reject empty observation batch"); err != nil {
		return err
	}

	// After every rejected attempt, retrying with the corrected file must
	// commit exactly once and leave exactly the file's records behind.
	retry := []batchPoint{after(1, 1.010, 20), after(2, 1.008, 20), after(3, 1.006, 20)}
	if err := appendBatch(api, id, retry, ptrInt64(baseVersion)); err != nil {
		return err
	}
	committed, err := readRun(api, id)
	if err != nil {
		return err
	}
	if committed.Version != baseVersion+int64(len(retry)) {
		return errInvalidWorkflow(fmt.Sprintf(
			"retrying a failed batch advanced version to %d, want %d",
			committed.Version, baseVersion+int64(len(retry))))
	}
	if len(committed.Observations) != len(retry) {
		return errInvalidWorkflow("failed batches left partial observations behind")
	}
	// Importing the same file again is deterministically rejected: sequences
	// restart at 1 while the run now expects 4, so no duplicate is written.
	duplicateResponse, err := api.call("POST", "/v1/runs/"+id+"/observations/batch",
		batchPayload(retry), ptrInt64(committed.Version))
	if err != nil {
		return err
	}
	if err := requireCode(duplicateResponse, 409, string(domain.CodeObservationConflict),
		"reimport the same observation file"); err != nil {
		return err
	}
	reRead, err := readRun(api, id)
	if err != nil {
		return err
	}
	if reRead.Version != committed.Version || len(reRead.Observations) != len(retry) {
		return errInvalidWorkflow("duplicate import changed the run")
	}
	return nil
}

type batchPoint struct {
	observedAt  time.Time
	gravity     float64
	temperature float64
	note        string
}

func singlePayload(sequence int, point batchPoint) map[string]any {
	return map[string]any{
		"sequence":      sequence,
		"gravity":       point.gravity,
		"temperature_c": point.temperature,
		"observed_at":   point.observedAt,
		"note":          point.note,
	}
}

func batchPayload(points []batchPoint) map[string]any {
	records := make([]map[string]any, 0, len(points))
	for index, point := range points {
		payload := singlePayload(index+1, point)
		records = append(records, payload)
	}
	return map[string]any{"observations": records}
}

func readRun(api *client, id string) (runView, error) {
	response, err := api.call("GET", "/v1/runs/"+id, nil, nil)
	if err != nil {
		return runView{}, err
	}
	if err := requireStatus(response, 200, "read run"); err != nil {
		return runView{}, err
	}
	var run runView
	if err := response.decode(&run); err != nil {
		return runView{}, err
	}
	return run, nil
}

func appendBatch(api *client, id string, points []batchPoint, expected *int64) error {
	response, err := api.call("POST", "/v1/runs/"+id+"/observations/batch",
		batchPayload(points), expected)
	if err != nil {
		return err
	}
	return requireStatus(response, 200, "append observation batch")
}

func requireBatchRejected(
	api *client,
	id string,
	points []batchPoint,
	expectedVersion int64,
	status int,
	code string,
	operation string,
) error {
	response, err := api.call("POST", "/v1/runs/"+id+"/observations/batch",
		batchPayload(points), ptrInt64(expectedVersion))
	if err != nil {
		return err
	}
	if err := requireCode(response, status, code, operation); err != nil {
		return err
	}
	readBack, err := readRun(api, id)
	if err != nil {
		return err
	}
	if readBack.Version != expectedVersion {
		return errInvalidWorkflow(operation + ": rejected batch advanced the run version")
	}
	return nil
}

func completeRun(api *client, run *runView) error {
	conditioningResponse, err := api.call("POST", "/v1/runs/"+run.ID+"/conditioning",
		nil, ptrInt64(run.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(conditioningResponse, 200, "begin conditioning"); err != nil {
		return err
	}
	if err := conditioningResponse.decode(run); err != nil {
		return err
	}
	completeResponse, err := api.call("POST", "/v1/runs/"+run.ID+"/complete",
		nil, ptrInt64(run.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(completeResponse, 200, "complete run"); err != nil {
		return err
	}
	var completed pairView
	if err := completeResponse.decode(&completed); err != nil {
		return err
	}
	*run = completed.Run
	return nil
}

func requireSameObservations(batch, single runView) error {
	if len(batch.Observations) != len(single.Observations) {
		return errInvalidWorkflow(fmt.Sprintf(
			"observation count differs: batch %d, single %d",
			len(batch.Observations), len(single.Observations)))
	}
	for index := range batch.Observations {
		left := batch.Observations[index]
		right := single.Observations[index]
		left.RunID, right.RunID = "", ""
		if !reflect.DeepEqual(left, right) {
			return errInvalidWorkflow(fmt.Sprintf(
				"observation %d differs: batch %+v, single %+v",
				index+1, left, right))
		}
	}
	return nil
}
