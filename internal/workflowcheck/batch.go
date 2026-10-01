package workflowcheck

import (
	"fmt"
	"reflect"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

var batchCheckBase = time.Date(2026, time.January, 1, 23, 59, 40, 0, time.UTC)

type observationDetail struct {
	Sequence     int
	Gravity      float64
	TemperatureC float64
	ObservedAt   time.Time
	Note         string
	ReceivedAt   time.Time
}

func observationBatch(api *client) error {
	formula, err := ensureApprovedFormula(api, "Batch Formula")
	if err != nil {
		return err
	}

	singleVessel, err := createCheckVessel(api, "VSL-301", 300)
	if err != nil {
		return err
	}
	singleStarted, err := createStartedRun(api, formula.ID, singleVessel.ID, "RUN-BATCH-SINGLE")
	if err != nil {
		return err
	}
	points := batchObservationPoints()
	singleRun, err := appendObservationSingles(api, singleStarted.Run.ID, singleStarted.Run.Version, points)
	if err != nil {
		return err
	}

	batchVessel, err := createCheckVessel(api, "VSL-302", 300)
	if err != nil {
		return err
	}
	batchStarted, err := createStartedRun(api, formula.ID, batchVessel.ID, "RUN-BATCH-ATOMIC")
	if err != nil {
		return err
	}
	batchResponse, err := api.call("POST",
		"/v1/runs/"+batchStarted.Run.ID+"/observations/batch",
		map[string]any{"observations": points}, ptrInt64(batchStarted.Run.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(batchResponse, 200, "append observation batch"); err != nil {
		return err
	}
	var batchRun runView
	if err := batchResponse.decode(&batchRun); err != nil {
		return err
	}
	if batchRun.Version != singleRun.Version {
		return errInvalidWorkflow("batch version does not match single-write version")
	}
	if err := requireSameObservationDetails(singleRun, batchRun); err != nil {
		return err
	}

	if err := rejectStaleBatch(api, batchRun); err != nil {
		return err
	}
	if err := rejectBatchWithoutReordering(api, batchRun); err != nil {
		return err
	}
	if err := rejectBatchWithNonStrictTimes(api, batchRun); err != nil {
		return err
	}

	completedSingle, err := completeCheckRun(api, singleRun)
	if err != nil {
		return err
	}
	completedBatch, err := completeCheckRun(api, batchRun)
	if err != nil {
		return err
	}
	if completedBatch.Run.Version != completedSingle.Run.Version {
		return errInvalidWorkflow("completed batch version does not match single-write version")
	}
	if !reflect.DeepEqual(completedBatch.Run.Metrics, completedSingle.Run.Metrics) {
		return fmt.Errorf("batch metrics %#v do not match single-write metrics %#v",
			completedBatch.Run.Metrics, completedSingle.Run.Metrics)
	}
	if err := requireSameObservationDetails(completedSingle.Run, completedBatch.Run); err != nil {
		return err
	}

	if err := verifyGravityBatchRollbackAndRetry(api, formula.ID); err != nil {
		return err
	}
	if err := verifyTemperatureBatchRollbackAndVersion(api, formula.ID); err != nil {
		return err
	}
	return nil
}

func batchObservationPoints() []map[string]any {
	return []map[string]any{
		{
			"sequence":      1,
			"gravity":       1.050,
			"temperature_c": 20.0,
			"observed_at":   verifiedAt(batchCheckBase, 1),
			"note":          "batch observation 1",
		},
		{
			"sequence":      2,
			"gravity":       1.028,
			"temperature_c": 20.5,
			"observed_at":   verifiedAt(batchCheckBase, 2),
			"note":          "batch observation 2",
		},
		{
			"sequence":      3,
			"gravity":       1.014,
			"temperature_c": 19.8,
			"observed_at":   verifiedAt(batchCheckBase, 3),
			"note":          "batch observation 3",
		},
	}
}

func appendObservationSingles(
	api *client,
	runID string,
	version int64,
	points []map[string]any,
) (runView, error) {
	var run runView
	for _, point := range points {
		response, err := api.call("POST", "/v1/runs/"+runID+"/observations",
			point, ptrInt64(version))
		if err != nil {
			return runView{}, err
		}
		if err := requireStatus(response, 200, "append comparison observation"); err != nil {
			return runView{}, err
		}
		if err := response.decode(&run); err != nil {
			return runView{}, err
		}
		version = run.Version
	}
	return run, nil
}

func rejectStaleBatch(api *client, run runView) error {
	response, err := api.call("POST", "/v1/runs/"+run.ID+"/observations/batch",
		map[string]any{"observations": []map[string]any{
			{
				"sequence":      4,
				"gravity":       1.012,
				"temperature_c": 20,
				"observed_at":   batchCheckBase.Add(25 * time.Second),
			},
		}}, ptrInt64(run.Version-1))
	if err != nil {
		return err
	}
	if err := requireCode(response, 412, string(domain.CodeRevisionConflict),
		"reject stale observation batch"); err != nil {
		return err
	}
	return requireRunUnchanged(api, run)
}

func rejectBatchWithoutReordering(api *client, run runView) error {
	response, err := api.call("POST", "/v1/runs/"+run.ID+"/observations/batch",
		map[string]any{"observations": []map[string]any{
			{
				"sequence":      5,
				"gravity":       1.011,
				"temperature_c": 20,
				"observed_at":   batchCheckBase.Add(45 * time.Second),
			},
			{
				"sequence":      4,
				"gravity":       1.012,
				"temperature_c": 20,
				"observed_at":   batchCheckBase.Add(40 * time.Second),
			},
		}}, ptrInt64(run.Version))
	if err != nil {
		return err
	}
	if err := requireCode(response, 409, string(domain.CodeObservationConflict),
		"preserve batch file order for sequences"); err != nil {
		return err
	}
	return requireRunUnchanged(api, run)
}

func rejectBatchWithNonStrictTimes(api *client, run runView) error {
	response, err := api.call("POST", "/v1/runs/"+run.ID+"/observations/batch",
		map[string]any{"observations": []map[string]any{
			{
				"sequence":      4,
				"gravity":       1.012,
				"temperature_c": 20,
				"observed_at":   batchCheckBase.Add(40 * time.Second),
			},
			{
				"sequence":      5,
				"gravity":       1.011,
				"temperature_c": 20,
				"observed_at":   batchCheckBase.Add(40 * time.Second),
			},
		}}, ptrInt64(run.Version))
	if err != nil {
		return err
	}
	if err := requireCode(response, 409, string(domain.CodeObservationConflict),
		"reject non-strict observation times across midnight"); err != nil {
		return err
	}
	return requireRunUnchanged(api, run)
}

func verifyGravityBatchRollbackAndRetry(api *client, formulaID string) error {
	vessel, err := createCheckVessel(api, "VSL-303", 300)
	if err != nil {
		return err
	}
	started, err := createStartedRun(api, formulaID, vessel.ID, "RUN-BATCH-GRAVITY")
	if err != nil {
		return err
	}
	invalid := batchObservationPoints()
	invalid[2]["gravity"] = 1.4
	response, err := api.call("POST", "/v1/runs/"+started.Run.ID+"/observations/batch",
		map[string]any{"observations": invalid}, ptrInt64(started.Run.Version))
	if err != nil {
		return err
	}
	if err := requireCode(response, 400, string(domain.CodeInvalidInput),
		"reject batch with invalid gravity"); err != nil {
		return err
	}
	if err := requireRunUnchanged(api, started.Run); err != nil {
		return err
	}

	response, err = api.call("POST", "/v1/runs/"+started.Run.ID+"/observations/batch",
		map[string]any{"observations": batchObservationPoints()}, ptrInt64(started.Run.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(response, 200, "retry complete valid gravity batch"); err != nil {
		return err
	}
	var imported runView
	if err := response.decode(&imported); err != nil {
		return err
	}
	if imported.Version != started.Run.Version+3 || len(imported.Observations) != 3 {
		return errInvalidWorkflow("corrected batch did not import exactly once")
	}

	duplicate, err := api.call("POST", "/v1/runs/"+started.Run.ID+"/observations/batch",
		map[string]any{"observations": batchObservationPoints()}, ptrInt64(imported.Version))
	if err != nil {
		return err
	}
	if err := requireCode(duplicate, 409, string(domain.CodeObservationConflict),
		"reject duplicate imported batch"); err != nil {
		return err
	}
	return requireRunUnchanged(api, imported)
}

func verifyTemperatureBatchRollbackAndVersion(api *client, formulaID string) error {
	vessel, err := createCheckVessel(api, "VSL-304", 300)
	if err != nil {
		return err
	}
	started, err := createStartedRun(api, formulaID, vessel.ID, "RUN-BATCH-TEMPERATURE")
	if err != nil {
		return err
	}
	invalid := batchObservationPoints()
	invalid[1]["temperature_c"] = 100
	response, err := api.call("POST", "/v1/runs/"+started.Run.ID+"/observations/batch",
		map[string]any{"observations": invalid}, ptrInt64(started.Run.Version))
	if err != nil {
		return err
	}
	if err := requireCode(response, 400, string(domain.CodeInvalidInput),
		"reject batch with invalid temperature"); err != nil {
		return err
	}
	if err := requireRunUnchanged(api, started.Run); err != nil {
		return err
	}

	stale, err := api.call("POST", "/v1/runs/"+started.Run.ID+"/observations/batch",
		map[string]any{"observations": batchObservationPoints()}, ptrInt64(started.Run.Version-1))
	if err != nil {
		return err
	}
	if err := requireCode(stale, 412, string(domain.CodeRevisionConflict),
		"retry valid batch with stale version"); err != nil {
		return err
	}
	if err := requireRunUnchanged(api, started.Run); err != nil {
		return err
	}

	valid, err := api.call("POST", "/v1/runs/"+started.Run.ID+"/observations/batch",
		map[string]any{"observations": batchObservationPoints()}, ptrInt64(started.Run.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(valid, 200, "retry complete valid temperature batch"); err != nil {
		return err
	}
	var imported runView
	if err := valid.decode(&imported); err != nil {
		return err
	}
	if imported.Version != started.Run.Version+3 || len(imported.Observations) != 3 {
		return errInvalidWorkflow("temperature batch did not import atomically after retry")
	}
	return nil
}

func completeCheckRun(api *client, run runView) (pairView, error) {
	conditioningResponse, err := api.call("POST", "/v1/runs/"+run.ID+"/conditioning",
		nil, ptrInt64(run.Version))
	if err != nil {
		return pairView{}, err
	}
	if err := requireStatus(conditioningResponse, 200, "begin batch comparison conditioning"); err != nil {
		return pairView{}, err
	}
	var conditioning runView
	if err := conditioningResponse.decode(&conditioning); err != nil {
		return pairView{}, err
	}
	completeResponse, err := api.call("POST", "/v1/runs/"+run.ID+"/complete",
		nil, ptrInt64(conditioning.Version))
	if err != nil {
		return pairView{}, err
	}
	if err := requireStatus(completeResponse, 200, "complete batch comparison run"); err != nil {
		return pairView{}, err
	}
	var completed pairView
	if err := completeResponse.decode(&completed); err != nil {
		return pairView{}, err
	}
	return completed, nil
}

func requireRunUnchanged(api *client, expected runView) error {
	response, err := api.call("GET", "/v1/runs/"+expected.ID, nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(response, 200, "read run after rejected batch"); err != nil {
		return err
	}
	var actual runView
	if err := response.decode(&actual); err != nil {
		return err
	}
	if actual.Version != expected.Version || len(actual.Observations) != len(expected.Observations) {
		return errInvalidWorkflow("rejected observation batch changed persisted state")
	}
	return nil
}

func requireSameObservationDetails(expected, actual runView) error {
	if len(expected.Observations) != len(actual.Observations) {
		return errInvalidWorkflow("batch observation count does not match single writes")
	}
	for index := range expected.Observations {
		left := observationDetails(expected.Observations[index])
		right := observationDetails(actual.Observations[index])
		if !reflect.DeepEqual(left, right) {
			return fmt.Errorf("batch observation %d %#v does not match single write %#v",
				index+1, right, left)
		}
	}
	return nil
}

func observationDetails(value domain.GravityObservation) observationDetail {
	return observationDetail{
		Sequence:     value.Sequence,
		Gravity:      value.Gravity,
		TemperatureC: value.TemperatureC,
		ObservedAt:   value.ObservedAt,
		Note:         value.Note,
		ReceivedAt:   value.ReceivedAt,
	}
}
