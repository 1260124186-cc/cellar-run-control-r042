package domain

import (
	"errors"
	"fmt"
	"time"
)

type RunState string

const (
	RunPlanned      RunState = "planned"
	RunReserved     RunState = "reserved"
	RunFermenting   RunState = "fermenting"
	RunConditioning RunState = "conditioning"
	RunCompleted    RunState = "completed"
	RunAborted      RunState = "aborted"
)

type FermentationRun struct {
	ID             string               `json:"id"`
	Code           string               `json:"code"`
	FormulaID      string               `json:"formula_id"`
	VesselID       *string              `json:"vessel_id,omitempty"`
	PlannedVolumeL float64              `json:"planned_volume_liters"`
	Notes          string               `json:"notes,omitempty"`
	State          RunState             `json:"state"`
	StartedAt      *time.Time           `json:"started_at,omitempty"`
	EndedAt        *time.Time           `json:"ended_at,omitempty"`
	Observations   []GravityObservation `json:"observations"`
	Metrics        *RunMetrics          `json:"metrics,omitempty"`
	Version        int64                `json:"version"`
	CreatedAt      time.Time            `json:"created_at"`
	UpdatedAt      time.Time            `json:"updated_at"`
}

func (r FermentationRun) Clone() FermentationRun {
	cloned := r
	cloned.Observations = append([]GravityObservation(nil), r.Observations...)
	if r.VesselID != nil {
		value := *r.VesselID
		cloned.VesselID = &value
	}
	if r.StartedAt != nil {
		value := *r.StartedAt
		cloned.StartedAt = &value
	}
	if r.EndedAt != nil {
		value := *r.EndedAt
		cloned.EndedAt = &value
	}
	if r.Metrics != nil {
		value := *r.Metrics
		cloned.Metrics = &value
	}
	return cloned
}

func (r FermentationRun) Validate() error {
	if err := ValidateRunCode(r.Code, "code"); err != nil {
		return err
	}
	if r.PlannedVolumeL <= 0 {
		return FieldError(CodeInvalidInput, "planned_volume_liters",
			"must be greater than zero")
	}
	if len([]rune(r.Notes)) > 1000 {
		return FieldError(CodeInvalidInput, "notes",
			"must not exceed 1000 characters")
	}
	return nil
}

func (r *FermentationRun) ReserveVessel(vesselID string, now time.Time) error {
	if r.State != RunPlanned {
		return NewError(CodeRunState, "only a planned run can reserve a vessel")
	}
	value := vesselID
	r.VesselID = &value
	r.State = RunReserved
	r.UpdatedAt = now.UTC()
	r.Version++
	return nil
}

func (r *FermentationRun) StartFermentation(now time.Time) error {
	if r.State != RunReserved || r.VesselID == nil {
		return NewError(CodeRunState, "only a reserved run can start fermentation")
	}
	stamp := now.UTC()
	r.State = RunFermenting
	r.StartedAt = &stamp
	r.UpdatedAt = stamp
	r.Version++
	return nil
}

func (r *FermentationRun) AddObservation(observation GravityObservation) error {
	return r.AddObservations([]GravityObservation{observation})
}

// AddObservations appends a batch of observations atomically. Every record is
// validated against the resulting series before the run is mutated, so a single
// invalid record rejects the whole batch without leaving a partial series.
// Applying the records one at a time with the same shared receipt time is
// exactly equivalent, which keeps batch and single-record writes on the same
// legal data identical.
func (r *FermentationRun) AddObservations(observations []GravityObservation) error {
	if r.State != RunFermenting || r.StartedAt == nil {
		return NewError(CodeRunState, "observations require a fermenting run")
	}
	if len(observations) == 0 {
		return FieldError(CodeInvalidInput, "observations",
			"at least one observation is required")
	}
	if len(r.Observations)+len(observations) > 500 {
		return NewError(CodeObservationConflict,
			"run cannot contain more than 500 observations")
	}

	candidates := make([]GravityObservation, len(r.Observations), len(r.Observations)+len(observations))
	copy(candidates, r.Observations)
	var previous *GravityObservation
	if len(candidates) > 0 {
		value := candidates[len(candidates)-1]
		previous = &value
	}
	receivedAt := observations[0].ReceivedAt.UTC()
	for index, observation := range observations {
		if err := ValidateObservation(*r.StartedAt, previous, observation, receivedAt); err != nil {
			return batchObservationError(index, err)
		}
		expected := len(candidates) + 1
		if observation.Sequence != expected {
			return batchObservationError(index, NewError(CodeObservationConflict,
				"observation sequence must be contiguous and begin at 1"))
		}
		observation.RunID = r.ID
		candidates = append(candidates, observation)
		value := candidates[len(candidates)-1]
		previous = &value
	}

	r.Observations = candidates
	r.UpdatedAt = receivedAt
	r.Version += int64(len(observations))
	return nil
}

// batchObservationError prefixes a record-level error with its zero-based
// position in the submitted file so failed batches can be corrected.
func batchObservationError(index int, err error) error {
	var domainErr *Error
	if !errors.As(err, &domainErr) {
		return err
	}
	copy := *domainErr
	copy.Message = fmt.Sprintf("observation %d: %s", index+1, copy.Message)
	return &copy
}

func (r *FermentationRun) BeginConditioning(now time.Time) error {
	if r.State != RunFermenting {
		return NewError(CodeRunState, "only a fermenting run can enter conditioning")
	}
	if len(r.Observations) < 2 {
		return NewError(CodeRunState,
			"conditioning requires at least two observations")
	}
	r.State = RunConditioning
	r.UpdatedAt = now.UTC()
	r.Version++
	return nil
}

func (r *FermentationRun) Complete(originalGravity float64, now time.Time) error {
	if r.State != RunConditioning {
		return NewError(CodeRunState, "only a conditioning run can be completed")
	}
	if len(r.Observations) < 3 {
		return NewError(CodeRunState,
			"completion requires at least three observations")
	}
	stamp := now.UTC()
	final := r.Observations[len(r.Observations)-1]
	metrics := CalculateMetrics(originalGravity, final.Gravity, len(r.Observations))
	r.Metrics = &metrics
	r.State = RunCompleted
	r.EndedAt = &stamp
	r.UpdatedAt = stamp
	r.Version++
	return nil
}

func (r *FermentationRun) Abort(now time.Time) error {
	if r.State == RunCompleted || r.State == RunAborted {
		return NewError(CodeRunState, "completed or aborted runs cannot be aborted")
	}
	stamp := now.UTC()
	r.State = RunAborted
	r.EndedAt = &stamp
	r.UpdatedAt = stamp
	r.Version++
	return nil
}

func (r FermentationRun) HadStarted() bool {
	return r.StartedAt != nil
}
