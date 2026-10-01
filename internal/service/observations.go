package service

import "github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"

func (s *Service) AppendObservation(
	id string,
	input AppendObservationInput,
) (domain.FermentationRun, error) {
	return s.appendObservations(id, input.ExpectedVersion,
		[]domain.GravityObservation{s.toObservation(input.Observation)})
}

// AppendObservations commits a file's worth of observations in one revision.
// The whole batch is validated before anything is persisted, so any gravity,
// temperature, time-ordering, or sequence violation rejects the entire batch.
func (s *Service) AppendObservations(
	id string,
	input AppendObservationsInput,
) (domain.FermentationRun, error) {
	observations := make([]domain.GravityObservation, 0, len(input.Observations))
	for _, item := range input.Observations {
		observations = append(observations, s.toObservation(item))
	}
	return s.appendObservations(id, input.ExpectedVersion, observations)
}

func (s *Service) toObservation(input ObservationInput) domain.GravityObservation {
	return domain.GravityObservation{
		Sequence:     input.Sequence,
		Gravity:      input.Gravity,
		TemperatureC: input.TemperatureC,
		ObservedAt:   input.ObservedAt.UTC(),
		Note:         input.Note,
		ReceivedAt:   s.clock.Now(),
	}
}

func (s *Service) appendObservations(
	id string,
	expectedVersion *int64,
	observations []domain.GravityObservation,
) (domain.FermentationRun, error) {
	var updated domain.FermentationRun
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, expectedVersion); err != nil {
			return err
		}
		// Every record shares one receipt time, so a batch and the same legal
		// records written one by one within the same instant stay identical.
		receivedAt := s.clock.Now()
		for index := range observations {
			observations[index].ReceivedAt = receivedAt
		}
		if err := run.AddObservations(observations); err != nil {
			return err
		}
		snapshot.Runs[id] = run.Clone()
		updated = run.Clone()
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, err
	}
	return updated, nil
}

func (s *Service) BeginConditioning(
	id string,
	input VersionInput,
) (domain.FermentationRun, error) {
	var updated domain.FermentationRun
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, input.ExpectedVersion); err != nil {
			return err
		}
		if err := run.BeginConditioning(s.clock.Now()); err != nil {
			return err
		}
		snapshot.Runs[id] = run.Clone()
		updated = run.Clone()
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, err
	}
	return updated, nil
}

func (s *Service) CompleteRun(
	id string,
	input VersionInput,
) (domain.FermentationRun, domain.Vessel, error) {
	var updatedRun domain.FermentationRun
	var updatedVessel domain.Vessel
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, input.ExpectedVersion); err != nil {
			return err
		}
		formula, ok := snapshot.Formulas[run.FormulaID]
		if !ok {
			return domain.NewError(domain.CodeFormulaNotFound, "attached formula was not found")
		}
		if err := run.Complete(formula.TargetOriginalGravity, s.clock.Now()); err != nil {
			return err
		}
		if run.VesselID == nil {
			return domain.NewError(domain.CodeRunState, "run has no vessel")
		}
		vessel, ok := snapshot.Vessels[*run.VesselID]
		if !ok {
			return domain.NewError(domain.CodeVesselNotFound, "vessel was not found")
		}
		if err := vessel.StartCleaning(run.ID, s.clock.Now()); err != nil {
			return err
		}
		snapshot.Runs[id] = run.Clone()
		snapshot.Vessels[vessel.ID] = vessel
		updatedRun = run.Clone()
		updatedVessel = vessel
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, domain.Vessel{}, err
	}
	return updatedRun, updatedVessel, nil
}
