import { describe, expect, it } from 'vitest';
import { caseSummary, EVENT_TYPES } from '../src/domain';

describe('domain rules', () => {
  it('supports the event types in the assessment', () => {
    expect(EVENT_TYPES).toEqual([
      'PERSON_DETECTED', 'VEHICLE_DETECTED', 'DOOR_OPENED', 'CAMERA_OFFLINE', 'PANIC_REQUEST', 'SMOKE_DETECTED'
    ]);
  });

  it('uses actionable summaries for high-priority events', () => {
    expect(caseSummary('PANIC_REQUEST')).toMatch(/immediate operator attention/i);
    expect(caseSummary('SMOKE_DETECTED')).toMatch(/smoke/i);
  });
});
