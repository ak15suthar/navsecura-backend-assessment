export const EVENT_TYPES = [
  'PERSON_DETECTED', 'VEHICLE_DETECTED', 'DOOR_OPENED', 'CAMERA_OFFLINE', 'PANIC_REQUEST', 'SMOKE_DETECTED'
] as const;
export type EventType = typeof EVENT_TYPES[number];

export interface IncomingEvent {
  eventId: string;
  customerId: string;
  siteId: string;
  sourceId: string;
  eventType: EventType;
  observedAt: string;
  payload?: Record<string, unknown>;
}

export interface CorrelationInput {
  customerId: string;
  siteId: string;
  eventType: EventType;
  observedAt: Date;
}

export function caseSummary(eventType: EventType): string {
  switch (eventType) {
    case 'PANIC_REQUEST': return 'Panic request requires immediate operator attention';
    case 'SMOKE_DETECTED': return 'Possible smoke detected';
    case 'CAMERA_OFFLINE': return 'Security camera offline';
    case 'DOOR_OPENED': return 'Door opened';
    case 'VEHICLE_DETECTED': return 'Vehicle detected';
    case 'PERSON_DETECTED': return 'Person detected';
  }
}
