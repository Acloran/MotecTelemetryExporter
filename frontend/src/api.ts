import type {
  ChannelDef,
  ChannelChartDefinition,
  DayDetail,
  DriveDay,
  ExportResponse,
  GpsResponse,
  LiveLapExportResponse,
  SegmentResponse,
  SeriesResponse,
  SourceDef,
  KafkaTransport,
  TrackDefinition,
} from "./types";

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(await res.text());
  return res.json() as Promise<T>;
}

async function postJson<T>(url: string, body: unknown): Promise<T> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json() as Promise<T>;
}

export const api = {
  health: () => getJson<{ ok: boolean; source: string; postgres_enabled: boolean }>("/api/health"),
  sources: () => getJson<{ sources: SourceDef[] }>("/api/sources"),
  channels: (source: string) => getJson<{ channels: ChannelDef[]; default: string }>(`/api/channels?source=${encodeURIComponent(source)}`),
  calendar: (source: string, channel: string, threshold: number, minDurationS: number, validOnly = true) =>
    getJson<{ days: DriveDay[] }>(
      `/api/calendar?source=${encodeURIComponent(source)}&channel=${encodeURIComponent(channel)}&threshold=${threshold}&minDurationS=${minDurationS}&validOnly=${validOnly}`,
    ),
  day: (source: string, date: string, channel: string) =>
    getJson<DayDetail>(`/api/day/${date}?source=${encodeURIComponent(source)}&channel=${encodeURIComponent(channel)}`),
  series: (source: string, channel: string, startMs: number, endMs: number, maxPoints = 5000) =>
    getJson<SeriesResponse>(
      `/api/series?source=${encodeURIComponent(source)}&channel=${encodeURIComponent(channel)}&startMs=${startMs}&endMs=${endMs}&maxPoints=${maxPoints}`,
    ),
  gps: (source: string, startMs: number, endMs: number, maxPoints = 2000) =>
    getJson<GpsResponse>(`/api/gps?source=${encodeURIComponent(source)}&startMs=${startMs}&endMs=${endMs}&maxPoints=${maxPoints}`),
  segments: (source: string, channel: string, startMs: number, endMs: number, threshold: number, minDurationS: number) =>
    getJson<SegmentResponse>(
      `/api/segments?source=${encodeURIComponent(source)}&channel=${encodeURIComponent(channel)}&startMs=${startMs}&endMs=${endMs}&threshold=${threshold}&minDurationS=${minDurationS}`,
    ),
  tracks: () => getJson<{ tracks: TrackDefinition[] }>("/api/tracks"),
  saveTrack: (track: TrackDefinition) => postJson<TrackDefinition>("/api/tracks", track),
  channelCharts: () => getJson<{ charts: ChannelChartDefinition[] }>("/api/channel-charts"),
  saveChannelChart: (chart: ChannelChartDefinition) => postJson<ChannelChartDefinition>("/api/channel-charts", chart),
  export: (body: unknown) => postJson<ExportResponse>("/api/export", body),
  liveConfig: (source: string, topic = "", transport: KafkaTransport = "local") =>
    getJson<{ source: string; topic: string; transport: KafkaTransport; bootstrap_servers: string; mqtt_host?: string; mqtt_port?: number }>(
      `/api/live/config?source=${encodeURIComponent(source)}&topic=${encodeURIComponent(topic)}&transport=${encodeURIComponent(transport)}`,
    ),
  exportLiveLap: (body: unknown) => postJson<LiveLapExportResponse>("/api/live/export-lap", body),
};
