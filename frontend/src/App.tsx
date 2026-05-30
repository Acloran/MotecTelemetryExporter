import { useEffect, useMemo, useRef, useState, type ChangeEvent, type Dispatch, type MouseEvent, type ReactNode, type SetStateAction } from "react";
import { Activity, ArrowDown, ArrowUp, CalendarDays, Disc3, Download, FileText, Flag, Gauge, MapPinned, Moon, NotebookText, Plus, Radio, RefreshCcw, Save, Scissors, SlidersHorizontal, Sun, Target, Thermometer, Timer, Trash2, Upload, Zap } from "lucide-react";
import { api } from "./api";
import type {
  ChannelDef,
  ChannelChartDefinition,
  DayDetail,
  DriveDay,
  GateLine,
  GpsPoint,
  KafkaTransport,
  LiveSample,
  LiveStreamEvent,
  SegmentSummary,
  SeriesPoint,
  SessionSummary,
  SourceDef,
  TrackDefinition,
} from "./types";

const DEFAULT_TRACK: TrackDefinition = {
  name: "Orion Test Track",
  slug: "orion-test-track",
  notes: "",
  gates: [],
};

const DEFAULT_CHANNEL_CHART: ChannelChartDefinition = {
  name: "Orion Default",
  slug: "orion-default",
  notes: "Default Orion channel quantity/unit chart.",
  entries: [],
};
const ANGELIQUE_CHANNEL_CHART_SLUG = "angelique-channel-chart-04-14-26";

const METADATA_FIELDS = [
  "driver",
  "vehicle_id",
  "vehicle_weight",
  "vehicle_type",
  "vehicle_comment",
  "venue",
  "event",
  "session",
  "short_comment",
  "long_comment",
] as const;

type MetadataField = (typeof METADATA_FIELDS)[number];
type SessionMetadata = Record<MetadataField, string>;
type GateDrawMode = "start_finish" | "split" | null;
type AppTab = "exporter" | "live" | "track-builder" | "race-ops";
type LapPreviewRow = {
  id: string;
  label: string;
  kind: "lap" | "outlap";
  startMs: number;
  endMs: number;
  durationMs: number;
};
type LiveLap = {
  id: string;
  label: string;
  kind: "flying" | "manual";
  startMs: number;
  endMs: number;
  durationMs: number;
  sectors: number[];
  energyWh: number;
  distanceM: number;
  avgSpeedMps: number | null;
  samples: LiveSample[];
};
type LiveSessionState = {
  running: boolean;
  connected: boolean;
  status: string;
  topic: string;
  startedAt: number | null;
  lastSample: LiveSample | null;
  previousSample: LiveSample | null;
  samples: LiveSample[];
  lapSamples: LiveSample[];
  laps: LiveLap[];
  lapStartMs: number | null;
  sectorStartMs: number | null;
  nextSplitIndex: number;
  currentSectors: number[];
  totalEnergyWh: number;
  lapEnergyWh: number;
  lapDistanceM: number;
  deltaRate: number | null;
  deltaMs: number;
};
type CarPreset = {
  id: string;
  name: string;
  source: "orion" | "angelique";
  topic: string;
  transport: KafkaTransport;
  trackSlug: string;
  channelChartSlug: string;
  metadata: SessionMetadata;
};

const EMPTY_METADATA: SessionMetadata = {
  driver: "",
  vehicle_id: "",
  vehicle_weight: "",
  vehicle_type: "EV",
  vehicle_comment: "",
  venue: "",
  event: "",
  session: "",
  short_comment: "",
  long_comment: "",
};

const EMPTY_LIVE_STATE: LiveSessionState = {
  running: false,
  connected: false,
  status: "Idle",
  topic: "",
  startedAt: null,
  lastSample: null,
  previousSample: null,
  samples: [],
  lapSamples: [],
  laps: [],
  lapStartMs: null,
  sectorStartMs: null,
  nextSplitIndex: 0,
  currentSectors: [],
  totalEnergyWh: 0,
  lapEnergyWh: 0,
  lapDistanceM: 0,
  deltaRate: null,
  deltaMs: 0,
};

function defaultCarPresets(): CarPreset[] {
  return [
    {
      id: "orion",
      name: "Orion",
      source: "orion",
      topic: "orion",
      transport: "mqtt",
      trackSlug: DEFAULT_TRACK.slug,
      channelChartSlug: DEFAULT_CHANNEL_CHART.slug,
      metadata: {
        ...EMPTY_METADATA,
        vehicle_id: "Orion",
        vehicle_type: "EV",
        event: "Orion Live Telemetry",
      },
    },
    {
      id: "angelique",
      name: "Angelique",
      source: "angelique",
      topic: "grafana_data_angelique",
      transport: "local",
      trackSlug: DEFAULT_TRACK.slug,
      channelChartSlug: ANGELIQUE_CHANNEL_CHART_SLUG,
      metadata: {
        ...EMPTY_METADATA,
        vehicle_id: "Angelique",
        vehicle_type: "EV",
        event: "Angelique Live Telemetry",
      },
    },
  ];
}

function loadCarPresets(): CarPreset[] {
  try {
    const parsed = JSON.parse(localStorage.getItem("motec-car-presets") || "null");
    if (!Array.isArray(parsed)) return defaultCarPresets();
    return parsed.map(normalizeCarPreset).filter(Boolean) as CarPreset[];
  } catch {
    return defaultCarPresets();
  }
}

function normalizeCarPreset(value: unknown): CarPreset | null {
  if (!value || typeof value !== "object") return null;
  const raw = value as Partial<CarPreset>;
  const source = raw.source === "angelique" ? "angelique" : "orion";
  const name = String(raw.name || (source === "angelique" ? "Angelique" : "Orion")).trim();
  const metadata = { ...EMPTY_METADATA, ...(raw.metadata || {}) };
  const channelChartSlug = String(raw.channelChartSlug || defaultChannelChartSlugForSource(source));
  const migratedChannelChartSlug =
    source === "angelique" && raw.id === "angelique" && channelChartSlug === DEFAULT_CHANNEL_CHART.slug
      ? ANGELIQUE_CHANNEL_CHART_SLUG
      : channelChartSlug;
  return {
    id: String(raw.id || slugifyTrackName(name) || source),
    name,
    source,
    topic: String(raw.topic || (source === "orion" ? "orion" : `grafana_data_${source}`)),
    transport: raw.transport === "mqtt" ? "mqtt" : raw.transport === "kafka" ? "kafka" : "local",
    trackSlug: String(raw.trackSlug || DEFAULT_TRACK.slug),
    channelChartSlug: migratedChannelChartSlug,
    metadata,
  };
}

function App() {
  const [activeTab, setActiveTab] = useState<AppTab>("exporter");
  const [health, setHealth] = useState<{ source: string; postgres_enabled: boolean } | null>(null);
  const [sources, setSources] = useState<SourceDef[]>([]);
  const [source, setSource] = useState<"orion" | "angelique">("orion");
  const [channels, setChannels] = useState<ChannelDef[]>([]);
  const [channel, setChannel] = useState("");
  const [threshold, setThreshold] = useState(0);
  const [minDurationS, setMinDurationS] = useState(10);
  const [exportType, setExportType] = useState<"motec" | "csv">("motec");
  const [days, setDays] = useState<DriveDay[]>([]);
  const [sessionCountsByDate, setSessionCountsByDate] = useState<Record<string, number>>({});
  const [selectedDate, setSelectedDate] = useState("");
  const [detail, setDetail] = useState<DayDetail | null>(null);
  const [session, setSession] = useState<SessionSummary | null>(null);
  const [segments, setSegments] = useState<SegmentSummary[]>([]);
  const [series, setSeries] = useState<SeriesPoint[]>([]);
  const [gps, setGps] = useState<GpsPoint[]>([]);
  const [range, setRange] = useState<[number, number] | null>(null);
  const [tracks, setTracks] = useState<TrackDefinition[]>([]);
  const [track, setTrack] = useState<TrackDefinition>(DEFAULT_TRACK);
  const [channelCharts, setChannelCharts] = useState<ChannelChartDefinition[]>([]);
  const [channelChart, setChannelChart] = useState<ChannelChartDefinition>(DEFAULT_CHANNEL_CHART);
  const [gateDrawMode, setGateDrawMode] = useState<GateDrawMode>(null);
  const [selectedSegments, setSelectedSegments] = useState<Set<string>>(new Set());
  const [previewSelectedSegments, setPreviewSelectedSegments] = useState<Set<string>>(new Set());
  const [lastPreviewSegmentId, setLastPreviewSegmentId] = useState("");
  const [exportUrl, setExportUrl] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [sessionMetadata, setSessionMetadata] = useState<Record<string, SessionMetadata>>({});
  const [metadataDraft, setMetadataDraft] = useState<SessionMetadata>(EMPTY_METADATA);
  const [mixedMetadata, setMixedMetadata] = useState<Set<MetadataField>>(new Set());
  const [darkMode, setDarkMode] = useState(() => localStorage.getItem("motec-theme") === "dark");
  const [builderCenter, setBuilderCenter] = useState({ lat: 30.3922, lon: -97.7287 });
  const [builderSearch, setBuilderSearch] = useState("30.3922, -97.7287");
  const [autoLiveDownload, setAutoLiveDownload] = useState(false);
  const [energyWindowS, setEnergyWindowS] = useState(60);
  const [liveSampleHz, setLiveSampleHz] = useState(() => Number(localStorage.getItem("motec-live-sample-hz") || 2));
  const [liveTopic, setLiveTopic] = useState(() => localStorage.getItem("motec-live-topic") || "");
  const [liveTransport, setLiveTransport] = useState<KafkaTransport>(() => {
    const stored = localStorage.getItem("motec-live-transport");
    return stored === "mqtt" || stored === "kafka" ? stored : "local";
  });
  const [carPresets, setCarPresets] = useState<CarPreset[]>(loadCarPresets);
  const [selectedCarPresetId, setSelectedCarPresetId] = useState(() => localStorage.getItem("motec-selected-car-preset") || "orion");
  const [carPresetDraft, setCarPresetDraft] = useState<CarPreset>(() => loadCarPresets()[0] ?? defaultCarPresets()[0]);
  const [liveState, setLiveState] = useState<LiveSessionState>(EMPTY_LIVE_STATE);
  const liveSourceRef = useRef<EventSource | null>(null);
  const liveStateRef = useRef<LiveSessionState>(EMPTY_LIVE_STATE);
  const liveTrackRef = useRef(track);
  const liveMetadataRef = useRef(metadataDraft);
  const liveChannelChartRef = useRef(channelChart);
  const liveAutoDownloadRef = useRef(autoLiveDownload);
  const liveSourceNameRef = useRef(source);

  const selectedChannel = channels.find((c) => c.key === channel);
  const sourceLabel = sources.find((item) => item.key === source)?.label || source;
  const hasStartFinish = track.gates.some((gate) => gate.role === "start_finish");
  const splitGates = track.gates.filter((gate) => gate.role === "split");
  const filteredLiveSamples = useMemo(() => smoothLiveSamples(liveState.samples), [liveState.samples]);
  const filteredLiveLastSample = filteredLiveSamples.at(-1) ?? liveState.lastSample;
  const filteredLiveState = useMemo<LiveSessionState>(() => ({
    ...liveState,
    lastSample: filteredLiveLastSample,
    previousSample: filteredLiveSamples.length > 1 ? filteredLiveSamples[filteredLiveSamples.length - 2] : liveState.previousSample,
    samples: filteredLiveSamples,
  }), [liveState, filteredLiveLastSample, filteredLiveSamples]);
  const bestLap = useMemo(() => liveState.laps.reduce<LiveLap | null>((best, lap) => (!best || lap.durationMs < best.durationMs ? lap : best), null), [liveState.laps]);
  const bestSectors = useMemo(() => bestSectorTimes(liveState.laps), [liveState.laps]);
  const liveLapElapsedMs = liveState.lastSample && liveState.lapStartMs ? Math.max(0, liveState.lastSample.t - liveState.lapStartMs) : 0;
  const liveBadgeLabel = liveState.lastSample ? "Live" : liveState.connected ? "Listening" : liveState.running ? "Connecting" : "Standby";
  const liveBadgeClass = liveState.lastSample ? "liveBadge liveOn" : liveState.connected ? "liveBadge liveListening" : liveState.running ? "liveBadge liveConnecting" : "liveBadge";
  const previewSegments = useMemo(
    () => segments.filter((segment) => previewSelectedSegments.has(segment.id)),
    [segments, previewSelectedSegments],
  );
  const previewKey = useMemo(
    () => [...previewSelectedSegments].sort().join("|"),
    [previewSelectedSegments],
  );
  const trackViewKey = useMemo(() => track.gates.map((gate) => `${gate.id}:${gate.lat1}:${gate.lon1}:${gate.lat2}:${gate.lon2}`).join("|"), [track.gates]);
  const selectedTrackView = useMemo(() => trackViewFromGates(track.gates), [trackViewKey]);
  const previewSummary = useMemo(() => summarizeSegments(previewSegments), [previewSegments]);
  const lapPreview = useMemo(() => buildLapPreview(gps, track.gates, previewSummary), [gps, track.gates, previewSummary]);
  const sourceRanges = detail?.sessions ?? (session ? [session] : []);
  const sourceRangeKey = useMemo(
    () => sourceRanges.map((item) => `${item.id}:${item.start_ms}:${item.end_ms}`).join("|"),
    [sourceRanges],
  );
  const exportSegments = useMemo(() => {
    if (range) {
      return [{ id: "manual-selection", label: "Manual plot range", start_ms: range[0], end_ms: range[1], metadata: metadataDraft }];
    }
    return segments
      .filter((s) => selectedSegments.has(s.id))
      .map((s) => ({
        id: s.id,
        label: s.label,
        start_ms: s.start_ms,
        end_ms: s.end_ms,
        metadata: sessionMetadata[s.id] ?? defaultMetadataForSegment(s, sourceLabel, selectedDate),
      }));
  }, [range, segments, selectedSegments, sessionMetadata, metadataDraft, sourceLabel, selectedDate]);

  useEffect(() => {
    refreshBase(source);
  }, [source]);

  useEffect(() => {
    if (!channel) return;
    const timeout = window.setTimeout(() => {
      void refreshCalendar(source, channel, threshold, minDurationS);
    }, 300);
    return () => window.clearTimeout(timeout);
  }, [source, channel, threshold, minDurationS]);

  useEffect(() => {
    document.documentElement.dataset.theme = darkMode ? "dark" : "light";
    localStorage.setItem("motec-theme", darkMode ? "dark" : "light");
  }, [darkMode]);

  useEffect(() => {
    localStorage.setItem("motec-live-topic", liveTopic);
  }, [liveTopic]);

  useEffect(() => {
    localStorage.setItem("motec-live-transport", liveTransport);
  }, [liveTransport]);

  useEffect(() => {
    localStorage.setItem("motec-live-sample-hz", String(liveSampleHz));
  }, [liveSampleHz]);

  useEffect(() => {
    localStorage.setItem("motec-car-presets", JSON.stringify(carPresets));
  }, [carPresets]);

  useEffect(() => {
    localStorage.setItem("motec-selected-car-preset", selectedCarPresetId);
    const preset = carPresets.find((item) => item.id === selectedCarPresetId);
    if (preset) setCarPresetDraft(preset);
  }, [carPresets, selectedCarPresetId]);

  useEffect(() => {
    if (!selectedTrackView) return;
    setBuilderCenter(selectedTrackView.center);
    setBuilderSearch(`${selectedTrackView.center.lat.toFixed(6)}, ${selectedTrackView.center.lon.toFixed(6)}`);
  }, [selectedTrackView]);

  useEffect(() => {
    liveStateRef.current = liveState;
  }, [liveState]);

  useEffect(() => {
    liveTrackRef.current = track;
    liveMetadataRef.current = metadataDraft;
    liveChannelChartRef.current = channelChart;
    liveAutoDownloadRef.current = autoLiveDownload;
    liveSourceNameRef.current = source;
  }, [track, metadataDraft, channelChart, autoLiveDownload, source]);

  useEffect(() => {
    return () => {
      liveSourceRef.current?.close();
    };
  }, []);

  useEffect(() => {
    if (!selectedDate || !channel) return;
    let cancelled = false;
    setBusy(true);
    setError("");
    api
      .day(source, selectedDate, channel)
      .then((d) => {
        if (cancelled) return;
        setDetail(d);
        setSession(d.sessions[0] ?? null);
        setSegments([]);
        setSelectedSegments(new Set());
        setPreviewSelectedSegments(new Set());
        setLastPreviewSegmentId("");
        setSeries([]);
        setGps([]);
      })
      .catch((e) => {
        if (!cancelled) showError(e);
      })
      .finally(() => {
        if (!cancelled) setBusy(false);
      });
    return () => {
      cancelled = true;
    };
  }, [source, selectedDate, channel]);

  useEffect(() => {
    if (!sourceRanges.length || !channel) return;
    let cancelled = false;
    setRange(null);
    setError("");
    setSegments([]);
    setSelectedSegments(new Set());
    setPreviewSelectedSegments(new Set());
    setLastPreviewSegmentId("");
    setSeries([]);
    setGps([]);

    Promise.all(sourceRanges.map((item) => api.segments(source, channel, item.start_ms, item.end_ms, threshold, minDurationS)))
      .then((responses) => {
        if (cancelled) return;
        const nextSegments = responses
          .flatMap((response) => response.segments)
          .sort((a, b) => a.start_ms - b.start_ms)
          .map((segment, index) => ({ ...segment, label: `Session ${index + 1}` }));
        setSegments(nextSegments);
        setSelectedSegments(new Set(nextSegments.map((segment) => segment.id)));
        setPreviewSelectedSegments(new Set(nextSegments[0] ? [nextSegments[0].id] : []));
        setLastPreviewSegmentId(nextSegments[0]?.id ?? "");
        setSessionMetadata((prev) => {
          const next = { ...prev };
          nextSegments.forEach((segment) => {
            next[segment.id] = next[segment.id] ?? defaultMetadataForSegment(segment, sourceLabel, selectedDate);
          });
          return next;
        });
        setDays((prev) =>
          prev.map((day) =>
            day.date === selectedDate
              ? { ...day, sessions: nextSegments.length, label: `${day.date} (${nextSegments.length} sessions)` }
              : day,
          ),
        );
        setSessionCountsByDate((prev) => ({ ...prev, [selectedDate]: nextSegments.length }));
      })
      .catch((e) => {
        if (!cancelled) showError(e);
      });
    return () => {
      cancelled = true;
    };
  }, [source, sourceRangeKey, channel, threshold, minDurationS, sourceLabel, selectedDate]);

  useEffect(() => {
    if (!channel || !previewSegments.length) {
      setSeries([]);
      setGps([]);
      return;
    }
    let cancelled = false;
    const ordered = [...previewSegments].sort((a, b) => a.start_ms - b.start_ms);
    const seriesPointsPerSegment = Math.max(300, Math.floor(8000 / ordered.length));
    const gpsPointsPerSegment = Math.max(150, Math.floor(4000 / ordered.length));
    setBusy(true);
    Promise.all([
      Promise.all(ordered.map((segment) => api.series(source, channel, segment.start_ms, segment.end_ms, seriesPointsPerSegment))),
      Promise.all(ordered.map((segment) => api.gps(source, segment.start_ms, segment.end_ms, gpsPointsPerSegment))),
    ])
      .then(([seriesResponses, gpsResponses]) => {
        if (cancelled) return;
        setSeries(seriesResponses.flatMap((response) => response.points).sort((a, b) => a.t - b.t));
        setGps(gpsResponses.flatMap((response) => response.points).sort((a, b) => a.t - b.t));
        setError("");
      })
      .catch(showError)
      .finally(() => {
        if (!cancelled) setBusy(false);
      });
    return () => {
      cancelled = true;
    };
  }, [source, channel, previewKey, previewSegments]);

  useEffect(() => {
    if (!previewSegments.length) {
      setMetadataDraft(defaultMetadataBase(sourceLabel, selectedDate));
      setMixedMetadata(new Set());
      return;
    }
    const nextDraft = { ...EMPTY_METADATA };
    const mixed = new Set<MetadataField>();
    METADATA_FIELDS.forEach((field) => {
      const values = previewSegments.map((segment) => (sessionMetadata[segment.id] ?? defaultMetadataForSegment(segment, sourceLabel, selectedDate))[field] ?? "");
      const first = values[0] ?? "";
      if (values.every((value) => value === first)) {
        nextDraft[field] = first;
      } else {
        nextDraft[field] = "";
        mixed.add(field);
      }
    });
    setMetadataDraft(nextDraft);
    setMixedMetadata(mixed);
  }, [previewKey, previewSegments, sessionMetadata, sourceLabel, selectedDate]);

  async function refreshBase(nextSource = source) {
    setBusy(true);
    setError("");
    try {
      const [h, src, c, tr, charts] = await Promise.all([
        api.health(),
        api.sources(),
        api.channels(nextSource),
        api.tracks(),
        api.channelCharts(),
      ]);
      setHealth(h);
      setSources(src.sources);
      setChannels(c.channels);
      setChannel(c.default);
      setSessionCountsByDate({});
      setDays([]);
      setSelectedDate("");
      setTracks(tr.tracks);
      setTrack(tr.tracks[0] ?? DEFAULT_TRACK);
      setChannelCharts(charts.charts);
      setChannelChart(preferredChannelChart(charts.charts, nextSource));
      setDetail(null);
      setSession(null);
    } catch (e) {
      showError(e);
    } finally {
      setBusy(false);
    }
  }

  async function refreshCalendar(nextSource = source, nextChannel = channel, nextThreshold = threshold, nextMinDurationS = minDurationS) {
    if (!nextChannel) return;
    setBusy(true);
    setError("");
    try {
      const cal = await api.calendar(nextSource, nextChannel, nextThreshold, nextMinDurationS, true);
      setDays(cal.days);
      setSessionCountsByDate({});
      setSelectedDate((current) => (cal.days.some((day) => day.date === current) ? current : cal.days[0]?.date || ""));
      if (!cal.days.length) {
        setDetail(null);
        setSession(null);
        setSegments([]);
        setSelectedSegments(new Set());
        setPreviewSelectedSegments(new Set());
      }
    } catch (e) {
      showError(e);
    } finally {
      setBusy(false);
    }
  }

  function showError(e: unknown) {
    setError(e instanceof Error ? e.message : String(e));
  }

  function toggleSegment(segment: SegmentSummary) {
    setRange(null);
    setSelectedSegments((prev) => {
      const next = new Set(prev);
      if (next.has(segment.id)) next.delete(segment.id);
      else next.add(segment.id);
      return next;
    });
  }

  function handleSessionClick(event: MouseEvent, segment: SegmentSummary) {
    setRange(null);
    const segmentIds = segments.map((item) => item.id);
    const currentIndex = segmentIds.indexOf(segment.id);
    const lastIndex = segmentIds.indexOf(lastPreviewSegmentId);
    if (event.shiftKey && lastIndex >= 0 && currentIndex >= 0) {
      const [start, end] = [lastIndex, currentIndex].sort((a, b) => a - b);
      setPreviewSelectedSegments(new Set(segmentIds.slice(start, end + 1)));
    } else if (event.metaKey || event.ctrlKey) {
      setPreviewSelectedSegments((prev) => {
        const next = new Set(prev);
        if (next.has(segment.id)) next.delete(segment.id);
        else next.add(segment.id);
        return next.size ? next : new Set([segment.id]);
      });
    } else {
      setPreviewSelectedSegments(new Set([segment.id]));
    }
    setLastPreviewSegmentId(segment.id);
  }

  function selectAllSessionsForPreview() {
    setRange(null);
    setPreviewSelectedSegments(new Set(segments.map((segment) => segment.id)));
    setLastPreviewSegmentId(segments.at(-1)?.id ?? "");
  }

  function applyMetadata() {
    const selected = previewSegments;
    if (!selected.length) return;
    setSessionMetadata((prev) => {
      const next = { ...prev };
      selected.forEach((segment) => {
        const current = next[segment.id] ?? defaultMetadataForSegment(segment, sourceLabel, selectedDate);
        const updated = { ...current };
        METADATA_FIELDS.forEach((field) => {
          if (!mixedMetadata.has(field) || metadataDraft[field] !== "") {
            updated[field] = metadataDraft[field];
          }
        });
        next[segment.id] = updated;
      });
      return next;
    });
  }

  function handleDrawGate(gate: GateLine) {
    setTrack((prev) => {
      if (gate.role === "start_finish" && prev.gates.some((item) => item.role === "start_finish")) return prev;
      return normalizeTrack({ ...prev, gates: [...prev.gates, gate] });
    });
    setGateDrawMode(null);
  }

  function updateGate(index: number, patch: Partial<GateLine>) {
    setTrack((prev) => ({
      ...prev,
      gates: prev.gates.map((gate, gateIndex) => (gateIndex === index ? { ...gate, ...patch } : gate)),
    }));
  }

  function removeGate(index: number) {
    setTrack((prev) => normalizeTrack({ ...prev, gates: prev.gates.filter((_, gateIndex) => gateIndex !== index) }));
  }

  function moveSplitGate(index: number, direction: -1 | 1) {
    setTrack((prev) => {
      const gate = prev.gates[index];
      if (!gate || gate.role !== "split") return prev;
      const splitIndexes = prev.gates.flatMap((item, itemIndex) => (item.role === "split" ? [itemIndex] : []));
      const splitPosition = splitIndexes.indexOf(index);
      const targetIndex = splitIndexes[splitPosition + direction];
      if (targetIndex == null) return prev;
      const gates = [...prev.gates];
      [gates[index], gates[targetIndex]] = [gates[targetIndex], gates[index]];
      return normalizeTrack({ ...prev, gates });
    });
  }

  async function saveTrack(nextTrack = track) {
    setBusy(true);
    try {
      const saved = await api.saveTrack(normalizeTrack(nextTrack));
      setTrack(saved);
      const list = await api.tracks();
      setTracks(list.tracks);
    } catch (e) {
      showError(e);
    } finally {
      setBusy(false);
    }
  }

  function newTrack() {
    setTrack({ name: "New Track", slug: "", notes: "", gates: [] });
    setGateDrawMode(null);
  }

  function downloadTrack() {
    const blob = new Blob([JSON.stringify(track, null, 2) + "\n"], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${slugifyTrackName(track.name)}.json`;
    link.click();
    URL.revokeObjectURL(url);
  }

  async function uploadTrack(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = "";
    if (!file) return;
    try {
      const uploaded = JSON.parse(await file.text()) as TrackDefinition;
      if (!uploaded.name || !Array.isArray(uploaded.gates)) throw new Error("Track JSON is missing a name or gates array.");
      await saveTrack({
        ...DEFAULT_TRACK,
        ...uploaded,
        slug: uploaded.slug || slugifyTrackName(uploaded.name),
        gates: uploaded.gates,
      });
    } catch (e) {
      showError(e);
    }
  }

  async function saveChannelChart(nextChart = channelChart) {
    setBusy(true);
    try {
      const saved = await api.saveChannelChart(nextChart);
      setChannelChart(saved);
      const list = await api.channelCharts();
      setChannelCharts(list.charts);
    } catch (e) {
      showError(e);
    } finally {
      setBusy(false);
    }
  }

  function downloadChannelChart() {
    const blob = new Blob([JSON.stringify(channelChart, null, 2) + "\n"], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${slugifyTrackName(channelChart.name)}.json`;
    link.click();
    URL.revokeObjectURL(url);
  }

  async function uploadChannelChart(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = "";
    if (!file) return;
    try {
      const text = await file.text();
      const uploaded = file.name.toLowerCase().endsWith(".json")
        ? parseChannelChartJson(text, file.name)
        : parseChannelChartCsv(text, file.name);
      await saveChannelChart(uploaded);
    } catch (e) {
      showError(e);
    }
  }

  function applyCarPreset(preset = carPresetDraft) {
    setSelectedCarPresetId(preset.id);
    setSource(preset.source);
    setLiveTopic(preset.topic);
    setLiveTransport(preset.transport);
    setMetadataDraft({ ...EMPTY_METADATA, ...preset.metadata });
    const presetTrack = tracks.find((item) => item.slug === preset.trackSlug);
    if (presetTrack) setTrack(normalizeTrack(presetTrack));
    const presetChart = channelCharts.find((item) => item.slug === preset.channelChartSlug);
    if (presetChart) setChannelChart(presetChart);
  }

  function saveCarPreset() {
    const normalized = normalizeCarPreset({
      ...carPresetDraft,
      id: carPresetDraft.id || slugifyTrackName(carPresetDraft.name),
      name: carPresetDraft.name || sourceLabel,
      topic: carPresetDraft.topic.trim() || `grafana_data_${carPresetDraft.source}`,
      trackSlug: carPresetDraft.trackSlug || track.slug,
      channelChartSlug: carPresetDraft.channelChartSlug || channelChart.slug,
    });
    if (!normalized) return;
    setCarPresets((current) => {
      const existing = current.some((item) => item.id === normalized.id);
      return existing ? current.map((item) => (item.id === normalized.id ? normalized : item)) : [...current, normalized];
    });
    setSelectedCarPresetId(normalized.id);
    setCarPresetDraft(normalized);
  }

  function newCarPreset() {
    const id = `car-${Date.now()}`;
    const preset: CarPreset = {
      id,
      name: "New Car",
      source,
      topic: `grafana_data_${source}`,
      transport: "local",
      trackSlug: track.slug,
      channelChartSlug: channelChart.slug,
      metadata: { ...EMPTY_METADATA, vehicle_id: "New Car", vehicle_type: "EV" },
    };
    setCarPresets((current) => [...current, preset]);
    setSelectedCarPresetId(id);
    setCarPresetDraft(preset);
  }

  function deleteCarPreset() {
    const next = carPresets.filter((item) => item.id !== carPresetDraft.id);
    const fallback = next[0] ?? defaultCarPresets()[0];
    setCarPresets(next.length ? next : [fallback]);
    setSelectedCarPresetId(fallback.id);
    setCarPresetDraft(fallback);
  }

  async function startLiveData() {
    liveSourceRef.current?.close();
    setExportUrl("");
    setLiveState({
      ...EMPTY_LIVE_STATE,
      running: true,
      status: liveTransport === "local" ? "Connecting to local replay topic..." : liveTransport === "mqtt" ? "Connecting to MQTT..." : "Connecting to Kafka...",
      startedAt: Date.now(),
    });
    try {
      const requestedTopic = liveTopic.trim();
      const config = await api.liveConfig(source, requestedTopic, liveTransport);
      setLiveState((prev) => ({ ...prev, topic: config.topic, status: liveTransport === "mqtt" ? `Listening to MQTT ${config.topic}` : `Listening to ${config.topic}` }));
      const params = new URLSearchParams({ source, topic: requestedTopic, transport: liveTransport, sampleHz: String(liveSampleHz) });
      const eventSource = new EventSource(`/api/live/stream?${params.toString()}`);
      liveSourceRef.current = eventSource;
      eventSource.onopen = () => {
        setLiveState((prev) => ({ ...prev, connected: true }));
      };
      eventSource.onmessage = (event) => {
        try {
          const parsed = JSON.parse(event.data) as LiveStreamEvent;
          handleLiveEvent(parsed);
        } catch (e) {
          setLiveState((prev) => ({ ...prev, status: e instanceof Error ? e.message : String(e) }));
        }
      };
      eventSource.onerror = () => {
        liveSourceRef.current?.close();
        liveSourceRef.current = null;
        setLiveState((prev) => {
          if (!prev.running) return prev;
          return { ...prev, running: false, connected: false, status: "Live stream connection interrupted." };
        });
      };
    } catch (e) {
      showError(e);
      setLiveState((prev) => ({ ...prev, running: false, status: "Unable to start live data." }));
    }
  }

  function stopLiveData() {
    liveSourceRef.current?.close();
    liveSourceRef.current = null;
    setLiveState((prev) => ({ ...prev, running: false, connected: false, status: prev.laps.length ? "Stopped" : "Stopped without completed laps" }));
  }

  function handleLiveEvent(event: LiveStreamEvent) {
    if (event.type === "status") {
      if (!event.ok) {
        liveSourceRef.current?.close();
        liveSourceRef.current = null;
      }
      setLiveState((prev) => ({
        ...prev,
        running: event.ok ? prev.running : false,
        connected: event.ok,
        status: event.ok ? event.message : `${event.message}${event.detail ? ` ${event.detail}` : ""}`,
        topic: event.topic || prev.topic,
      }));
      return;
    }
    if (event.type === "heartbeat") {
      setLiveState((prev) => ({
        ...prev,
        connected: true,
        topic: event.topic || prev.topic,
        status: prev.lastSample
          ? prev.status
          : event.transport === "mqtt"
            ? `MQTT connected; waiting for samples on ${event.topic || prev.topic}.`
            : `Broker connected; waiting for samples on ${event.topic || prev.topic}.`,
      }));
      return;
    }
    if (event.type !== "sample") return;
    const completedLap = processLiveSample(event.sample);
    if (completedLap && liveAutoDownloadRef.current) {
      void downloadLiveLap(completedLap);
    }
  }

  function processLiveSample(sample: LiveSample) {
    const current = liveStateRef.current;
    const previous = current.lastSample;
    const trackForLive = liveTrackRef.current;
    let completedLap: LiveLap | null = null;
    const dtSeconds = previous ? Math.max(0, (sample.t - previous.t) / 1000) : 0;
    const powerKw = Math.abs(sample.power_kw ?? sample.values.power_kw ?? 0);
    const energyDeltaWh = Math.max(0, powerKw * dtSeconds / 3.6);
    const distanceDeltaM = previous && hasGps(previous) && hasGps(sample) ? distanceMeters(previous.lat, previous.lon, sample.lat, sample.lon) : 0;
    let lapStartMs = current.lapStartMs;
    let sectorStartMs = current.sectorStartMs;
    let nextSplitIndex = current.nextSplitIndex;
    let currentSectors = [...current.currentSectors];
    let lapEnergyWh = current.lapEnergyWh + energyDeltaWh;
    let lapDistanceM = current.lapDistanceM + distanceDeltaM;
    let lapSamples = [...current.lapSamples, sample].slice(-20000);
    let resetDelta = false;
    const laps = [...current.laps];

    if (previous && hasGps(previous) && hasGps(sample)) {
      const startGate = trackForLive.gates.find((gate) => gate.role === "start_finish");
      const splits = trackForLive.gates.filter((gate) => gate.role === "split");
      if (startGate && sampleCrossesGate(previous, sample, startGate)) {
        if (lapStartMs && sample.t - lapStartMs > 5000) {
          const durationMs = sample.t - lapStartMs;
          const finalSectorMs = sectorStartMs ? sample.t - sectorStartMs : durationMs;
          const sectors = splits.length && finalSectorMs > 0 ? [...currentSectors, finalSectorMs] : [];
          const completedSamples = lapSamples;
          completedLap = {
            id: `live-lap-${laps.length + 1}-${sample.t}`,
            label: `Lap ${laps.length + 1}`,
            kind: "flying",
            startMs: lapStartMs,
            endMs: sample.t,
            durationMs,
            sectors,
            energyWh: lapEnergyWh,
            distanceM: lapDistanceM,
            avgSpeedMps: durationMs > 0 && lapDistanceM > 0 ? lapDistanceM / (durationMs / 1000) : null,
            samples: completedSamples,
          };
          laps.push(completedLap);
        }
        lapStartMs = sample.t;
        sectorStartMs = sample.t;
        nextSplitIndex = 0;
        currentSectors = [];
        lapEnergyWh = 0;
        lapDistanceM = 0;
        lapSamples = [sample];
        resetDelta = true;
      } else if (lapStartMs && nextSplitIndex < splits.length) {
        const nextGate = splits[nextSplitIndex];
        if (sampleCrossesGate(previous, sample, nextGate)) {
          const sectorStart = sectorStartMs ?? lapStartMs;
          const sectorMs = sample.t - sectorStart;
          if (sectorMs > 500) {
            currentSectors.push(sectorMs);
            sectorStartMs = sample.t;
            nextSplitIndex += 1;
          }
        }
      }
    }

    const best = laps.reduce<LiveLap | null>((candidate, lap) => (!candidate || lap.durationMs < candidate.durationMs ? lap : candidate), null);
    const deltaRate = estimateDeltaRate(sample, best);
    const deltaMs = resetDelta ? 0 : lapStartMs && deltaRate != null ? current.deltaMs + deltaRate * dtSeconds * 1000 : current.deltaMs;
    const nextState = {
      ...current,
      running: true,
      connected: true,
      status: current.topic ? `Live on ${current.topic}` : "Live",
      lastSample: sample,
      previousSample: previous,
      samples: [...current.samples, sample].slice(-30000),
      lapSamples,
      laps,
      lapStartMs,
      sectorStartMs,
      nextSplitIndex,
      currentSectors,
      totalEnergyWh: current.totalEnergyWh + energyDeltaWh,
      lapEnergyWh,
      lapDistanceM,
      deltaRate,
      deltaMs,
    };
    liveStateRef.current = nextState;
    setLiveState(nextState);
    return completedLap;
  }

  function triggerManualLap() {
    const current = liveStateRef.current;
    const sample = current.lastSample;
    if (!sample) {
      setLiveState((prev) => ({ ...prev, status: "Waiting for a live sample before starting a manual lap." }));
      return;
    }
    if (!current.lapStartMs) {
      const nextState = {
        ...current,
        lapStartMs: sample.t,
        sectorStartMs: sample.t,
        nextSplitIndex: 0,
        currentSectors: [],
        lapEnergyWh: 0,
        lapDistanceM: 0,
        lapSamples: [sample],
        deltaMs: 0,
        status: "Manual lap started.",
      };
      liveStateRef.current = nextState;
      setLiveState(nextState);
      return;
    }

    const durationMs = sample.t - current.lapStartMs;
    if (durationMs <= 0) {
      setLiveState((prev) => ({ ...prev, status: "Manual lap needs a positive time range." }));
      return;
    }

    const laps = [...current.laps];
    const completedLap: LiveLap = {
      id: `manual-lap-${laps.length + 1}-${sample.t}`,
      label: `Manual ${laps.length + 1}`,
      kind: "manual",
      startMs: current.lapStartMs,
      endMs: sample.t,
      durationMs,
      sectors: current.currentSectors,
      energyWh: current.lapEnergyWh,
      distanceM: current.lapDistanceM,
      avgSpeedMps: durationMs > 0 && current.lapDistanceM > 0 ? current.lapDistanceM / (durationMs / 1000) : null,
      samples: current.lapSamples.length ? current.lapSamples : [sample],
    };
    laps.push(completedLap);
    const nextState = {
      ...current,
      laps,
      lapStartMs: sample.t,
      sectorStartMs: sample.t,
      nextSplitIndex: 0,
      currentSectors: [],
      lapEnergyWh: 0,
      lapDistanceM: 0,
      lapSamples: [sample],
      deltaMs: 0,
      status: `${completedLap.label} logged.`,
    };
    liveStateRef.current = nextState;
    setLiveState(nextState);
  }

  async function downloadLiveLap(lap: LiveLap) {
    const current = liveStateRef.current;
    const lapSamples = lap.samples?.length ? lap.samples : current.samples.filter((sample) => sample.t >= lap.startMs && sample.t <= lap.endMs);
    if (lapSamples.length < 2 || lapSamples.at(-1)!.t <= lapSamples[0].t) {
      setLiveState((prev) => ({ ...prev, status: "Skipped live export because the completed lap did not have enough samples." }));
      return;
    }
    try {
      const response = await api.exportLiveLap({
        car: liveSourceNameRef.current,
        lap_label: lap.label,
        track_slug: liveTrackRef.current.slug || null,
        channel_chart_slug: liveChannelChartRef.current.slug || null,
        frequency_hz: 50,
        metadata: {
          ...liveMetadataRef.current,
          session: liveMetadataRef.current.session || lap.label,
          short_comment: liveMetadataRef.current.short_comment || "live-lap-latest",
        },
        samples: lapSamples.map((sample) => ({ t: sample.t, values: sample.values })),
      });
      const link = document.createElement("a");
      link.href = response.download_url;
      link.download = `${liveSourceNameRef.current}_live_latest.zip`;
      link.click();
      setExportUrl(response.download_url);
    } catch (e) {
      showError(e);
    }
  }

  async function exportSelection() {
    if (!exportSegments.length) return;
    setBusy(true);
    setError("");
    try {
      const response = await api.export({
        car: source,
        channel_keys: channels.map((c) => c.key),
        segments: exportSegments,
        export_type: exportType,
        frequency_hz: 50,
        track_slug: exportType === "motec" && track.gates.length ? track.slug : null,
        channel_chart_slug: channelChart.slug || null,
        metadata: {
          ...metadataDraft,
          vehicle_id: metadataDraft.vehicle_id || sourceLabel,
          event: metadataDraft.event || `${sourceLabel} Telemetry Export`,
          session: metadataDraft.session || selectedDate,
        },
      });
      setExportUrl(`/api/export/${response.export_id}/download`);
    } catch (e) {
      showError(e);
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="shell">
      <header className="topbar">
        <div>
          <h1>MoTeC Telemetry Exporter</h1>
          <p>{sourceLabel} database to plot, GPS splits, threshold files, and MoTeC export</p>
        </div>
        <div className="topActions">
          <button className="tool iconOnly" onClick={() => setDarkMode((value) => !value)} aria-label="Toggle dark mode">
            {darkMode ? <Sun size={16} /> : <Moon size={16} />}
          </button>
          <div className="status">
            <span className={health?.postgres_enabled ? "dot live" : "dot"} />
            {sourceLabel} {busy ? "syncing" : "ready"}
          </div>
        </div>
      </header>

      {error ? <div className="error">{error}</div> : null}

      <nav className="tabBar" aria-label="Telemetry workspace">
        <button className={activeTab === "exporter" ? "tab activeTab" : "tab"} onClick={() => setActiveTab("exporter")}>
          <Download size={16} /> Exporter
        </button>
        <button className={activeTab === "live" ? "tab activeTab" : "tab"} onClick={() => setActiveTab("live")}>
          <Radio size={16} /> Live Viewer
        </button>
        <button className={activeTab === "track-builder" ? "tab activeTab" : "tab"} onClick={() => setActiveTab("track-builder")}>
          <MapPinned size={16} /> Track Builder
        </button>
        <button className={activeTab === "race-ops" ? "tab activeTab" : "tab"} onClick={() => setActiveTab("race-ops")}>
          <NotebookText size={16} /> Race Ops
        </button>
      </nav>

      {activeTab === "exporter" ? <section className="grid">
        <aside className="sidebar">
          <Panel title="Days" icon={<CalendarDays size={18} />}>
            <div className="sourceRow">
              <select value={source} onChange={(e) => setSource(e.target.value as "orion" | "angelique")}>
                {sources.map((item) => (
                  <option key={item.key} value={item.key}>{item.label}</option>
                ))}
              </select>
              <button className="tool iconOnly" onClick={() => refreshCalendar()} aria-label="Refresh"><RefreshCcw size={16} /></button>
            </div>
            <div className="dayList">
              {days.map((day) => (
                <button
                  key={day.date}
                  className={day.date === selectedDate ? "day selected" : "day"}
                  onClick={() => setSelectedDate(day.date)}
                >
                  <strong>{day.date}</strong>
                  <span>
                    {sessionCountsByDate[day.date] == null
                      ? "valid"
                      : `${sessionCountsByDate[day.date]} session${sessionCountsByDate[day.date] === 1 ? "" : "s"}`}
                  </span>
                </button>
              ))}
              {!days.length ? <small className="muted">No days match the current threshold and minimum length.</small> : null}
            </div>
          </Panel>

          <Panel title="Sessions" icon={<Scissors size={18} />}>
            <div className="stack compact">
              {sourceRanges.length ? (
                <small className="muted">
                  Threshold sessions for selected local day
                </small>
              ) : null}
              <div className="sessionSummary">
                <strong>{previewSegments.length ? `${previewSegments.length} previewed` : "No preview selected"}</strong>
                <span>
                  {previewSummary
                    ? `${formatTime(previewSummary.startMs)} - ${formatDuration(previewSummary.durationS)} total`
                    : "Click a session row to load its graph, GPS, and metadata."}
                </span>
                {segments.length > 1 ? <button className="textButton" onClick={selectAllSessionsForPreview}>Preview all</button> : null}
              </div>
              {segments.map((segment) => (
                <div
                  key={segment.id}
                  className={previewSelectedSegments.has(segment.id) ? "sessionRow previewSelected" : "sessionRow"}
                  role="button"
                  tabIndex={0}
                  onClick={(event) => handleSessionClick(event, segment)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      setRange(null);
                      setPreviewSelectedSegments(new Set([segment.id]));
                      setLastPreviewSegmentId(segment.id);
                    }
                  }}
                >
                  <input
                    type="checkbox"
                    checked={selectedSegments.has(segment.id) && !range}
                    onClick={(event) => event.stopPropagation()}
                    onChange={() => toggleSegment(segment)}
                    aria-label={`Export ${segment.label}`}
                  />
                  <span>
                    <span className="sessionTitle">
                      <strong>{segment.label}</strong>
                      <SessionGpsBadge segment={segment} />
                    </span>
                    <small>{formatTime(segment.start_ms)} - {formatDuration(segment.duration_s)}</small>
                  </span>
                </div>
              ))}
              {!segments.length ? <small className="muted">No threshold sessions in this day.</small> : null}
            </div>
          </Panel>
        </aside>

        <section className="workspace">
          <div className="toolbar">
            <label className="channelPicker">
              <Gauge size={16} />
              <select value={channel} onChange={(e) => setChannel(e.target.value)}>
                {channels.map((c) => (
                  <option key={c.key} value={c.key}>{c.label}</option>
                ))}
              </select>
            </label>
            <label className="thresholdBox">
              <SlidersHorizontal size={16} />
              <span>Threshold</span>
              <input type="number" value={threshold} onChange={(e) => setThreshold(Number(e.target.value))} />
            </label>
            <label className="thresholdBox smallInput">
              <span>Min Length</span>
              <input type="number" min={0} step={1} value={minDurationS} onChange={(e) => setMinDurationS(Math.max(0, Number(e.target.value)))} />
              <span>s</span>
            </label>
            <label className="thresholdBox exportTypeBox">
              <span>Export</span>
              <select value={exportType} onChange={(e) => setExportType(e.target.value as "motec" | "csv")}>
                <option value="motec">MoTeC</option>
                <option value="csv">CSV</option>
              </select>
            </label>
            <label className="thresholdBox trackPicker">
              <Flag size={16} />
              <select
                value={track.slug}
                onChange={(e) => setTrack(normalizeTrack(tracks.find((t) => t.slug === e.target.value) ?? track))}
              >
                <option value={track.slug}>{track.name}</option>
                {tracks.filter((t) => t.slug !== track.slug).map((t) => (
                  <option key={t.slug} value={t.slug}>{t.name}</option>
                ))}
              </select>
            </label>
            <button className="primary" disabled={!exportSegments.length || busy} onClick={exportSelection}>
              <Download size={16} /> Export {exportType === "csv" ? "CSV" : "MoTeC"} {range ? "range" : exportSegments.length || ""}
            </button>
            {exportUrl ? <a className="download" href={exportUrl}>Download ZIP</a> : null}
          </div>

          <Panel title={selectedChannel?.label || "Channel"} icon={<Gauge size={18} />}>
            <TelemetryChart
              points={series}
              unit={selectedChannel?.unit || ""}
              range={range}
              segments={segments}
              previewSegmentIds={previewSelectedSegments}
              threshold={threshold}
              onRange={setRange}
            />
            <div className="rangeBar">
              <span>{range ? `${formatDateTime(range[0])} to ${formatDateTime(range[1])}` : "The plot follows clicked sessions. Drag across it only when you need a one-off manual export range."}</span>
              {range ? <button className="tool" onClick={() => setRange(null)}>Use Checked Sessions</button> : null}
            </div>
          </Panel>

          <div className="lower">
            <Panel title="GPS Trace" icon={<MapPinned size={18} />} className="gpsPanel">
              <GpsTrace
                points={gps}
                gates={track.gates}
                drawMode={null}
                onDrawGate={handleDrawGate}
                nextSplitNumber={track.gates.filter((gate) => gate.role === "split").length + 1}
              />
              <LapPreview rows={lapPreview} hasStartFinish={hasStartFinish} gpsPointCount={gps.length} />
            </Panel>

            <div className="rightRail">
              <Panel title="MoTeC Metadata" icon={<FileText size={18} />}>
                <div className="metadataStatus">
                  <strong>{previewSegments.length ? `${previewSegments.length} selected` : "No session selected"}</strong>
                  <span>{mixedMetadata.size ? `${mixedMetadata.size} mixed fields` : "Fields match across selection"}</span>
                </div>
                <div className="metadataGrid">
                  {METADATA_FIELDS.map((key) => (
                    <label key={key}>
                      <span>{key.replace("_", " ")}</span>
                      {key === "long_comment" ? (
                        <textarea
                          value={metadataDraft[key]}
                          placeholder={mixedMetadata.has(key) ? "Mixed values" : ""}
                          className={mixedMetadata.has(key) ? "mixedField" : ""}
                          onChange={(e) => setMetadataDraft((prev) => ({ ...prev, [key]: e.target.value }))}
                        />
                      ) : (
                        <input
                          value={metadataDraft[key]}
                          placeholder={mixedMetadata.has(key) ? "Mixed values" : ""}
                          className={mixedMetadata.has(key) ? "mixedField" : ""}
                          onChange={(e) => setMetadataDraft((prev) => ({ ...prev, [key]: e.target.value }))}
                        />
                      )}
                    </label>
                  ))}
                </div>
                <button className="primary fullWidth" disabled={!previewSegments.length} onClick={applyMetadata}>
                  <Save size={16} /> Apply Metadata
                </button>
              </Panel>

              <Panel title="Channel Chart" icon={<Gauge size={18} />}>
                <div className="trackForm">
                  <input value={channelChart.name} onChange={(e) => setChannelChart({ ...channelChart, name: e.target.value })} />
                  <select
                    value={channelChart.slug}
                    onChange={(e) => setChannelChart(channelCharts.find((chart) => chart.slug === e.target.value) ?? channelChart)}
                  >
                    <option value={channelChart.slug}>{channelChart.name}</option>
                    {channelCharts.filter((chart) => chart.slug !== channelChart.slug).map((chart) => (
                      <option key={chart.slug} value={chart.slug}>{chart.name}</option>
                    ))}
                  </select>
                  <textarea value={channelChart.notes} placeholder="Chart notes" onChange={(e) => setChannelChart({ ...channelChart, notes: e.target.value })} />
                </div>
                <div className="chartSummary">
                  <strong>{channelChart.entries.length}</strong>
                  <span>channel metadata rows</span>
                </div>
                <div className="gateButtons">
                  <button className="tool" onClick={() => saveChannelChart()}><Save size={15} /> Save</button>
                  <label className="tool fileTool">
                    <Upload size={15} /> Upload
                    <input type="file" accept="application/json,.json,text/csv,.csv" onChange={uploadChannelChart} />
                  </label>
                  <button className="tool" onClick={downloadChannelChart}><Download size={15} /> Download</button>
                </div>
              </Panel>

            </div>
          </div>
        </section>
      </section> : null}

      {activeTab === "live" ? (
        <section className="liveShell">
          <div className="liveHero">
            <div>
              <span className={liveBadgeClass}><Radio size={14} /> {liveBadgeLabel}</span>
              <h2>{liveState.lapStartMs ? formatLapTime(liveLapElapsedMs) : "0:00.00"}</h2>
              <p>{liveState.lapStartMs ? "Flying lap" : "Out lap / waiting for start"}</p>
              <DeltaBar rate={liveState.deltaRate} totalMs={liveState.deltaMs} />
            </div>
            <div className="liveHeroStats">
              <Metric label="Speed" value={filteredLiveState.lastSample?.speed == null ? "--" : formatSpeed(filteredLiveState.lastSample.speed)} />
              <Metric label="Energy" value={`${liveState.totalEnergyWh.toFixed(1)} Wh`} />
              <Metric label="Best" value={bestLap ? formatLapTime(bestLap.durationMs) : "--"} tone="purple" />
              <Metric label="Topic" value={liveState.topic || (liveTransport === "kafka" ? "Kafka broker" : "Local replay bus")} />
            </div>
            <div className="liveControls">
              <button className={liveState.running ? "primary dangerPrimary" : "primary"} onClick={liveState.running ? stopLiveData : startLiveData}>
                {liveState.running ? <Activity size={16} /> : <Radio size={16} />} {liveState.running ? "Stop Live" : "Start Live"}
              </button>
              <button className="tool" disabled={!liveState.running || !liveState.lastSample} onClick={triggerManualLap}>
                <Flag size={15} /> {liveState.lapStartMs ? "Log Lap" : "Start Lap"}
              </button>
              <label className="checkInline">
                <input type="checkbox" checked={autoLiveDownload} onChange={(e) => setAutoLiveDownload(e.target.checked)} />
                Auto MoTeC on lap
              </label>
              <small className="muted">{liveState.status}</small>
            </div>
          </div>

          <div className="liveLayout">
            <div className="leftRail">
              <Panel title="Live Laps" icon={<Timer size={18} />}>
                <LiveLapTable
                  laps={liveState.laps}
                  bestLap={bestLap}
                  bestSectors={bestSectors}
                  currentSectors={liveState.currentSectors}
                  currentLapElapsedMs={liveLapElapsedMs}
                  currentLapEnergyWh={liveState.lapEnergyWh}
                  sectorCount={hasStartFinish && splitGates.length ? splitGates.length + 1 : 0}
                />
              </Panel>
            </div>
            <div className="liveMainColumn">
              <Panel title="Live Position" icon={<MapPinned size={18} />} className="liveMapPanel">
                <TrackBuilderMap
                  points={filteredLiveState.samples.filter(hasGps).map((sample) => ({ t: sample.t, lat: sample.lat ?? 0, lon: sample.lon ?? 0 }))}
                  liveSample={filteredLiveState.lastSample}
                  gates={track.gates}
                  drawMode={null}
                  onDrawGate={handleDrawGate}
                  center={builderCenter}
                  onCenter={setBuilderCenter}
                  targetSpanM={selectedTrackView?.spanM}
                />
              </Panel>
              <Panel title="Energy Window" icon={<Zap size={18} />}>
                <EnergyWindowChart
                  state={filteredLiveState}
                  windowS={energyWindowS}
                  onWindowS={setEnergyWindowS}
                />
              </Panel>
              <Panel title="Temperature Window" icon={<Thermometer size={18} />}>
                <TemperatureWindowChart
                  state={filteredLiveState}
                  windowS={energyWindowS}
                />
              </Panel>
            </div>
            <div className="rightRail">
              <PackStatusPanel state={filteredLiveState} />
              <DriverControlsPanel sample={filteredLiveState.lastSample} />
              <TempsStatusPanel sample={filteredLiveState.lastSample} />
              <Panel title="Live Data" icon={<Gauge size={18} />}>
                <LiveDataPanel state={filteredLiveState} />
              </Panel>
              <Panel title="Live Setup" icon={<SlidersHorizontal size={18} />}>
                <div className="trackForm">
                  <label>
                    <span>Car</span>
                    <select
                      value={source}
                      disabled={liveState.running}
                      onChange={(e) => setSource(e.target.value as "orion" | "angelique")}
                    >
                      {sources.map((item) => <option key={item.key} value={item.key}>{item.label}</option>)}
                    </select>
                  </label>
                  <label>
                    <span>Topic source</span>
                    <select value={liveTransport} disabled={liveState.running} onChange={(e) => setLiveTransport(e.target.value as KafkaTransport)}>
                      <option value="local">Local replay bus (simulator)</option>
                      <option value="kafka">Kafka broker (car)</option>
                      <option value="mqtt">MQTT broker (car direct)</option>
                    </select>
                  </label>
                  <label>
                    <span>Topic</span>
                    <input value={liveTopic} placeholder={liveTransport === "mqtt" ? "orion" : `grafana_data_${source}`} disabled={liveState.running} onChange={(e) => setLiveTopic(e.target.value)} />
                  </label>
                  <label>
                    <span>Sample rate</span>
                    <select value={liveSampleHz} disabled={liveState.running} onChange={(e) => setLiveSampleHz(Number(e.target.value))}>
                      <option value={1}>1 Hz</option>
                      <option value={2}>2 Hz</option>
                      <option value={5}>5 Hz</option>
                      <option value={10}>10 Hz</option>
                      <option value={20}>20 Hz</option>
                    </select>
                  </label>
                  <label>
                    <span>Track</span>
                    <select value={track.slug} onChange={(e) => setTrack(normalizeTrack(tracks.find((t) => t.slug === e.target.value) ?? track))}>
                      <option value={track.slug}>{track.name}</option>
                      {tracks.filter((t) => t.slug !== track.slug).map((t) => <option key={t.slug} value={t.slug}>{t.name}</option>)}
                    </select>
                  </label>
                  <label>
                    <span>Channel chart</span>
                    <select value={channelChart.slug} onChange={(e) => setChannelChart(channelCharts.find((chart) => chart.slug === e.target.value) ?? channelChart)}>
                      <option value={channelChart.slug}>{channelChart.name}</option>
                      {channelCharts.filter((chart) => chart.slug !== channelChart.slug).map((chart) => <option key={chart.slug} value={chart.slug}>{chart.name}</option>)}
                    </select>
                  </label>
                  <label>
                    <span>Session</span>
                    <input value={metadataDraft.session} placeholder="Session metadata" onChange={(e) => setMetadataDraft((prev) => ({ ...prev, session: e.target.value }))} />
                  </label>
                </div>
              </Panel>
              <Panel title="Pre-Set Metadata" icon={<FileText size={18} />}>
                <div className="metadataGrid compactMetadata">
                  {METADATA_FIELDS.map((key) => (
                    <label key={key}>
                      <span>{key.replace("_", " ")}</span>
                      {key === "long_comment" ? (
                        <textarea value={metadataDraft[key]} onChange={(e) => setMetadataDraft((prev) => ({ ...prev, [key]: e.target.value }))} />
                      ) : (
                        <input value={metadataDraft[key]} onChange={(e) => setMetadataDraft((prev) => ({ ...prev, [key]: e.target.value }))} />
                      )}
                    </label>
                  ))}
                </div>
              </Panel>
            </div>
          </div>
        </section>
      ) : null}

      {activeTab === "track-builder" ? (
        <section className="builderShell">
          <aside className="builderSidebar">
            <Panel title="Search Data" icon={<CalendarDays size={18} />}>
              <div className="sourceRow">
                <select value={source} onChange={(e) => setSource(e.target.value as "orion" | "angelique")}>
                  {sources.map((item) => <option key={item.key} value={item.key}>{item.label}</option>)}
                </select>
                <button className="tool iconOnly" onClick={() => refreshCalendar()} aria-label="Refresh"><RefreshCcw size={16} /></button>
              </div>
              <div className="builderControls">
                <select value={channel} onChange={(e) => setChannel(e.target.value)}>
                  {channels.map((c) => <option key={c.key} value={c.key}>{c.label}</option>)}
                </select>
                <input type="number" value={threshold} onChange={(e) => setThreshold(Number(e.target.value))} />
              </div>
              <div className="dayList builderDayList">
                {days.map((day) => (
                  <button key={day.date} className={day.date === selectedDate ? "day selected" : "day"} onClick={() => setSelectedDate(day.date)}>
                    <strong>{day.date}</strong>
                    <span>{sessionCountsByDate[day.date] ?? day.sessions} sessions</span>
                  </button>
                ))}
              </div>
            </Panel>
            <Panel title="Reference Sessions" icon={<Scissors size={18} />}>
              <div className="stack compact builderSessionList">
                {segments.map((segment) => (
                  <div key={segment.id} className={previewSelectedSegments.has(segment.id) ? "sessionRow previewSelected" : "sessionRow"} role="button" tabIndex={0} onClick={(event) => handleSessionClick(event, segment)}>
                    <input type="checkbox" checked={previewSelectedSegments.has(segment.id)} readOnly />
                    <span>
                      <span className="sessionTitle">
                        <strong>{segment.label}</strong>
                        <SessionGpsBadge segment={segment} />
                      </span>
                      <small>{formatTime(segment.start_ms)} - {formatDuration(segment.duration_s)}</small>
                    </span>
                  </div>
                ))}
                {!segments.length ? <small className="muted">Load a day to use historic GPS as a drawing reference.</small> : null}
              </div>
            </Panel>
          </aside>

          <Panel title="Track Map" icon={<MapPinned size={18} />} className="builderMapPanel">
            <div className="mapToolbar">
              <input value={builderSearch} onChange={(e) => setBuilderSearch(e.target.value)} placeholder="lat, lon" />
              <button className="tool" onClick={() => {
                const parsed = parseLatLon(builderSearch);
                if (parsed) setBuilderCenter(parsed);
              }}><Target size={15} /> Center</button>
              <button className="tool" onClick={() => {
                navigator.geolocation?.getCurrentPosition((position) => {
                  setBuilderCenter({ lat: position.coords.latitude, lon: position.coords.longitude });
                  setBuilderSearch(`${position.coords.latitude.toFixed(6)}, ${position.coords.longitude.toFixed(6)}`);
                }, (geoError) => setError(geoError.message));
              }}><MapPinned size={15} /> My Location</button>
            </div>
            <TrackBuilderMap
              points={gps}
              liveSample={null}
              gates={track.gates}
              drawMode={gateDrawMode}
              onDrawGate={handleDrawGate}
              center={builderCenter}
              onCenter={setBuilderCenter}
              targetSpanM={selectedTrackView?.spanM}
            />
          </Panel>

          <div className="rightRail">
            <TrackManagerPanel
              track={track}
              tracks={tracks}
              hasStartFinish={hasStartFinish}
              gateDrawMode={gateDrawMode}
              onSetTrack={setTrack}
              onNewTrack={newTrack}
              onSetGateDrawMode={setGateDrawMode}
              onSaveTrack={saveTrack}
              onUploadTrack={uploadTrack}
              onDownloadTrack={downloadTrack}
              onUpdateGate={updateGate}
              onMoveSplitGate={moveSplitGate}
              onRemoveGate={removeGate}
            />
          </div>
        </section>
      ) : null}

      {activeTab === "race-ops" ? (
        <section className="opsGrid">
          <Panel title="Run Plan" icon={<NotebookText size={18} />}>
            <div className="opsCards">
              <Metric label="Car" value={sourceLabel} />
              <Metric label="Track" value={track.name} />
              <Metric label="Channel Chart" value={channelChart.name} />
              <Metric label="Selected Sessions" value={`${selectedSegments.size}`} />
            </div>
            <div className="opsChecklist">
              {["Track gates saved", "Metadata prefilled", "Channel chart selected", "Live auto-download decision made", "Exporter source verified"].map((item) => (
                <label key={item} className="checkInline"><input type="checkbox" /> {item}</label>
              ))}
            </div>
          </Panel>
          <Panel title="Energy Brief" icon={<Zap size={18} />}>
            <div className="opsCards">
              <Metric label="Live Used" value={`${liveState.totalEnergyWh.toFixed(1)} Wh`} />
              <Metric label="Last Lap" value={liveState.laps.at(-1) ? `${liveState.laps.at(-1)!.energyWh.toFixed(1)} Wh` : "--"} />
              <Metric label="Best Lap" value={bestLap ? formatLapTime(bestLap.durationMs) : "--"} tone="purple" />
            </div>
          </Panel>
          <Panel title="Car Presets" icon={<SlidersHorizontal size={18} />} className="carPresetPanel">
            <div className="presetToolbar">
              <select
                value={selectedCarPresetId}
                onChange={(e) => {
                  const preset = carPresets.find((item) => item.id === e.target.value);
                  if (!preset) return;
                  setSelectedCarPresetId(preset.id);
                  setCarPresetDraft(preset);
                }}
              >
                {carPresets.map((preset) => <option key={preset.id} value={preset.id}>{preset.name}</option>)}
              </select>
              <button className="tool" onClick={() => applyCarPreset()}><Target size={15} /> Apply</button>
              <button className="tool" onClick={newCarPreset}><Plus size={15} /> New</button>
              <button className="tool dangerTool" onClick={deleteCarPreset}><Trash2 size={15} /> Delete</button>
            </div>
            <div className="presetGrid">
              <label>
                <span>Name</span>
                <input value={carPresetDraft.name} onChange={(e) => setCarPresetDraft((prev) => ({ ...prev, name: e.target.value }))} />
              </label>
              <label>
                <span>Source</span>
                <select value={carPresetDraft.source} onChange={(e) => setCarPresetDraft((prev) => ({ ...prev, source: e.target.value as "orion" | "angelique" }))}>
                  {sources.map((item) => <option key={item.key} value={item.key}>{item.label}</option>)}
                </select>
              </label>
              <label>
                <span>Topic</span>
                <input value={carPresetDraft.topic} onChange={(e) => setCarPresetDraft((prev) => ({ ...prev, topic: e.target.value }))} />
              </label>
              <label>
                <span>Topic source</span>
                <select value={carPresetDraft.transport} onChange={(e) => setCarPresetDraft((prev) => ({ ...prev, transport: e.target.value as KafkaTransport }))}>
                  <option value="local">Local replay bus</option>
                  <option value="kafka">Kafka broker</option>
                </select>
              </label>
              <label>
                <span>Track</span>
                <select value={carPresetDraft.trackSlug} onChange={(e) => setCarPresetDraft((prev) => ({ ...prev, trackSlug: e.target.value }))}>
                  <option value={track.slug}>{track.name}</option>
                  {tracks.filter((item) => item.slug !== track.slug).map((item) => <option key={item.slug} value={item.slug}>{item.name}</option>)}
                </select>
              </label>
              <label>
                <span>Channel chart</span>
                <select value={carPresetDraft.channelChartSlug} onChange={(e) => setCarPresetDraft((prev) => ({ ...prev, channelChartSlug: e.target.value }))}>
                  <option value={channelChart.slug}>{channelChart.name}</option>
                  {channelCharts.filter((item) => item.slug !== channelChart.slug).map((item) => <option key={item.slug} value={item.slug}>{item.name}</option>)}
                </select>
              </label>
            </div>
            <div className="metadataGrid compactMetadata presetMetadata">
              {METADATA_FIELDS.map((key) => (
                <label key={key}>
                  <span>{key.replace("_", " ")}</span>
                  {key === "long_comment" ? (
                    <textarea
                      value={carPresetDraft.metadata[key]}
                      onChange={(e) => setCarPresetDraft((prev) => ({ ...prev, metadata: { ...prev.metadata, [key]: e.target.value } }))}
                    />
                  ) : (
                    <input
                      value={carPresetDraft.metadata[key]}
                      onChange={(e) => setCarPresetDraft((prev) => ({ ...prev, metadata: { ...prev.metadata, [key]: e.target.value } }))}
                    />
                  )}
                </label>
              ))}
            </div>
            <div className="gateButtons">
              <button className="primary" onClick={saveCarPreset}><Save size={15} /> Save Preset</button>
            </div>
          </Panel>
        </section>
      ) : null}
    </main>
  );
}

function Panel({ title, icon, children, className = "" }: { title: string; icon: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={className ? `panel ${className}` : "panel"}>
      <div className="panelTitle">{icon}<h2>{title}</h2></div>
      {children}
    </section>
  );
}

function Metric({ label, value, tone = "" }: { label: string; value: string; tone?: "purple" | "" }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong className={tone === "purple" ? "purpleText" : ""}>{value}</strong>
    </div>
  );
}

function DeltaBar({ rate, totalMs }: { rate: number | null; totalMs: number }) {
  const bounded = Math.max(-1.5, Math.min(1.5, rate ?? 0));
  const width = Math.min(50, Math.abs(bounded) / 1.5 * 50);
  const gaining = bounded < 0;
  const style = gaining
    ? { left: `${50 - width}%`, width: `${width}%` }
    : { left: "50%", width: `${width}%` };
  return (
    <div className="deltaBox">
      <div className="deltaReadout">
        <strong>{rate == null ? "--" : `${rate >= 0 ? "+" : ""}${rate.toFixed(3)} s/s`}</strong>
        <span>{formatSignedSeconds(totalMs)}</span>
      </div>
      <div className="deltaTrack">
        <span className="deltaZero" />
        <span className={gaining ? "deltaFill gaining" : "deltaFill losing"} style={style} />
      </div>
    </div>
  );
}

function PackStatusPanel({ state }: { state: LiveSessionState }) {
  const pack = packStatus(state.samples);
  const sample = state.lastSample;
  return (
    <Panel title="Pack Status" icon={<Zap size={18} />}>
      <div className="packStatus">
        <div className="socHeader">
          <span>{pack.socSource === "ocv" ? "SOC Est" : "SOC Est"}</span>
          <strong>{pack.socPercent == null ? "--" : `${pack.socPercent.toFixed(0)}%`}</strong>
        </div>
        <div className="socBar" aria-label="Pack state of charge">
          <span style={{ width: `${pack.socPercent ?? 0}%` }} />
        </div>
        <small className="muted">
          {pack.socSource === "ocv"
            ? `OCV estimate from low-current voltage (${pack.ocvVoltage?.toFixed(1)} V).`
            : "Waiting for low-current voltage to estimate OCV SOC."}
        </small>
        <div className="packMetricGrid">
          <Metric label="Voltage" value={pack.voltage == null ? "--" : `${pack.voltage.toFixed(1)} V`} />
          <Metric label="Current" value={pack.current == null ? "--" : `${pack.current.toFixed(1)} A`} />
          <Metric label="Power" value={sample?.power_kw == null ? "--" : `${sample.power_kw.toFixed(2)} kW`} />
        </div>
      </div>
    </Panel>
  );
}

function DriverControlsPanel({ sample }: { sample: LiveSample | null }) {
  const controls = driverControls(sample);
  return (
    <Panel title="Driver Controls" icon={<Disc3 size={18} />}>
      <div className="driverControls">
        <div className="steeringReadout">
          <Disc3
            size={82}
            strokeWidth={1.7}
            style={{ transform: `rotate(${controls.steeringAngleDeg ?? 0}deg)` }}
            aria-label="Steering angle"
          />
          <strong>{controls.steeringAngleDeg == null ? "--" : `${controls.steeringAngleDeg.toFixed(1)} deg`}</strong>
        </div>
        <div className="pedalBars">
          <VerticalControlBar label="Accel" value={controls.throttlePercent} max={100} unit="%" tone="throttle" />
          <VerticalControlBar label="BSE 1" value={controls.bse1Psi} max={3000} unit="psi" tone="brake" />
          <VerticalControlBar label="BSE 2" value={controls.bse2Psi} max={3000} unit="psi" tone="brake" />
        </div>
      </div>
    </Panel>
  );
}

function VerticalControlBar({
  label,
  value,
  max,
  unit,
  tone,
}: {
  label: string;
  value: number | null;
  max: number;
  unit: string;
  tone: "throttle" | "brake";
}) {
  const percent = value == null ? 0 : clamp(value / max * 100, 0, 100);
  const display = value == null
    ? "--"
    : unit === "%"
      ? `${value.toFixed(0)}%`
      : `${Math.round(value).toLocaleString()} psi`;
  return (
    <div className="controlBar">
      <span>{label}</span>
      <div className={`controlTrack ${tone}`}>
        <span style={{ height: `${percent}%` }} />
      </div>
      <strong>{display}</strong>
    </div>
  );
}

function TempsStatusPanel({ sample }: { sample: LiveSample | null }) {
  const temps = tempStatus(sample);
  return (
    <Panel title="Temps" icon={<Thermometer size={18} />}>
      <div className="tempMetricGrid">
        <Metric label="Ambient" value={temps.ambient == null ? "--" : `${temps.ambient.toFixed(1)} C`} />
        <Metric label="Coolant" value={temps.coolant == null ? "--" : `${temps.coolant.toFixed(1)} C`} />
        <Metric label="Fan RPM" value={temps.fanRpm == null ? "--" : `${Math.round(temps.fanRpm).toLocaleString()} rpm`} />
        <Metric label="Motor Temp" value={temps.motor == null ? "--" : `${temps.motor.toFixed(1)} C`} />
        <Metric label="Inverter Temp" value={temps.inverter == null ? "--" : `${temps.inverter.toFixed(1)} C`} />
      </div>
    </Panel>
  );
}

function EnergyWindowChart({
  state,
  windowS,
  onWindowS,
}: {
  state: LiveSessionState;
  windowS: number;
  onWindowS: Dispatch<SetStateAction<number>>;
}) {
  const trace = energyTrace(state.samples, windowS);
  const width = 900;
  const height = 160;
  const padLeft = 46;
  const padRight = 18;
  const padTop = 14;
  const padBottom = 28;
  const plotW = width - padLeft - padRight;
  const plotH = height - padTop - padBottom;
  const lastT = state.lastSample?.t ?? Date.now();
  const startT = lastT - windowS * 1000;
  const maxEnergy = Math.max(1, ...trace.map((point) => point.energyWh));
  const points = trace
    .map((point) => {
      const x = padLeft + Math.max(0, Math.min(1, (point.t - startT) / (windowS * 1000))) * plotW;
      const y = padTop + (1 - Math.max(0, Math.min(1, point.energyWh / maxEnergy))) * plotH;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  const lapBreaks = state.laps.filter((lap) => lap.endMs >= startT && lap.endMs <= lastT);
  const latestEnergy = trace.at(-1)?.energyWh ?? 0;
  return (
    <div className="energyWindow">
      <div className="energyToolbar">
        <Metric label="Window Energy" value={`${latestEnergy.toFixed(1)} Wh`} />
        <label>
          <span>Window</span>
          <select value={windowS} onChange={(event) => onWindowS(Number(event.target.value))}>
            <option value={10}>10 s</option>
            <option value={30}>30 s</option>
            <option value={60}>1 min</option>
            <option value={120}>2 min</option>
            <option value={300}>5 min</option>
          </select>
        </label>
      </div>
      <svg className="energyChart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Energy over selected time window">
        <rect x={padLeft} y={padTop} width={plotW} height={plotH} rx="6" />
        <line x1={padLeft} x2={width - padRight} y1={height - padBottom} y2={height - padBottom} />
        <line x1={padLeft} x2={padLeft} y1={padTop} y2={height - padBottom} />
        <text x={padLeft} y={height - 7}>-{windowS}s</text>
        <text x={width - padRight - 28} y={height - 7}>now</text>
        <text x={6} y={padTop + 10}>{maxEnergy.toFixed(1)} Wh</text>
        <text x={10} y={height - padBottom}>0 Wh</text>
        {points ? <polyline points={points} /> : null}
        {lapBreaks.map((lap) => {
          const x = padLeft + Math.max(0, Math.min(1, (lap.endMs - startT) / (windowS * 1000))) * plotW;
          return (
            <g key={lap.id} className="lapBreak">
              <line x1={x} x2={x} y1={padTop} y2={height - padBottom} />
              <text x={x + 4} y={padTop + 13}>{lap.label}</text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function TemperatureWindowChart({ state, windowS }: { state: LiveSessionState; windowS: number }) {
  const series = temperatureSeries(state.samples, windowS);
  const width = 900;
  const height = 160;
  const padLeft = 46;
  const padRight = 18;
  const padTop = 14;
  const padBottom = 28;
  const plotW = width - padLeft - padRight;
  const plotH = height - padTop - padBottom;
  const lastT = state.lastSample?.t ?? Date.now();
  const startT = lastT - windowS * 1000;
  const allValues = series.flatMap((item) => item.segments.flatMap((segment) => segment.map((point) => point.value)));
  const minTemp = allValues.length ? Math.floor(Math.min(...allValues) / 5) * 5 : 0;
  const maxTemp = allValues.length ? Math.ceil(Math.max(...allValues) / 5) * 5 : 100;
  const span = Math.max(10, maxTemp - minTemp);
  const yFor = (value: number) => padTop + (1 - clamp((value - minTemp) / span, 0, 1)) * plotH;
  const xFor = (t: number) => padLeft + clamp((t - startT) / (windowS * 1000), 0, 1) * plotW;
  return (
    <div className="energyWindow">
      <div className="tempLegend">
        {series.map((item) => (
          <span key={item.key}>
            <i style={{ background: item.color }} />
            {item.label}
          </span>
        ))}
      </div>
      <svg className="energyChart tempChart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Temperatures over selected time window">
        <rect x={padLeft} y={padTop} width={plotW} height={plotH} rx="6" />
        <line x1={padLeft} x2={width - padRight} y1={height - padBottom} y2={height - padBottom} />
        <line x1={padLeft} x2={padLeft} y1={padTop} y2={height - padBottom} />
        <text x={padLeft} y={height - 7}>-{windowS}s</text>
        <text x={width - padRight - 28} y={height - 7}>now</text>
        <text x={7} y={padTop + 10}>{maxTemp.toFixed(0)} C</text>
        <text x={7} y={height - padBottom}>{minTemp.toFixed(0)} C</text>
        {series.flatMap((item) => item.segments.map((segment, index) => {
          const points = segment.map((point) => `${xFor(point.t).toFixed(1)},${yFor(point.value).toFixed(1)}`).join(" ");
          return points ? <polyline key={`${item.key}-${index}`} points={points} style={{ stroke: item.color }} /> : null;
        }))}
      </svg>
    </div>
  );
}

function LiveDataPanel({ state }: { state: LiveSessionState }) {
  const sample = state.lastSample;
  const values = sample?.values ?? {};
  const dcBusV = sample?.values.dc_bus_v ?? sample?.values.bus_voltage ?? sample?.values.pack_dc_bus_v ?? sample?.hv_pack_v ?? null;
  const dcBusCurrent = sample?.values.dc_bus_current ?? sample?.values.pack_dc_bus_current ?? sample?.hv_c ?? null;
  const motorRpm = firstLiveValue(values, ["controls_motor_speed", "motor_speed", "dynamics_inverter_rpm", "inverter_rpm"]);
  const rawRows = topLiveValues(values);
  return (
    <div className="liveDataPanel">
      <div className="liveDataGrid">
        <Metric label="Samples" value={state.samples.length.toLocaleString()} />
        <Metric label="Last Sample" value={sample ? formatTime(sample.t) : "--"} />
        <Metric label="Power" value={sample?.power_kw == null ? "--" : `${sample.power_kw.toFixed(2)} kW`} />
        <Metric label="DC Bus" value={dcBusV == null || dcBusCurrent == null ? "--" : `${dcBusV.toFixed(1)} V / ${dcBusCurrent.toFixed(1)} A`} />
        <Metric label="Motor RPM" value={motorRpm == null ? "--" : `${Math.round(motorRpm).toLocaleString()} rpm`} />
        <Metric label="Raw Channels" value={Object.keys(values).length.toLocaleString()} />
      </div>
      {sample ? (
        <div className="rawValueList">
          {rawRows.map(([key, value]) => (
            <div key={key} className="rawValueRow">
              <span>{labelFromKey(key)}</span>
              <strong>{formatRawLiveValue(key, value)}</strong>
            </div>
          ))}
          {!rawRows.length ? <small className="muted">Sample arrived, but it did not contain numeric channels.</small> : null}
        </div>
      ) : (
        <small className="muted">
          {state.connected
            ? state.status.includes("MQTT")
              ? "MQTT link is up. No car samples have arrived on this topic yet."
              : "Broker link is up. No car samples have arrived on this topic yet."
            : "Start Live to watch broker samples and raw channels."}
        </small>
      )}
    </div>
  );
}

function LiveLapTable({
  laps,
  bestLap,
  bestSectors,
  currentSectors,
  currentLapElapsedMs,
  currentLapEnergyWh,
  sectorCount,
}: {
  laps: LiveLap[];
  bestLap: LiveLap | null;
  bestSectors: Array<number | null>;
  currentSectors: number[];
  currentLapElapsedMs: number;
  currentLapEnergyWh: number;
  sectorCount: number;
}) {
  const columns = sectorCount;
  return (
    <div className="lapTableWrap">
      <table className="lapTable">
        <thead>
          <tr>
            <th>Lap</th>
            <th>Time</th>
            {Array.from({ length: columns }, (_sector, index) => <th key={`sector-head-${index}`}>S{index + 1}</th>)}
            <th>Energy</th>
          </tr>
        </thead>
        <tbody>
          {currentLapElapsedMs > 0 ? (
            <tr className="currentLapRow">
              <td>Current</td>
              <td>{formatLapTime(currentLapElapsedMs)}</td>
              {Array.from({ length: columns }, (_unused, index) => (
                <td key={`current-sector-${index}`}>{currentSectors[index] == null ? "--" : formatLapTime(currentSectors[index])}</td>
              ))}
              <td>{currentLapEnergyWh.toFixed(1)} Wh</td>
            </tr>
          ) : null}
          {laps.map((lap) => (
            <tr key={lap.id}>
              <td className={bestLap?.id === lap.id ? "purpleText" : ""}>{lap.label}</td>
              <td className={bestLap?.id === lap.id ? "purpleText" : ""}>{formatLapTime(lap.durationMs)}</td>
              {Array.from({ length: columns }, (_unused, index) => {
                const bestSector = bestSectors[index];
                return (
                <td key={`${lap.id}-sector-${index}`} className={bestSector != null && lap.sectors[index] === bestSector ? "purpleText" : ""}>
                  {lap.sectors[index] == null ? "--" : formatLapTime(lap.sectors[index])}
                </td>
                );
              })}
              <td>{lap.energyWh.toFixed(1)} Wh</td>
            </tr>
          ))}
          {!laps.length ? (
            <tr>
              <td colSpan={Math.max(4, columns + 3)}>No completed flying laps yet. Out lap and in lap are excluded from best lap.</td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}

function TrackManagerPanel({
  track,
  tracks,
  hasStartFinish,
  gateDrawMode,
  onSetTrack,
  onNewTrack,
  onSetGateDrawMode,
  onSaveTrack,
  onUploadTrack,
  onDownloadTrack,
  onUpdateGate,
  onMoveSplitGate,
  onRemoveGate,
}: {
  track: TrackDefinition;
  tracks: TrackDefinition[];
  hasStartFinish: boolean;
  gateDrawMode: GateDrawMode;
  onSetTrack: Dispatch<SetStateAction<TrackDefinition>>;
  onNewTrack: () => void;
  onSetGateDrawMode: Dispatch<SetStateAction<GateDrawMode>>;
  onSaveTrack: () => void;
  onUploadTrack: (event: ChangeEvent<HTMLInputElement>) => void;
  onDownloadTrack: () => void;
  onUpdateGate: (index: number, patch: Partial<GateLine>) => void;
  onMoveSplitGate: (index: number, direction: -1 | 1) => void;
  onRemoveGate: (index: number) => void;
}) {
  return (
    <Panel title="Track Split JSON" icon={<Flag size={18} />}>
      <div className="trackForm">
        <input value={track.name} onChange={(e) => onSetTrack({ ...track, name: e.target.value })} />
        <select
          value={track.slug}
          onChange={(e) => onSetTrack(normalizeTrack(tracks.find((t) => t.slug === e.target.value) ?? track))}
        >
          <option value={track.slug}>{track.name}</option>
          {tracks.filter((t) => t.slug !== track.slug).map((t) => (
            <option key={t.slug} value={t.slug}>{t.name}</option>
          ))}
        </select>
        <textarea value={track.notes} placeholder="Track notes" onChange={(e) => onSetTrack({ ...track, notes: e.target.value })} />
      </div>
      <div className="gateButtons">
        <button className="tool" onClick={onNewTrack}><Plus size={15} /> New</button>
        <button
          className={gateDrawMode === "start_finish" ? "tool activeTool" : "tool"}
          disabled={hasStartFinish}
          onClick={() => onSetGateDrawMode((mode) => (mode === "start_finish" ? null : "start_finish"))}
        >
          <Plus size={15} /> Start
        </button>
        <button
          className={gateDrawMode === "split" ? "tool activeTool" : "tool"}
          onClick={() => onSetGateDrawMode((mode) => (mode === "split" ? null : "split"))}
        >
          <Plus size={15} /> Split
        </button>
        <button className="tool" onClick={onSaveTrack}><Save size={15} /> Save</button>
        <label className="tool fileTool">
          <Upload size={15} /> Upload
          <input type="file" accept="application/json,.json" onChange={onUploadTrack} />
        </label>
        <button className="tool" onClick={onDownloadTrack}><Download size={15} /> Download</button>
      </div>
      <div className="gateList">
        <small className="muted">
          {gateDrawMode ? "Drag a line across the GPS map to place the selected gate." : "Select Start or Split, then draw the gate on the GPS map."}
        </small>
        {track.gates.map((gate, index) => {
          const splitIndexes = track.gates.flatMap((item, itemIndex) => (item.role === "split" ? [itemIndex] : []));
          const splitPosition = splitIndexes.indexOf(index);
          return (
            <div key={gate.id} className="gate gateEditor">
              <div className="gateMain">
                <input
                  className="gateName"
                  value={gate.label}
                  aria-label={`${gate.role.replace("_", " ")} label`}
                  onChange={(e) => onUpdateGate(index, { label: e.target.value })}
                />
                <span className={gate.role === "start_finish" ? "rolePill startRole" : "rolePill"}>{gate.role === "start_finish" ? "Start Finish" : "Split"}</span>
              </div>
              <div className="gateActions">
                {gate.role === "split" ? (
                  <>
                    <button className="miniTool" disabled={splitPosition <= 0} onClick={() => onMoveSplitGate(index, -1)} aria-label={`Move ${gate.label} up`}>
                      <ArrowUp size={14} />
                    </button>
                    <button className="miniTool" disabled={splitPosition < 0 || splitPosition >= splitIndexes.length - 1} onClick={() => onMoveSplitGate(index, 1)} aria-label={`Move ${gate.label} down`}>
                      <ArrowDown size={14} />
                    </button>
                  </>
                ) : null}
                <button className="miniTool dangerTool" onClick={() => onRemoveGate(index)} aria-label={`Remove ${gate.label}`}>
                  <Trash2 size={14} />
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </Panel>
  );
}

function TelemetryChart({
  points,
  unit,
  range,
  segments,
  previewSegmentIds,
  threshold,
  onRange,
}: {
  points: SeriesPoint[];
  unit: string;
  range: [number, number] | null;
  segments: SegmentSummary[];
  previewSegmentIds: Set<string>;
  threshold: number;
  onRange: (range: [number, number] | null) => void;
}) {
  const [dragStart, setDragStart] = useState<number | null>(null);
  const width = 1100;
  const height = 330;
  const pad = 42;
  const finite = points.filter((p) => typeof p.v === "number") as Array<{ t: number; v: number }>;
  const minT = finite[0]?.t ?? 0;
  const maxT = finite.at(-1)?.t ?? minT + 1;
  const values = finite.map((p) => p.v).concat([threshold]);
  const minV = values.length ? Math.min(...values) : 0;
  const maxV = values.length ? Math.max(...values) : 1;
  const x = (t: number) => pad + ((t - minT) / Math.max(1, maxT - minT)) * (width - pad * 2);
  const y = (v: number) => height - pad - ((v - minV) / Math.max(1, maxV - minV)) * (height - pad * 2);
  const toT = (clientX: number, rect: DOMRect) => {
    const local = Math.max(pad, Math.min(width - pad, ((clientX - rect.left) / rect.width) * width));
    return Math.round(minT + ((local - pad) / (width - pad * 2)) * (maxT - minT));
  };
  const line = finite.map((p) => `${x(p.t)},${y(p.v)}`).join(" ");
  const xTicks = Array.from({ length: 6 }, (_, i) => minT + ((maxT - minT) * i) / 5);
  const yTicks = Array.from({ length: 5 }, (_, i) => minV + ((maxV - minV) * i) / 4);
  const visibleSegments = segments.filter((segment) => segment.end_ms >= minT && segment.start_ms <= maxT);

  return (
    <svg
      className="chart"
      viewBox={`0 0 ${width} ${height}`}
      onMouseDown={(e) => setDragStart(toT(e.clientX, e.currentTarget.getBoundingClientRect()))}
      onMouseMove={(e) => {
        if (dragStart == null) return;
        const t = toT(e.clientX, e.currentTarget.getBoundingClientRect());
        onRange([Math.min(dragStart, t), Math.max(dragStart, t)]);
      }}
      onMouseUp={() => setDragStart(null)}
      onMouseLeave={() => setDragStart(null)}
    >
      <rect x="0" y="0" width={width} height={height} rx="8" />
      {visibleSegments.map((segment) => (
        <rect
          key={segment.id}
          className={previewSegmentIds.has(segment.id) ? "autoSegment previewSegment" : "autoSegment"}
          x={x(segment.start_ms)}
          y={pad}
          width={Math.max(1, x(segment.end_ms) - x(segment.start_ms))}
          height={height - pad * 2}
        />
      ))}
      {yTicks.map((tick, index) => (
        <g key={`y-${index}-${tick}`}>
          <line x1={pad} x2={width - pad} y1={y(tick)} y2={y(tick)} />
          <text x={pad + 6} y={y(tick) + 4}>{formatValue(tick)} {unit}</text>
        </g>
      ))}
      {xTicks.map((tick, index) => (
        <g key={`x-${index}-${tick}`}>
          <line className="xTick" x1={x(tick)} x2={x(tick)} y1={height - pad} y2={height - pad + 6} />
          <text x={x(tick)} y={height - 12} textAnchor="middle">{formatTime(tick)}</text>
        </g>
      ))}
      <line className="thresholdLine" x1={pad} x2={width - pad} y1={y(threshold)} y2={y(threshold)} />
      {range ? <rect className="selection" x={x(range[0])} y={pad} width={Math.max(1, x(range[1]) - x(range[0]))} height={height - pad * 2} /> : null}
      <polyline points={line} />
    </svg>
  );
}

function SessionGpsBadge({ segment }: { segment: SegmentSummary }) {
  if (!segment.has_gps) return null;
  const pointText = segment.gps_points === 1 ? "1 GPS point" : `${segment.gps_points.toLocaleString()} GPS points`;
  return (
    <span className="sessionGpsBadge" title={pointText} aria-label={pointText}>
      <MapPinned size={14} />
    </span>
  );
}

function GpsTrace({
  points,
  gates,
  drawMode,
  onDrawGate,
  nextSplitNumber,
}: {
  points: GpsPoint[];
  gates: GateLine[];
  drawMode: GateDrawMode;
  onDrawGate: (gate: GateLine) => void;
  nextSplitNumber: number;
}) {
  const [drawStart, setDrawStart] = useState<{ x: number; y: number } | null>(null);
  const [drawEnd, setDrawEnd] = useState<{ x: number; y: number } | null>(null);
  const width = 760;
  const height = 520;
  const pad = 24;
  if (points.length < 2) {
    return (
      <div className="noGps">
        <strong>No GPS data in the selected session preview</strong>
        <span>Choose a session with GPS samples, or select a different day/channel range.</span>
      </div>
    );
  }
  const projected = points.map((p) => ({ ...p, ...project(p.lat, p.lon) }));
  const bounds = mapBounds(projected, width, height, pad);
  const line = projected.map((p) => `${bounds.x(p.mx)},${bounds.y(p.my)}`).join(" ");
  const tiles = satelliteTiles(bounds);
  const pointer = (event: MouseEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    return {
      x: ((event.clientX - rect.left) / rect.width) * width,
      y: ((event.clientY - rect.top) / rect.height) * height,
    };
  };
  const finishDraw = (end: { x: number; y: number }) => {
    if (!drawMode || !drawStart) return;
    const dx = end.x - drawStart.x;
    const dy = end.y - drawStart.y;
    if (Math.hypot(dx, dy) < 8) return;
    const a = bounds.latLon(drawStart.x, drawStart.y);
    const b = bounds.latLon(end.x, end.y);
    onDrawGate({
      id: `${drawMode}-${Date.now()}`,
      label: drawMode === "start_finish" ? "Start Finish" : `Split ${nextSplitNumber}`,
      role: drawMode,
      lat1: a.lat,
      lon1: a.lon,
      lat2: b.lat,
      lon2: b.lon,
    });
  };
  return (
    <svg
      className={drawMode ? "map drawing" : "map"}
      viewBox={`0 0 ${width} ${height}`}
      onMouseDown={(event) => {
        if (!drawMode) return;
        const start = pointer(event);
        setDrawStart(start);
        setDrawEnd(start);
      }}
      onMouseMove={(event) => {
        if (!drawStart) return;
        setDrawEnd(pointer(event));
      }}
      onMouseUp={(event) => {
        const end = pointer(event);
        finishDraw(end);
        setDrawStart(null);
        setDrawEnd(null);
      }}
      onMouseLeave={() => {
        setDrawStart(null);
        setDrawEnd(null);
      }}
    >
      <rect x="0" y="0" width={width} height={height} rx="8" />
      {tiles.map((tile) => (
        <image key={`${tile.z}-${tile.x}-${tile.y}`} href={tile.url} x={bounds.x(tile.left)} y={bounds.y(tile.top)} width={bounds.scale * tile.size} height={bounds.scale * tile.size} preserveAspectRatio="none" />
      ))}
      <polyline points={line} />
      {gates.map((gate) => {
        const a = project(gate.lat1, gate.lon1);
        const b = project(gate.lat2, gate.lon2);
        const labelX = bounds.x((a.mx + b.mx) / 2);
        const labelY = bounds.y((a.my + b.my) / 2);
        return (
          <g key={gate.id}>
            <line className={gate.role === "start_finish" ? "startGate" : "splitGate"} x1={bounds.x(a.mx)} y1={bounds.y(a.my)} x2={bounds.x(b.mx)} y2={bounds.y(b.my)} />
            <text
              className={gate.role === "start_finish" ? "gateLabel startGateLabel" : "gateLabel splitGateLabel"}
              x={labelX + 6}
              y={labelY - 6}
            >
              {gate.label}
            </text>
          </g>
        );
      })}
      {drawStart && drawEnd ? (
        <line
          className={drawMode === "start_finish" ? "startGate drawingGate" : "splitGate drawingGate"}
          x1={drawStart.x}
          y1={drawStart.y}
          x2={drawEnd.x}
          y2={drawEnd.y}
        />
      ) : null}
      <text className="mapCredit" x={width - 10} y={height - 10} textAnchor="end">Esri World Imagery</text>
    </svg>
  );
}

function TrackBuilderMap({
  points,
  liveSample,
  gates,
  drawMode,
  onDrawGate,
  center,
  onCenter,
  targetSpanM,
}: {
  points: GpsPoint[];
  liveSample: LiveSample | null;
  gates: GateLine[];
  drawMode: GateDrawMode;
  onDrawGate: (gate: GateLine) => void;
  center: { lat: number; lon: number };
  onCenter: (center: { lat: number; lon: number }) => void;
  targetSpanM?: number;
}) {
  const [drawStart, setDrawStart] = useState<{ x: number; y: number } | null>(null);
  const [drawEnd, setDrawEnd] = useState<{ x: number; y: number } | null>(null);
  const [panStart, setPanStart] = useState<{ x: number; y: number; cx: number; cy: number } | null>(null);
  const [spanM, setSpanM] = useState(520);
  const width = 920;
  const height = 620;
  const pad = 0;
  useEffect(() => {
    if (targetSpanM == null) return;
    setSpanM(Math.max(80, Math.min(8000, targetSpanM)));
  }, [targetSpanM]);
  const projectedCenter = project(center.lat, center.lon);
  const bounds = mapBoundsFromCenter(projectedCenter.mx, projectedCenter.my, width, height, spanM, pad);
  const tiles = satelliteTiles(bounds);
  const line = points.length >= 2
    ? points.map((p) => {
      const projected = project(p.lat, p.lon);
      return `${bounds.x(projected.mx)},${bounds.y(projected.my)}`;
    }).join(" ")
    : "";
  const liveProjected = liveSample && liveSample.lat != null && liveSample.lon != null ? project(liveSample.lat, liveSample.lon) : null;
  const pointer = (event: MouseEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    return {
      x: ((event.clientX - rect.left) / rect.width) * width,
      y: ((event.clientY - rect.top) / rect.height) * height,
    };
  };
  const finishDraw = (end: { x: number; y: number }) => {
    if (!drawMode || !drawStart) return;
    if (Math.hypot(end.x - drawStart.x, end.y - drawStart.y) < 8) return;
    const a = bounds.latLon(drawStart.x, drawStart.y);
    const b = bounds.latLon(end.x, end.y);
    onDrawGate({
      id: `${drawMode}-${Date.now()}`,
      label: drawMode === "start_finish" ? "Start Finish" : "Split",
      role: drawMode,
      lat1: a.lat,
      lon1: a.lon,
      lat2: b.lat,
      lon2: b.lon,
    });
  };
  return (
    <svg
      className={drawMode ? "map builderMap drawing" : "map builderMap"}
      viewBox={`0 0 ${width} ${height}`}
      onWheel={(event) => {
        event.preventDefault();
        setSpanM((current) => nextMapSpan(current, event.deltaY));
      }}
      onMouseDown={(event) => {
        const start = pointer(event);
        if (drawMode) {
          setDrawStart(start);
          setDrawEnd(start);
          return;
        }
        setPanStart({ ...start, cx: projectedCenter.mx, cy: projectedCenter.my });
      }}
      onMouseMove={(event) => {
        const current = pointer(event);
        if (drawStart) {
          setDrawEnd(current);
          return;
        }
        if (panStart) {
          const dx = (current.x - panStart.x) / bounds.scale;
          const dy = (current.y - panStart.y) / bounds.scale;
          const next = unproject(panStart.cx - dx, panStart.cy + dy);
          onCenter(next);
        }
      }}
      onMouseUp={(event) => {
        const end = pointer(event);
        finishDraw(end);
        setDrawStart(null);
        setDrawEnd(null);
        setPanStart(null);
      }}
      onMouseLeave={() => {
        setDrawStart(null);
        setDrawEnd(null);
        setPanStart(null);
      }}
    >
      <rect x="0" y="0" width={width} height={height} rx="8" />
      {tiles.map((tile) => (
        <image key={`${tile.z}-${tile.x}-${tile.y}`} href={tile.url} x={bounds.x(tile.left)} y={bounds.y(tile.top)} width={bounds.scale * tile.size} height={bounds.scale * tile.size} preserveAspectRatio="none" />
      ))}
      {line ? <polyline points={line} /> : null}
      {gates.map((gate) => {
        const a = project(gate.lat1, gate.lon1);
        const b = project(gate.lat2, gate.lon2);
        const labelX = bounds.x((a.mx + b.mx) / 2);
        const labelY = bounds.y((a.my + b.my) / 2);
        return (
          <g key={gate.id}>
            <line className={gate.role === "start_finish" ? "startGate" : "splitGate"} x1={bounds.x(a.mx)} y1={bounds.y(a.my)} x2={bounds.x(b.mx)} y2={bounds.y(b.my)} />
            <text className={gate.role === "start_finish" ? "gateLabel startGateLabel" : "gateLabel splitGateLabel"} x={labelX + 6} y={labelY - 6}>{gate.label}</text>
          </g>
        );
      })}
      {liveProjected ? (
        <circle className="liveDot" cx={bounds.x(liveProjected.mx)} cy={bounds.y(liveProjected.my)} r="8" />
      ) : null}
      {drawStart && drawEnd ? (
        <line
          className={drawMode === "start_finish" ? "startGate drawingGate" : "splitGate drawingGate"}
          x1={drawStart.x}
          y1={drawStart.y}
          x2={drawEnd.x}
          y2={drawEnd.y}
        />
      ) : null}
      <text className="mapCredit" x={width - 10} y={height - 10} textAnchor="end">Esri World Imagery</text>
    </svg>
  );
}

function LapPreview({ rows, hasStartFinish, gpsPointCount }: { rows: LapPreviewRow[]; hasStartFinish: boolean; gpsPointCount: number }) {
  let emptyMessage = "No start/finish crossings detected in the selected GPS trace.";
  if (!gpsPointCount) emptyMessage = "No GPS samples loaded for the selected preview.";
  else if (!hasStartFinish) emptyMessage = "Draw a start/finish gate to preview generated laps.";
  return (
    <div className="lapPreview">
      <div className="lapPreviewHeader">
        <strong>Generated Laps</strong>
        <span>{rows.length ? `${rows.length} rows` : "No lap rows"}</span>
      </div>
      {rows.length ? (
        <table className="lapTable">
          <thead>
            <tr>
              <th>Lap</th>
              <th>Type</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id}>
                <td>{row.label}</td>
                <td>{row.kind === "outlap" ? "Outlap" : "Timed"}</td>
                <td>{formatLapTime(row.durationMs)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <small className="muted">{emptyMessage}</small>
      )}
    </div>
  );
}

function project(lat: number, lon: number) {
  const radius = 6378137;
  const clamped = Math.max(-85.0511, Math.min(85.0511, lat));
  const mx = radius * lon * Math.PI / 180;
  const my = radius * Math.log(Math.tan(Math.PI / 4 + (clamped * Math.PI) / 360));
  return { mx, my };
}

function unproject(mx: number, my: number) {
  const radius = 6378137;
  const lon = (mx / radius) * 180 / Math.PI;
  const lat = (2 * Math.atan(Math.exp(my / radius)) - Math.PI / 2) * 180 / Math.PI;
  return { lat, lon };
}

function mapBounds(points: Array<{ mx: number; my: number }>, width: number, height: number, pad: number) {
  const xs = points.map((p) => p.mx);
  const ys = points.map((p) => p.my);
  const minX = xs.length ? Math.min(...xs) : -10879330;
  const maxX = xs.length ? Math.max(...xs) : -10879030;
  const minY = ys.length ? Math.min(...ys) : 3547800;
  const maxY = ys.length ? Math.max(...ys) : 3548100;
  const span = Math.max(maxX - minX, maxY - minY, 80);
  const cx = (minX + maxX) / 2;
  const cy = (minY + maxY) / 2;
  const scale = Math.min((width - pad * 2) / span, (height - pad * 2) / span);
  const viewMinX = cx - (width - pad * 2) / scale / 2;
  const viewMaxY = cy + (height - pad * 2) / scale / 2;
  return {
    minX: viewMinX,
    maxX: cx + (width - pad * 2) / scale / 2,
    minY: cy - (height - pad * 2) / scale / 2,
    maxY: viewMaxY,
    scale,
    x: (mx: number) => pad + (mx - viewMinX) * scale,
    y: (my: number) => pad + (viewMaxY - my) * scale,
    latLon: (x: number, y: number) => unproject(viewMinX + (x - pad) / scale, viewMaxY - (y - pad) / scale),
  };
}

function mapBoundsFromCenter(centerX: number, centerY: number, width: number, height: number, spanM: number, pad: number) {
  const scale = Math.min((width - pad * 2) / spanM, (height - pad * 2) / spanM);
  const viewWidth = (width - pad * 2) / scale;
  const viewHeight = (height - pad * 2) / scale;
  const minX = centerX - viewWidth / 2;
  const maxY = centerY + viewHeight / 2;
  return {
    minX,
    maxX: centerX + viewWidth / 2,
    minY: centerY - viewHeight / 2,
    maxY,
    scale,
    x: (mx: number) => pad + (mx - minX) * scale,
    y: (my: number) => pad + (maxY - my) * scale,
    latLon: (x: number, y: number) => unproject(minX + (x - pad) / scale, maxY - (y - pad) / scale),
  };
}

function satelliteTiles(bounds: ReturnType<typeof mapBounds>) {
  const z = 18;
  const origin = 20037508.342789244;
  const tileSize = (origin * 2) / 2 ** z;
  const minTileX = Math.floor((bounds.minX + origin) / tileSize);
  const maxTileX = Math.floor((bounds.maxX + origin) / tileSize);
  const minTileY = Math.floor((origin - bounds.maxY) / tileSize);
  const maxTileY = Math.floor((origin - bounds.minY) / tileSize);
  const tiles = [];
  for (let x = minTileX; x <= maxTileX; x += 1) {
    for (let y = minTileY; y <= maxTileY; y += 1) {
      tiles.push({
        z,
        x,
        y,
        left: x * tileSize - origin,
        top: origin - y * tileSize,
        size: tileSize,
        url: `https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/${z}/${y}/${x}`,
      });
    }
  }
  return tiles.slice(0, 64);
}

function defaultMetadataBase(sourceLabel: string, selectedDate: string): SessionMetadata {
  return {
    ...EMPTY_METADATA,
    vehicle_id: sourceLabel,
    event: `${sourceLabel} Telemetry Export`,
    session: selectedDate,
  };
}

function defaultMetadataForSegment(segment: SegmentSummary, sourceLabel: string, selectedDate: string): SessionMetadata {
  return {
    ...defaultMetadataBase(sourceLabel, selectedDate),
    session: segment.label,
    short_comment: segment.id,
  };
}

function summarizeSegments(segments: SegmentSummary[]) {
  if (!segments.length) return null;
  const startMs = Math.min(...segments.map((segment) => segment.start_ms));
  const endMs = Math.max(...segments.map((segment) => segment.end_ms));
  const durationS = segments.reduce((total, segment) => total + segment.duration_s, 0);
  return { startMs, endMs, durationS };
}

function trackViewFromGates(gates: GateLine[]) {
  if (!gates.length) return null;
  const projected = gates.flatMap((gate) => [project(gate.lat1, gate.lon1), project(gate.lat2, gate.lon2)]);
  const xs = projected.map((point) => point.mx);
  const ys = projected.map((point) => point.my);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const center = unproject((minX + maxX) / 2, (minY + maxY) / 2);
  const spanM = Math.max(160, Math.min(8000, Math.max(maxX - minX, maxY - minY) * 1.8));
  return { center, spanM };
}

function normalizeTrack(track: TrackDefinition): TrackDefinition {
  let hasStartFinish = false;
  const gates = track.gates.filter((gate) => {
    if (gate.role !== "start_finish") return true;
    if (hasStartFinish) return false;
    hasStartFinish = true;
    return true;
  });
  return {
    ...track,
    name: track.name || "New Track",
    slug: track.slug || slugifyTrackName(track.name || "New Track"),
    gates: normalizeGateLabels(gates),
  };
}

function normalizeGateLabels(gates: GateLine[]) {
  let splitNumber = 1;
  return gates.map((gate) => {
    if (gate.role === "start_finish") {
      return { ...gate, label: gate.label.trim() || "Start Finish" };
    }
    const trimmed = gate.label.trim();
    const shouldAutoName = !trimmed || /^Split\s+\d+$/i.test(trimmed);
    const label = shouldAutoName ? `Split ${splitNumber}` : trimmed;
    splitNumber += 1;
    return { ...gate, label };
  });
}

function buildLapPreview(points: GpsPoint[], gates: GateLine[], summary: ReturnType<typeof summarizeSegments>): LapPreviewRow[] {
  const startGate = gates.find((gate) => gate.role === "start_finish");
  if (!startGate || points.length < 2) return [];
  const windowStart = summary?.startMs ?? points[0].t;
  const windowEnd = summary?.endMs ?? points.at(-1)?.t ?? windowStart;
  const crossings = dedupeCrossings(gateCrossingTimes(points, startGate), 1500)
    .filter((time) => time > windowStart && time < windowEnd)
    .sort((a, b) => a - b);
  if (!crossings.length) return [];

  const rows: LapPreviewRow[] = [];
  rows.push({
    id: "outlap-start",
    label: "Out lap",
    kind: "outlap",
    startMs: windowStart,
    endMs: crossings[0],
    durationMs: crossings[0] - windowStart,
  });
  for (let index = 0; index < crossings.length - 1; index += 1) {
    rows.push({
      id: `lap-${index + 1}`,
      label: `Lap ${index + 1}`,
      kind: "lap",
      startMs: crossings[index],
      endMs: crossings[index + 1],
      durationMs: crossings[index + 1] - crossings[index],
    });
  }
  const finalCrossing = crossings.at(-1) ?? windowStart;
  rows.push({
    id: "outlap-end",
    label: "Out lap",
    kind: "outlap",
    startMs: finalCrossing,
    endMs: windowEnd,
    durationMs: windowEnd - finalCrossing,
  });
  return rows.filter((row) => row.durationMs > 0);
}

function gateCrossingTimes(points: GpsPoint[], gate: GateLine) {
  const crossings: number[] = [];
  for (let index = 1; index < points.length; index += 1) {
    const previous = points[index - 1];
    const current = points[index];
    if (
      segmentsCross(
        [previous.lon, previous.lat],
        [current.lon, current.lat],
        [gate.lon1, gate.lat1],
        [gate.lon2, gate.lat2],
      )
    ) {
      crossings.push(current.t);
    }
  }
  return crossings;
}

function segmentsCross(a: [number, number], b: [number, number], c: [number, number], d: [number, number]) {
  const o1 = orientation(a, b, c);
  const o2 = orientation(a, b, d);
  const o3 = orientation(c, d, a);
  const o4 = orientation(c, d, b);
  return o1 * o2 < 0 && o3 * o4 < 0;
}

function orientation(a: [number, number], b: [number, number], c: [number, number]) {
  return (b[1] - a[1]) * (c[0] - b[0]) - (b[0] - a[0]) * (c[1] - b[1]);
}

function dedupeCrossings(times: number[], minimumGapMs: number) {
  const deduped: number[] = [];
  times.forEach((time) => {
    if (!deduped.length || time - deduped[deduped.length - 1] >= minimumGapMs) {
      deduped.push(time);
    }
  });
  return deduped;
}

function hasGps(sample: LiveSample): sample is LiveSample & { lat: number; lon: number } {
  return typeof sample.lat === "number" && typeof sample.lon === "number" && Number.isFinite(sample.lat) && Number.isFinite(sample.lon);
}

function distanceMeters(lat1: number, lon1: number, lat2: number, lon2: number) {
  const radius = 6371000;
  const phi1 = (lat1 * Math.PI) / 180;
  const phi2 = (lat2 * Math.PI) / 180;
  const dPhi = ((lat2 - lat1) * Math.PI) / 180;
  const dLambda = ((lon2 - lon1) * Math.PI) / 180;
  const a = Math.sin(dPhi / 2) ** 2 + Math.cos(phi1) * Math.cos(phi2) * Math.sin(dLambda / 2) ** 2;
  return radius * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function nextMapSpan(currentSpanM: number, deltaY: number) {
  const boundedDelta = Math.max(-120, Math.min(120, deltaY));
  const zoomFactor = Math.exp(boundedDelta * 0.0007);
  return Math.max(80, Math.min(8000, currentSpanM * zoomFactor));
}

function sampleCrossesGate(previous: LiveSample, current: LiveSample, gate: GateLine) {
  if (!hasGps(previous) || !hasGps(current)) return false;
  return segmentsCross(
    [previous.lon, previous.lat],
    [current.lon, current.lat],
    [gate.lon1, gate.lat1],
    [gate.lon2, gate.lat2],
  );
}

function bestSectorTimes(laps: LiveLap[]) {
  const sectorCount = Math.max(0, ...laps.map((lap) => lap.sectors.length));
  return Array.from({ length: sectorCount }, (_unused, index) => {
    const values = laps.map((lap) => lap.sectors[index]).filter((value): value is number => typeof value === "number" && value > 0);
    return values.length ? Math.min(...values) : null;
  });
}

function estimateDeltaRate(sample: LiveSample, bestLap: LiveLap | null) {
  if (!bestLap || sample.speed == null || sample.speed <= 0) return null;
  const bestAverageSpeed = bestLap.avgSpeedMps ?? 20;
  return Math.max(-1.5, Math.min(1.5, bestAverageSpeed / Math.max(0.5, sample.speed) - 1));
}

function parseLatLon(value: string) {
  const parts = value.split(/[,\s]+/).map((part) => Number(part.trim())).filter((part) => Number.isFinite(part));
  if (parts.length < 2) return null;
  const [lat, lon] = parts;
  if (Math.abs(lat) > 90 || Math.abs(lon) > 180) return null;
  return { lat, lon };
}

function slugifyTrackName(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "") || "track";
}

function defaultChannelChartSlugForSource(source: CarPreset["source"]) {
  return source === "angelique" ? ANGELIQUE_CHANNEL_CHART_SLUG : DEFAULT_CHANNEL_CHART.slug;
}

function preferredChannelChart(charts: ChannelChartDefinition[], source: CarPreset["source"]) {
  const defaultSlug = defaultChannelChartSlugForSource(source);
  const sourceToken = source.toLowerCase();
  return (
    charts.find((chart) => chart.slug === defaultSlug) ??
    charts.find((chart) => chart.slug.toLowerCase().includes(sourceToken) || chart.name.toLowerCase().includes(sourceToken)) ??
    charts[0] ??
    DEFAULT_CHANNEL_CHART
  );
}

function parseChannelChartJson(text: string, fileName: string): ChannelChartDefinition {
  const parsed = JSON.parse(text);
  if (Array.isArray(parsed)) {
    return {
      name: fileName.replace(/\.[^.]+$/, ""),
      slug: slugifyTrackName(fileName.replace(/\.[^.]+$/, "")),
      notes: "Imported channel chart",
      entries: parsed.map(normalizeChannelChartEntry),
    };
  }
  if (!parsed || !Array.isArray(parsed.entries)) throw new Error("Channel chart JSON must contain an entries array.");
  return {
    ...DEFAULT_CHANNEL_CHART,
    ...parsed,
    name: parsed.name || fileName.replace(/\.[^.]+$/, ""),
    slug: parsed.slug || slugifyTrackName(parsed.name || fileName.replace(/\.[^.]+$/, "")),
    entries: parsed.entries.map(normalizeChannelChartEntry),
  };
}

function parseChannelChartCsv(text: string, fileName: string): ChannelChartDefinition {
  const rows = parseCsv(text);
  if (!rows.length) throw new Error("Channel chart CSV is empty.");
  const headers = rows[0].map((header) => header.trim().toLowerCase());
  const channelIndex = findHeader(headers, ["channel_name", "channel", "name"]);
  const quantityIndex = findHeader(headers, ["quantity_type", "quantity", "qty", "type"]);
  const unitIndex = findHeader(headers, ["unit", "units"]);
  const notesIndex = findHeader(headers, ["notes", "note"]);
  if (channelIndex < 0 || (quantityIndex < 0 && unitIndex < 0)) {
    throw new Error("Channel chart CSV must contain channel_name plus quantity_type and/or unit columns.");
  }
  const entries = rows.slice(1)
    .map((row) => ({
      channel_name: row[channelIndex]?.trim() ?? "",
      quantity_type: quantityIndex >= 0 ? row[quantityIndex]?.trim() ?? "" : "",
      unit: unitIndex >= 0 ? row[unitIndex]?.trim() ?? "" : "",
      notes: notesIndex >= 0 ? row[notesIndex]?.trim() ?? "" : "",
    }))
    .filter((entry) => entry.channel_name && !entry.channel_name.startsWith("#"));
  return {
    name: fileName.replace(/\.[^.]+$/, ""),
    slug: slugifyTrackName(fileName.replace(/\.[^.]+$/, "")),
    notes: "Imported from CSV channel chart.",
    entries,
  };
}

function normalizeChannelChartEntry(entry: unknown) {
  const value = entry as Partial<{ channel_name: string; channel: string; name: string; quantity_type: string; quantity: string; unit: string; units: string; notes: string }>;
  return {
    channel_name: String(value.channel_name ?? value.channel ?? value.name ?? ""),
    quantity_type: String(value.quantity_type ?? value.quantity ?? ""),
    unit: String(value.unit ?? value.units ?? ""),
    notes: String(value.notes ?? ""),
  };
}

function findHeader(headers: string[], names: string[]) {
  return headers.findIndex((header) => names.includes(header));
}

function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let quoted = false;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];
    if (quoted) {
      if (char === "\"" && next === "\"") {
        field += "\"";
        index += 1;
      } else if (char === "\"") {
        quoted = false;
      } else {
        field += char;
      }
      continue;
    }
    if (char === "\"") {
      quoted = true;
    } else if (char === ",") {
      row.push(field);
      field = "";
    } else if (char === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else if (char !== "\r") {
      field += char;
    }
  }
  row.push(field);
  rows.push(row);
  return rows.filter((item) => item.some((value) => value.trim()));
}

function formatTime(ms: number) {
  return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatDateTime(ms: number) {
  return new Date(ms).toLocaleString([], { dateStyle: "medium", timeStyle: "medium" });
}

function formatDuration(seconds: number) {
  if (seconds > 3600) return `${(seconds / 3600).toFixed(1)} hr`;
  if (seconds > 60) return `${(seconds / 60).toFixed(1)} min`;
  return `${seconds.toFixed(1)} s`;
}

function formatLapTime(ms: number) {
  const totalSeconds = Math.max(0, ms / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${seconds.toFixed(2).padStart(5, "0")}`;
}

function formatSignedSeconds(ms: number) {
  const seconds = ms / 1000;
  return `${seconds >= 0 ? "+" : ""}${seconds.toFixed(2)} s`;
}

function formatSpeed(speed: number) {
  return `${(speed * 2.23694).toFixed(1)} mph`;
}

function firstLiveValue(values: Record<string, number>, keys: string[]) {
  for (const key of keys) {
    const value = values[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
  }
  return null;
}

function smoothLiveSamples(samples: LiveSample[]) {
  let previous: LiveSample | null = null;
  return samples.map((sample) => {
    const dtSeconds = previous ? Math.max(0.02, (sample.t - previous.t) / 1000) : 0.1;
    const values = { ...sample.values };
    const smoothed: LiveSample = { ...sample, values };

    const voltage = packVoltageFor(sample);
    const current = packCurrentFor(sample);
    const powerKw = sample.power_kw ?? sample.values.power_kw ?? null;
    const speed = sample.speed ?? sample.values.speed ?? null;

    const previousVoltage = previous ? packVoltageFor(previous) : null;
    const previousCurrent = previous ? packCurrentFor(previous) : null;
    const previousPowerKw = previous?.power_kw ?? previous?.values.power_kw ?? null;
    const previousSpeed = previous?.speed ?? previous?.values.speed ?? null;

    const filteredVoltage = smoothNumber(voltage, previousVoltage, dtSeconds, 3.5);
    const filteredCurrent = smoothNumber(current, previousCurrent, dtSeconds, 1.1);
    const filteredPowerKw = smoothNumber(powerKw, previousPowerKw, dtSeconds, 1.1);
    const filteredSpeed = smoothNumber(speed, previousSpeed, dtSeconds, 0.8);

    if (filteredVoltage != null) {
      smoothed.hv_pack_v = filteredVoltage;
      values.hv_pack_v = filteredVoltage;
      values.dc_bus_v = filteredVoltage;
    }
    if (filteredCurrent != null) {
      smoothed.hv_c = filteredCurrent;
      values.hv_c = filteredCurrent;
      values.dc_bus_current = filteredCurrent;
    }
    if (filteredPowerKw != null) {
      smoothed.power_kw = filteredPowerKw;
      values.power_kw = filteredPowerKw;
    }
    if (filteredSpeed != null) {
      smoothed.speed = filteredSpeed;
      values.speed = filteredSpeed;
    }

    if (hasGps(sample) && isReasonableGps(sample.lat, sample.lon)) {
      const previousGps = previous && hasGps(previous) ? previous : null;
      if (!previousGps) {
        smoothed.lat = sample.lat;
        smoothed.lon = sample.lon;
      } else {
        const jumpSpeedMps = distanceMeters(previousGps.lat, previousGps.lon, sample.lat, sample.lon) / dtSeconds;
        if (jumpSpeedMps > 85) {
          smoothed.lat = previousGps.lat;
          smoothed.lon = previousGps.lon;
        } else {
          const alpha = alphaForDt(dtSeconds, 0.65);
          smoothed.lat = previousGps.lat + (sample.lat - previousGps.lat) * alpha;
          smoothed.lon = previousGps.lon + (sample.lon - previousGps.lon) * alpha;
        }
      }
    } else {
      smoothed.lat = null;
      smoothed.lon = null;
    }

    previous = smoothed;
    return smoothed;
  });
}

function isReasonableGps(lat: number, lon: number) {
  return Number.isFinite(lat) && Number.isFinite(lon) && Math.abs(lat) <= 90 && Math.abs(lon) <= 180 && Math.hypot(lat, lon) > 0.001;
}

function smoothNumber(value: number | null, previous: number | null, dtSeconds: number, tauSeconds: number) {
  if (value == null || !Number.isFinite(value)) return previous;
  if (previous == null || !Number.isFinite(previous)) return value;
  const alpha = alphaForDt(dtSeconds, tauSeconds);
  return previous + (value - previous) * alpha;
}

function alphaForDt(dtSeconds: number, tauSeconds: number) {
  return clamp(1 - Math.exp(-Math.max(0.001, dtSeconds) / Math.max(0.001, tauSeconds)), 0.02, 0.45);
}

function packStatus(samples: LiveSample[]) {
  const sample = samples.at(-1) ?? null;
  const voltage = packVoltageFor(sample);
  const current = packCurrentFor(sample);
  const ocvEstimate = estimateOcVSoc(samples);
  return {
    voltage,
    current,
    ocvVoltage: ocvEstimate.voltage,
    socPercent: ocvEstimate.socPercent,
    socSource: ocvEstimate.socPercent == null ? "waiting" : "ocv",
  };
}

function packVoltageFor(sample: LiveSample | null) {
  const values = sample?.values ?? {};
  return firstLiveValue(values, ["hv_pack_v", "dc_bus_v", "bus_voltage", "pack_hv_pack_v", "pack_dc_bus_v"]) ?? sample?.hv_pack_v ?? null;
}

function packCurrentFor(sample: LiveSample | null) {
  const values = sample?.values ?? {};
  return firstLiveValue(values, ["hv_c", "dc_bus_current", "pack_hv_c", "pack_dc_bus_current"]) ?? sample?.hv_c ?? null;
}

function estimateOcVSoc(samples: LiveSample[]) {
  const lastT = samples.at(-1)?.t ?? 0;
  const recentSamples = lastT ? samples.filter((sample) => sample.t >= lastT - 180_000) : samples;
  const lowCurrentVoltages = recentSamples
    .map((sample) => {
      const voltage = packVoltageFor(sample);
      const current = packCurrentFor(sample);
      if (voltage == null || current == null) return null;
      if (Math.abs(current) > 5) return null;
      if (voltage < 300 || voltage > 560) return null;
      return voltage;
    })
    .filter((value): value is number => value != null);
  if (lowCurrentVoltages.length < 5) {
    return { voltage: null, socPercent: null };
  }

  const medianVoltage = median(lowCurrentVoltages);
  const filtered = lowCurrentVoltages.filter((value) => Math.abs(value - medianVoltage) <= 2);
  const source = filtered.length >= 5 ? filtered : lowCurrentVoltages;
  const smoothed = source.reduce((estimate, value) => estimate + (value - estimate) * 0.12, source[0]);
  return {
    voltage: smoothed,
    socPercent: estimateP30bSoc(smoothed / 130),
  };
}

function median(values: number[]) {
  const sorted = [...values].sort((a, b) => a - b);
  const midpoint = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[midpoint] : (sorted[midpoint - 1] + sorted[midpoint]) / 2;
}

function driverControls(sample: LiveSample | null) {
  const values = sample?.values ?? {};
  const voltageThrottlePercent = appsTravelPercent(values);
  const apps1 = normalizeTravelPercent(firstLiveValue(values, ["apps1_travel", "controls_apps1_travel"]));
  const apps2 = normalizeTravelPercent(firstLiveValue(values, ["apps2_travel", "controls_apps2_travel"]));
  const throttlePercent = voltageThrottlePercent ?? averageValues([apps1, apps2]);
  const bse1V = firstLiveValue(values, ["bse1_v", "controls_bse1_v"]);
  const bse2V = firstLiveValue(values, ["bse2_v", "controls_bse2_v"]);
  return {
    steeringAngleDeg: firstLiveValue(values, ["steer_col_angle", "dynamics_steer_col_angle", "steering_angle"]),
    throttlePercent: throttlePercent == null ? null : clamp(throttlePercent, 0, 100),
    bse1Psi: bse1V == null ? null : bse1Psi(bse1V),
    bse2Psi: bse2V == null ? null : bse2Psi(bse2V),
  };
}

function appsTravelPercent(values: Record<string, number>) {
  const apps1V = sensorVoltage(firstLiveValue(values, ["apps1_v", "controls_apps1_v"]));
  const apps2V = sensorVoltage(firstLiveValue(values, ["apps2_v", "controls_apps2_v"]));
  if (apps1V == null || apps2V == null) return null;
  const apps1Travel = (1.750 - apps1V) / 0.230;
  const apps2Travel = (0.190 - apps2V) / 0.210;
  return clamp(((apps1Travel + apps2Travel) / 2) * 100, 0, 100);
}

function sensorVoltage(value: number | null) {
  if (value == null || !Number.isFinite(value)) return null;
  if (value > 5 && value <= 4095) return value * 3.3 / 4095;
  return value;
}

function normalizeTravelPercent(value: number | null) {
  if (value == null || !Number.isFinite(value)) return null;
  return value <= 1.25 ? value * 100 : value;
}

function bse1Psi(volts: number) {
  return clamp(2000.6452 * volts - 636.8984, 0, 3000);
}

function bse2Psi(volts: number) {
  return clamp(2309.3868 * volts - 735.1852, 0, 3000);
}

function averageValues(values: Array<number | null>) {
  const finite = values.filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  return finite.length ? finite.reduce((sum, value) => sum + value, 0) / finite.length : null;
}

function tempStatus(sample: LiveSample | null) {
  const values = sample?.values ?? {};
  const moduleB = validTemp(firstLiveValue(values, ["module_b_temp", "thermal_module_b_temp"]));
  const moduleC = validTemp(firstLiveValue(values, ["module_c_temp", "thermal_module_c_temp"]));
  return {
    ambient: validTemp(firstLiveValue(values, ["ambient_temp", "thermal_ambient_temp"])),
    coolant: validTemp(firstLiveValue(values, ["coolant_temp", "thermal_coolant_temp"])),
    fanRpm: firstLiveValue(values, ["fan_rpm", "thermal_fan_rpm", "battery_fan_rpm", "thermal_battery_fan_rpm"]),
    motor: validTemp(firstLiveValue(values, ["motor_temp", "thermal_motor_temp"])),
    inverter: averageValues([moduleB, moduleC]) ?? validTemp(firstLiveValue(values, ["inverter_temp", "thermal_inverter_temp"])),
  };
}

function validTemp(value: number | null) {
  if (value == null || !Number.isFinite(value)) return null;
  return value >= -40 && value <= 180 ? value : null;
}

function temperatureSeries(samples: LiveSample[], windowS: number) {
  const lastT = samples.at(-1)?.t;
  const startT = lastT ? lastT - windowS * 1000 : 0;
  const definitions = [
    { key: "ambient", label: "Ambient", color: "#38bdf8" },
    { key: "coolant", label: "Coolant", color: "#22c55e" },
    { key: "motor", label: "Motor", color: "#f97316" },
    { key: "inverter", label: "Inverter", color: "#e879f9" },
  ] as const;
  const windowSamples = lastT ? samples.filter((sample) => sample.t >= startT) : [];
  return definitions.map((definition) => ({
    ...definition,
    segments: windowSamples.reduce<Array<Array<{ t: number; value: number }>>>((segments, sample) => {
        const value = tempStatus(sample)[definition.key];
        if (value == null) {
          if (segments.at(-1)?.length) segments.push([]);
          return segments;
        }
        if (!segments.length) segments.push([]);
        segments[segments.length - 1].push({ t: sample.t, value });
        return segments;
      }, [])
      .filter((segment) => segment.length),
  }));
}

function estimateP30bSoc(cellVoltage: number) {
  const curve = [
    [2.5, 0],
    [3.2, 5],
    [3.45, 10],
    [3.55, 20],
    [3.62, 30],
    [3.69, 40],
    [3.75, 50],
    [3.82, 60],
    [3.9, 70],
    [3.98, 80],
    [4.08, 90],
    [4.2, 100],
  ] as const;
  if (cellVoltage <= curve[0][0]) return 0;
  for (let index = 1; index < curve.length; index += 1) {
    const [v1, soc1] = curve[index];
    const [v0, soc0] = curve[index - 1];
    if (cellVoltage <= v1) {
      const ratio = (cellVoltage - v0) / (v1 - v0);
      return clamp(soc0 + ratio * (soc1 - soc0), 0, 100);
    }
  }
  return 100;
}

function energyTrace(samples: LiveSample[], windowS: number) {
  const lastT = samples.at(-1)?.t;
  if (!lastT) return [];
  const startT = lastT - windowS * 1000;
  const windowSamples = samples.filter((sample) => sample.t >= startT);
  if (!windowSamples.length) return [];
  let energyWh = 0;
  const points = [{ t: windowSamples[0].t, energyWh: 0 }];
  for (let index = 1; index < windowSamples.length; index += 1) {
    const previous = windowSamples[index - 1];
    const sample = windowSamples[index];
    const dtSeconds = Math.max(0, (sample.t - previous.t) / 1000);
    const powerKw = Math.abs(sample.power_kw ?? sample.values.power_kw ?? 0);
    energyWh += powerKw * dtSeconds / 3.6;
    points.push({ t: sample.t, energyWh });
  }
  return points;
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function topLiveValues(values: Record<string, number>) {
  const priority = [
    "controls_motor_speed",
    "motor_speed",
    "dynamics_gps_speed",
    "gps_speed",
    "wheel_speed",
    "flw_speed",
    "frw_speed",
    "blw_speed",
    "brw_speed",
    "dynamics_inverter_rpm",
    "bus_voltage",
    "pack_dc_bus_v",
    "dc_bus_v",
    "pack_dc_bus_current",
    "dc_bus_current",
    "pack_hv_pack_v",
    "hv_pack_v",
    "pack_hv_c",
    "hv_c",
    "pack_hv_soc",
    "hv_soc",
    "power_kw",
    "controls_torque_feedback",
    "controls_torque_request",
    "dynamics_inverter_torque",
    "thermal_motor_temp",
    "thermal_inverter_temp",
  ];
  const entries = Object.entries(values).filter(([, value]) => Number.isFinite(value));
  const rank = new Map(priority.map((key, index) => [key, index]));
  return entries
    .sort(([a], [b]) => (rank.get(a) ?? 1000) - (rank.get(b) ?? 1000) || a.localeCompare(b))
    .slice(0, 12);
}

function labelFromKey(key: string) {
  return key.replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatRawLiveValue(key: string, value: number) {
  const lower = key.toLowerCase();
  if (lower.includes("rpm") || lower.includes("motor_speed")) return `${Math.round(value).toLocaleString()} rpm`;
  if (lower.includes("speed")) return `${value.toFixed(2)} m/s`;
  if (lower.includes("voltage") || lower.endsWith("_v")) return `${value.toFixed(1)} V`;
  if (lower.includes("current") || lower.endsWith("_c")) return `${value.toFixed(1)} A`;
  if (lower.includes("power")) return `${value.toFixed(2)} kW`;
  if (lower.includes("temp")) return `${value.toFixed(1)} C`;
  if (lower.includes("torque")) return `${value.toFixed(1)} Nm`;
  return Math.abs(value) >= 100 ? Math.round(value).toLocaleString() : value.toFixed(3);
}

function formatValue(value: number) {
  if (Math.abs(value) >= 100) return Math.round(value).toString();
  return value.toFixed(1);
}

export default App;
