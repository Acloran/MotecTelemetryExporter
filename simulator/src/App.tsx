import { useEffect, useMemo, useState } from "react";
import { CalendarDays, Gauge, Play, RadioTower, RefreshCcw, Square, Zap } from "lucide-react";

type SourceDef = { key: "orion" | "angelique"; label: string };
type KafkaTransport = "local" | "kafka";
type ChannelDef = {
  key: string;
  label: string;
  table: string;
  column: string;
  unit: string;
  quantity: string;
  default: boolean;
  split_candidate: boolean;
};
type DriveDay = { date: string; sessions: number; start_ms: number; end_ms: number; label: string };
type SessionSummary = {
  id: string;
  label: string;
  start_ms: number;
  end_ms: number;
  duration_s: number;
  source: string;
  preview_safe: boolean;
  warning: string | null;
};
type SegmentSummary = {
  id: string;
  label: string;
  start_ms: number;
  end_ms: number;
  duration_s: number;
  source_channel: string;
};
type ReplayStatus = {
  running: boolean;
  source: "orion" | "angelique";
  topic: string;
  transport: KafkaTransport;
  start_ms: number | null;
  end_ms: number | null;
  current_ms: number | null;
  samples_sent: number;
  message: string;
};

const EMPTY_STATUS: ReplayStatus = {
  running: false,
  source: "orion",
  topic: "",
  transport: "local",
  start_ms: null,
  end_ms: null,
  current_ms: null,
  samples_sent: 0,
  message: "Idle",
};

function App() {
  const [sources, setSources] = useState<SourceDef[]>([]);
  const [source, setSource] = useState<"orion" | "angelique">("orion");
  const [channels, setChannels] = useState<ChannelDef[]>([]);
  const [channel, setChannel] = useState("");
  const [selectedChannelKeys, setSelectedChannelKeys] = useState<Set<string>>(new Set());
  const [days, setDays] = useState<DriveDay[]>([]);
  const [selectedDate, setSelectedDate] = useState("");
  const [segments, setSegments] = useState<SegmentSummary[]>([]);
  const [selectedSegmentId, setSelectedSegmentId] = useState("");
  const [threshold, setThreshold] = useState(0);
  const [minDurationS, setMinDurationS] = useState(10);
  const [speedMultiplier, setSpeedMultiplier] = useState(1);
  const [frequencyHz, setFrequencyHz] = useState(10);
  const [loop, setLoop] = useState(false);
  const [topic, setTopic] = useState(() => localStorage.getItem("motec-simulator-topic") || "");
  const [transport, setTransport] = useState<KafkaTransport>(() => (localStorage.getItem("motec-simulator-transport") === "kafka" ? "kafka" : "local"));
  const [status, setStatus] = useState<ReplayStatus>(EMPTY_STATUS);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const sourceLabel = sources.find((item) => item.key === source)?.label || source;
  const selectedSegment = segments.find((segment) => segment.id === selectedSegmentId) ?? null;
  const requiredChannelKeys = useMemo(() => requiredReplayChannelKeys(channels), [channels]);
  const selectedChannelList = useMemo(() => [...selectedChannelKeys], [selectedChannelKeys]);

  useEffect(() => {
    void loadBase(source);
  }, [source]);

  useEffect(() => {
    if (!channel) return;
    const timeout = window.setTimeout(() => {
      void loadCalendar(source, channel, threshold, minDurationS);
    }, 250);
    return () => window.clearTimeout(timeout);
  }, [source, channel, threshold, minDurationS]);

  useEffect(() => {
    if (!selectedDate || !channel) return;
    void loadSegments(selectedDate);
  }, [selectedDate, channel, threshold, minDurationS, source]);

  useEffect(() => {
    void refreshStatus();
    const interval = window.setInterval(() => void refreshStatus(), 1000);
    return () => window.clearInterval(interval);
  }, []);

  useEffect(() => {
    localStorage.setItem("motec-simulator-topic", topic);
  }, [topic]);

  useEffect(() => {
    localStorage.setItem("motec-simulator-transport", transport);
  }, [transport]);

  async function loadBase(nextSource: "orion" | "angelique") {
    setBusy(true);
    setError("");
    try {
      const [sourceResponse, channelResponse, replayStatus] = await Promise.all([
        getJson<{ sources: SourceDef[] }>("/api/sources"),
        getJson<{ channels: ChannelDef[]; default: string }>(`/api/channels?source=${encodeURIComponent(nextSource)}`),
        getJson<ReplayStatus>("/api/simulator/status"),
      ]);
      setSources(sourceResponse.sources);
      setChannels(channelResponse.channels);
      setChannel(channelResponse.default);
      setSelectedChannelKeys(new Set(defaultReplayChannelKeys(channelResponse.channels)));
      setStatus(replayStatus);
      setSegments([]);
      setSelectedSegmentId("");
    } catch (e) {
      showError(e);
    } finally {
      setBusy(false);
    }
  }

  async function loadCalendar(nextSource = source, nextChannel = channel, nextThreshold = threshold, nextMinDurationS = minDurationS) {
    setBusy(true);
    setError("");
    try {
      const response = await getJson<{ days: DriveDay[] }>(
        `/api/calendar?source=${encodeURIComponent(nextSource)}&channel=${encodeURIComponent(nextChannel)}&threshold=${nextThreshold}&minDurationS=${nextMinDurationS}&validOnly=true`,
      );
      setDays(response.days);
      setSelectedDate((current) => (response.days.some((day) => day.date === current) ? current : response.days[0]?.date || ""));
      if (!response.days.length) {
        setSegments([]);
        setSelectedSegmentId("");
      }
    } catch (e) {
      showError(e);
    } finally {
      setBusy(false);
    }
  }

  async function loadSegments(date: string) {
    setBusy(true);
    setError("");
    try {
      const detail = await getJson<{ date: string; sessions: SessionSummary[]; segments: SegmentSummary[] }>(
        `/api/day/${date}?source=${encodeURIComponent(source)}&channel=${encodeURIComponent(channel)}`,
      );
      const responses = await Promise.all(
        detail.sessions.map((session) =>
          getJson<{ segments: SegmentSummary[] }>(
            `/api/segments?source=${encodeURIComponent(source)}&channel=${encodeURIComponent(channel)}&startMs=${session.start_ms}&endMs=${session.end_ms}&threshold=${threshold}&minDurationS=${minDurationS}`,
          ),
        ),
      );
      const nextSegments = responses
        .flatMap((response) => response.segments)
        .sort((a, b) => a.start_ms - b.start_ms)
        .map((segment, index) => ({ ...segment, label: `Session ${index + 1}` }));
      setSegments(nextSegments);
      setSelectedSegmentId((current) => (nextSegments.some((segment) => segment.id === current) ? current : nextSegments[0]?.id || ""));
    } catch (e) {
      showError(e);
    } finally {
      setBusy(false);
    }
  }

  async function refreshStatus() {
    try {
      setStatus(await getJson<ReplayStatus>("/api/simulator/status"));
    } catch {
      setStatus((current) => ({ ...current, running: false, message: "Simulator backend unavailable." }));
    }
  }

  async function startReplay() {
    if (!selectedSegment) return;
    setBusy(true);
    setError("");
    try {
      const nextStatus = await postJson<ReplayStatus>("/api/simulator/start", {
        car: source,
        start_ms: selectedSegment.start_ms,
        end_ms: selectedSegment.end_ms,
        topic: topic.trim() || null,
        transport,
        channel_keys: selectedChannelList,
        speed_multiplier: speedMultiplier,
        frequency_hz: frequencyHz,
        loop,
      });
      setStatus(nextStatus);
    } catch (e) {
      showError(e);
    } finally {
      setBusy(false);
    }
  }

  async function stopReplay() {
    setBusy(true);
    setError("");
    try {
      setStatus(await postJson<ReplayStatus>("/api/simulator/stop", {}));
    } catch (e) {
      showError(e);
    } finally {
      setBusy(false);
    }
  }

  function toggleReplayChannel(key: string) {
    if (requiredChannelKeys.includes(key)) return;
    setSelectedChannelKeys((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next.size ? next : new Set([key]);
    });
  }

  function showError(e: unknown) {
    setError(e instanceof Error ? e.message : String(e));
  }

  return (
    <main className="shell">
      <header className="topbar">
        <div>
          <span className={status.running ? "badge live" : "badge"}><RadioTower size={15} /> {status.running ? "Publishing" : "Standby"}</span>
          <h1>Kafka Replay Simulator</h1>
        </div>
        <div className="statusCard">
          <span>{status.topic || "No topic"}</span>
          <strong>{status.samples_sent.toLocaleString()} samples</strong>
        </div>
      </header>

      {error ? <div className="error">{error}</div> : null}

      <section className="layout">
        <aside className="rail">
          <Panel title="Dataset" icon={<CalendarDays size={18} />}>
            <div className="row">
              <select value={source} onChange={(event) => setSource(event.target.value as "orion" | "angelique")} disabled={status.running}>
                {sources.map((item) => <option key={item.key} value={item.key}>{item.label}</option>)}
              </select>
              <button className="iconButton" onClick={() => loadCalendar()} disabled={busy || status.running} aria-label="Refresh days">
                <RefreshCcw size={16} />
              </button>
            </div>
            <div className="filters">
              <label>
                <span>Split Channel</span>
                <select value={channel} onChange={(event) => setChannel(event.target.value)} disabled={status.running}>
                  {channels.map((item) => <option key={item.key} value={item.key}>{item.label}</option>)}
                </select>
              </label>
              <label>
                <span>Threshold</span>
                <input type="number" value={threshold} onChange={(event) => setThreshold(Number(event.target.value))} disabled={status.running} />
              </label>
              <label>
                <span>Min Length</span>
                <input type="number" min={0} value={minDurationS} onChange={(event) => setMinDurationS(Math.max(0, Number(event.target.value)))} disabled={status.running} />
              </label>
            </div>
            <div className="dayList">
              {days.map((day) => (
                <button key={day.date} className={day.date === selectedDate ? "day selected" : "day"} onClick={() => setSelectedDate(day.date)} disabled={status.running}>
                  <strong>{day.date}</strong>
                  <span>{day.sessions} valid</span>
                </button>
              ))}
              {!days.length ? <small>No valid days for the current split settings.</small> : null}
            </div>
          </Panel>

          <Panel title="Sessions" icon={<Gauge size={18} />}>
            <div className="sessionList">
              {segments.map((segment) => (
                <button key={segment.id} className={segment.id === selectedSegmentId ? "session selected" : "session"} onClick={() => setSelectedSegmentId(segment.id)} disabled={status.running}>
                  <strong>{segment.label}</strong>
                  <span>{formatTime(segment.start_ms)} - {formatDuration(segment.duration_s)}</span>
                </button>
              ))}
              {!segments.length ? <small>Select a day with valid threshold sessions.</small> : null}
            </div>
          </Panel>
        </aside>

        <section className="workspace">
          <Panel title="Replay Control" icon={<Play size={18} />}>
            <div className="controlGrid">
              <Metric label="Source" value={sourceLabel} />
              <Metric label="Selected" value={selectedSegment ? selectedSegment.label : "None"} />
              <Metric label="Range" value={selectedSegment ? `${formatTime(selectedSegment.start_ms)} - ${formatDuration(selectedSegment.duration_s)}` : "--"} />
              <Metric label="Current" value={status.current_ms ? formatDateTime(status.current_ms) : "--"} />
            </div>
            <div className="controlStrip">
              <label>
                <span>Speed</span>
                <select value={speedMultiplier} onChange={(event) => setSpeedMultiplier(Number(event.target.value))} disabled={status.running}>
                  {[1, 2, 5, 10, 20, 50].map((value) => <option key={value} value={value}>{value}x</option>)}
                </select>
              </label>
              <label>
                <span>Hz</span>
                <input type="number" min={1} max={50} value={frequencyHz} onChange={(event) => setFrequencyHz(Math.max(1, Math.min(50, Number(event.target.value))))} disabled={status.running} />
              </label>
              <label>
                <span>Topic</span>
                <input value={topic} placeholder={`grafana_data_${source}`} onChange={(event) => setTopic(event.target.value)} disabled={status.running} />
              </label>
              <label>
                <span>Target</span>
                <select value={transport} onChange={(event) => setTransport(event.target.value as KafkaTransport)} disabled={status.running}>
                  <option value="local">Local replay bus</option>
                  <option value="kafka">Kafka broker</option>
                </select>
              </label>
              <label className="loopBox">
                <input type="checkbox" checked={loop} onChange={(event) => setLoop(event.target.checked)} disabled={status.running} />
                Loop
              </label>
            </div>
            <div className="actions">
              <button className="primary" onClick={startReplay} disabled={!selectedSegment || status.running || busy}>
                <Play size={16} /> Start
              </button>
              <button className="stop" onClick={stopReplay} disabled={!status.running || busy}>
                <Square size={16} /> Stop
              </button>
              <span className="message">{busy ? "Working..." : status.message}</span>
            </div>
          </Panel>

          <Panel title="Payload Channels" icon={<Zap size={18} />}>
            <div className="channelToolbar">
              <button onClick={() => setSelectedChannelKeys(new Set(defaultReplayChannelKeys(channels)))} disabled={status.running}>Core</button>
              <button onClick={() => setSelectedChannelKeys(new Set(withRequiredChannels(channels.map((item) => item.key), channels).slice(0, 18)))} disabled={status.running}>Max 18</button>
              <small>GPS, speed, and DC bus stay selected</small>
              <span>{selectedChannelKeys.size} selected</span>
            </div>
            <div className="channelGrid">
              {channels.map((item) => (
                <label key={item.key} className={selectedChannelKeys.has(item.key) ? "channel checked" : "channel"}>
                  <input
                    type="checkbox"
                    checked={selectedChannelKeys.has(item.key)}
                    onChange={() => toggleReplayChannel(item.key)}
                    disabled={status.running || requiredChannelKeys.includes(item.key)}
                  />
                  <strong>{item.label}</strong>
                  <span>{requiredChannelKeys.includes(item.key) ? "required" : item.unit || item.quantity || item.key}</span>
                </label>
              ))}
            </div>
          </Panel>
        </section>
      </section>
    </main>
  );
}

function Panel({ title, icon, children }: { title: string; icon: React.ReactNode; children: React.ReactNode }) {
  return (
    <section className="panel">
      <div className="panelTitle">{icon}<h2>{title}</h2></div>
      {children}
    </section>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function defaultReplayChannelKeys(channels: ChannelDef[]) {
  const tokens = ["wheel_speed", "motor_speed", "rpm", "torque", "apps", "brake", "hv_pack_v", "hv_c", "dc_bus", "temp"];
  const selected = requiredReplayChannelKeys(channels);
  channels.forEach((channel) => {
    if (channel.default && !selected.includes(channel.key)) selected.push(channel.key);
  });
  channels.forEach((channel) => {
    const haystack = `${channel.key} ${channel.label} ${channel.column}`.toLowerCase();
    if (selected.length < 12 && !selected.includes(channel.key) && tokens.some((token) => haystack.includes(token))) {
      selected.push(channel.key);
    }
  });
  return selected.length ? selected : channels.slice(0, 8).map((channel) => channel.key);
}

function requiredReplayChannelKeys(channels: ChannelDef[]) {
  const required: string[] = [];
  const gpsLatitude = channels.find((channel) => {
    const haystack = `${channel.key} ${channel.label} ${channel.column}`.toLowerCase();
    return haystack.includes("latitude") || channel.column === "gps[1]";
  });
  const gpsLongitude = channels.find((channel) => {
    const haystack = `${channel.key} ${channel.label} ${channel.column}`.toLowerCase();
    return haystack.includes("longitude") || channel.column === "gps[2]";
  });
  const gpsSpeed = channels.find((channel) => {
    const haystack = `${channel.key} ${channel.label} ${channel.column}`.toLowerCase();
    return haystack.includes("gps_speed");
  });
  const dcBusVoltage = channels.find((channel) => {
    const haystack = `${channel.key} ${channel.label} ${channel.column}`.toLowerCase();
    return haystack.includes("dc_bus") && (haystack.includes("voltage") || haystack.endsWith("_v"));
  });
  const dcBusCurrent = channels.find((channel) => {
    const haystack = `${channel.key} ${channel.label} ${channel.column}`.toLowerCase();
    return haystack.includes("dc_bus") && (haystack.includes("current") || haystack.endsWith("_c"));
  });
  const fallbackSpeed = channels.find((channel) => {
    const haystack = `${channel.key} ${channel.label} ${channel.column}`.toLowerCase();
    return haystack.includes("wheel_speed") || haystack.includes("motor_speed") || channel.quantity === "speed";
  });
  [gpsLatitude, gpsLongitude, gpsSpeed ?? fallbackSpeed, dcBusVoltage, dcBusCurrent].forEach((channel) => {
    if (channel && !required.includes(channel.key)) required.push(channel.key);
  });
  return required;
}

function withRequiredChannels(keys: string[], channels: ChannelDef[]) {
  const selected: string[] = [];
  [...requiredReplayChannelKeys(channels), ...keys].forEach((key) => {
    if (!selected.includes(key)) selected.push(key);
  });
  return selected;
}

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(await response.text());
  return response.json() as Promise<T>;
}

async function postJson<T>(url: string, body: unknown): Promise<T> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json() as Promise<T>;
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

export default App;
