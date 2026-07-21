(() => {
  "use strict";

  const root = document.getElementById("triDashboardRoot");
  const summary = document.getElementById("dashboardLiveSummary");
  const deviceCountBadge = document.getElementById("dashboardDeviceCountBadge");
  const section = root?.closest(".tri-dashboard-section");
  const compactToggle = document.getElementById("dashboardCompactToggle");
  if (!root) return;

  const endpoint = root.dataset.mqttEndpoint || "configured MQTT broker";
  const onlineMilliseconds = Number(root.dataset.onlineSeconds || 60) * 1000;
  const MAX_HISTORY = 30;
  const MAX_LOGS = 10;

  const TOPICS = Object.freeze({
    gateway: "digi/gateway/IX20-01/telemetry",
    sensor: "digi/sensor/SS-TEMP-07/telemetry",
    router: "prueba/router/telemetry",
    accessPoint: "prueba/ap/telemetry",
    emqxLogs: "prueba/logs",
    emqxExternal: "prueba/router",
    power: "Power/TEMP/telemetry",
    localLogs: "Power/TEMP/logs",
    localExternal: "Power/TEMP",
  });

  const groups = [
    {
      id: "hivemq",
      name: "HiveMQ Cloud",
      description: "Digi IX20 gateway + SmartSense sensor",
      topicLabel: "digi/#",
      accent: "cyan",
      cards: ["gateway", "sensor"],
    },
    {
      id: "emqx",
      name: "EMQX Cloud",
      description: "Edge router + Wi-Fi access point + log feed",
      topicLabel: "prueba/#",
      accent: "green",
      cards: ["router", "accessPoint", "emqxLogs"],
    },
    {
      id: "local",
      name: "Local Mosquitto",
      description: "Power / PDU monitor + local log feed",
      topicLabel: "Power/#",
      accent: "red",
      cards: ["power", "localLogs"],
    },
  ];

  const cardDefinitions = {
    gateway: {
      group: "hivemq",
      title: "Digi IX20 Gateway",
      topic: TOPICS.gateway,
      accent: "cyan",
      icon: "GW",
      historyField: "cpu_temp_c",
      render: renderGateway,
    },
    sensor: {
      group: "hivemq",
      title: "SmartSense Sensor",
      topic: TOPICS.sensor,
      accent: "magenta",
      icon: "SN",
      historyField: "temperature_c",
      render: renderSensor,
    },
    router: {
      group: "emqx",
      title: "Edge Router",
      topic: TOPICS.router,
      accent: "green",
      icon: "RT",
      historyField: "cpu_temp_c",
      render: renderRouter,
    },
    accessPoint: {
      group: "emqx",
      title: "Wi-Fi AP",
      topic: TOPICS.accessPoint,
      accent: "amber",
      icon: "AP",
      render: renderAccessPoint,
    },
    emqxLogs: {
      group: "emqx",
      title: "EMQX Log Feed",
      topics: [TOPICS.emqxLogs, TOPICS.emqxExternal],
      accent: "slate",
      icon: "LOG",
      logKey: "emqx",
    },
    power: {
      group: "local",
      title: "Power / PDU Monitor",
      topic: TOPICS.power,
      accent: "red",
      icon: "PDU",
      historyField: "temperature_c",
      render: renderPower,
    },
    localLogs: {
      group: "local",
      title: "Local Power Log Feed",
      topics: [TOPICS.localLogs, TOPICS.localExternal],
      accent: "slate",
      icon: "LOG",
      logKey: "local",
    },
  };

  const topicRoutes = new Map([
    [TOPICS.gateway, { type: "device", key: "gateway" }],
    [TOPICS.sensor, { type: "device", key: "sensor" }],
    [TOPICS.router, { type: "device", key: "router" }],
    [TOPICS.accessPoint, { type: "device", key: "accessPoint" }],
    [TOPICS.power, { type: "device", key: "power" }],
    [TOPICS.emqxLogs, { type: "log", key: "emqx", group: "emqx" }],
    [TOPICS.emqxExternal, { type: "log", key: "emqx", group: "emqx", external: true }],
    [TOPICS.localLogs, { type: "log", key: "local", group: "local" }],
    [TOPICS.localExternal, { type: "log", key: "local", group: "local", external: true }],
  ]);

  const state = {
    connected: false,
    devices: new Map(),
    histories: new Map(),
    deviceLastSeen: new Map(),
    logs: { emqx: [], local: [] },
    groupStats: new Map(groups.map((group) => [group.id, { received: 0, lastSeen: null }])),
  };

  const dom = {
    cards: new Map(),
    groups: new Map(),
  };
  let renderTimer = null;

  buildDashboard();
  initializeCompactToggle();

  const socket = window.dashboardSocket;
  if (!socket) {
    renderUnavailable();
    return;
  }

  socket.on("snapshot", (snapshot) => {
    resetState();
    state.connected = Boolean(snapshot?.status?.connected);
    const messages = Array.isArray(snapshot?.messages) ? [...snapshot.messages].reverse() : [];
    messages.forEach(ingestMessage);
    renderAll();
  });

  socket.on("mqtt_status", (status) => {
    state.connected = Boolean(status?.connected);
    renderAll();
  });

  socket.on("mqtt_messages", (messages) => {
    if (!Array.isArray(messages)) return;
    messages.forEach(ingestMessage);
    scheduleRender();
  });

  window.setInterval(renderAll, 5000);

  function resetState() {
    state.devices.clear();
    state.histories.clear();
    state.deviceLastSeen.clear();
    state.logs.emqx = [];
    state.logs.local = [];
    groups.forEach((group) => {
      state.groupStats.set(group.id, { received: 0, lastSeen: null });
    });
  }

  function initializeCompactToggle() {
    if (!section || !compactToggle) return;
    let compact = false;
    try {
      compact = window.localStorage.getItem("triDashboardCompact") === "true";
    } catch (_error) {
      compact = false;
    }
    setCompact(compact);
    compactToggle.addEventListener("click", () => {
      const nextCompact = !section.classList.contains("is-compact");
      setCompact(nextCompact);
      try {
        window.localStorage.setItem("triDashboardCompact", String(nextCompact));
      } catch (_error) {
        // The toggle still works when browser storage is unavailable.
      }
    });
  }

  function setCompact(compact) {
    section.classList.toggle("is-compact", compact);
    compactToggle.setAttribute("aria-expanded", String(!compact));
    compactToggle.setAttribute(
      "aria-label",
      compact ? "Expand dashboards" : "Minimize dashboards"
    );
    compactToggle.title = compact
      ? "Show complete dashboards"
      : "Show compact dashboard values";
  }

  function ingestMessage(message) {
    const route = topicRoutes.get(message?.topic);
    if (!route) return;

    const timestamp = validTimestamp(message.timestamp);
    const groupId = route.group || cardDefinitions[route.key].group;
    const groupStats = state.groupStats.get(groupId);
    groupStats.received += 1;
    groupStats.lastSeen = timestamp;

    if (route.type === "log") {
      const entry = normalizeLog(message, route.external);
      state.logs[route.key].push(entry);
      if (state.logs[route.key].length > MAX_LOGS) state.logs[route.key].shift();
      return;
    }

    const payload = objectPayload(message);
    if (!payload) return;
    state.devices.set(route.key, payload);
    state.deviceLastSeen.set(route.key, timestamp);

    const historyField = cardDefinitions[route.key].historyField;
    if (!historyField) return;
    const value = finiteNumber(payload[historyField]);
    if (value === null) return;
    const history = state.histories.get(route.key) || [];
    history.push(value);
    if (history.length > MAX_HISTORY) history.shift();
    state.histories.set(route.key, history);
  }

  function objectPayload(message) {
    if (message?.is_json && isPlainObject(message.json)) return message.json;
    try {
      const parsed = JSON.parse(message?.payload || "");
      return isPlainObject(parsed) ? parsed : null;
    } catch (_error) {
      return null;
    }
  }

  function normalizeLog(message, external) {
    const payload = objectPayload(message);
    const severity = external ? "EXT" : normalizeSeverity(payload?.sev);
    return {
      severity,
      message: external ? String(message.payload ?? "") : String(payload?.msg ?? message.payload ?? ""),
      time: String(payload?.ts || formatClock(message.timestamp)),
      timestamp: validTimestamp(message.timestamp),
    };
  }

  function normalizeSeverity(value) {
    const normalized = String(value || "INFO").toUpperCase();
    return ["INFO", "WARN", "ERR", "EXT"].includes(normalized) ? normalized : "INFO";
  }

  function buildDashboard() {
    const fragment = document.createDocumentFragment();
    groups.forEach((group) => {
      const section = document.createElement("section");
      section.className = `broker-dashboard broker-dashboard-${group.accent}`;

      const header = document.createElement("header");
      header.className = "broker-dashboard-header";

      const identity = document.createElement("div");
      identity.className = "broker-dashboard-identity";
      const titleRow = document.createElement("div");
      titleRow.className = "broker-dashboard-title-row";
      const marker = document.createElement("span");
      marker.className = "broker-dashboard-marker";
      marker.setAttribute("aria-hidden", "true");
      const title = document.createElement("h3");
      title.textContent = group.name;
      titleRow.append(marker, title);
      const description = document.createElement("p");
      description.textContent = group.description;
      identity.append(titleRow, description);

      const connection = document.createElement("div");
      connection.className = "broker-dashboard-connection";
      const endpointLabel = document.createElement("code");
      endpointLabel.textContent = `${endpoint} · ${group.topicLabel}`;
      const stateBadge = document.createElement("span");
      stateBadge.className = "broker-activity-badge";
      const stateDot = document.createElement("span");
      stateDot.className = "status-dot";
      const stateText = document.createElement("span");
      stateText.textContent = "Waiting";
      stateBadge.append(stateDot, stateText);
      connection.append(endpointLabel, stateBadge);

      const counters = document.createElement("div");
      counters.className = "broker-dashboard-counters";
      const received = counter("RX in view", "0");
      const updated = counter("Last update", "—");
      counters.append(received.element, updated.element);
      header.append(identity, connection, counters);

      const grid = document.createElement("div");
      grid.className = `broker-card-grid broker-card-grid-${group.id}`;
      group.cards.forEach((cardKey) => grid.append(buildCard(cardKey)));
      section.append(header, grid);
      fragment.append(section);

      dom.groups.set(group.id, {
        stateBadge,
        stateText,
        received: received.value,
        updated: updated.value,
      });
    });
    root.replaceChildren(fragment);
  }

  function buildCard(cardKey) {
    const definition = cardDefinitions[cardKey];
    const card = document.createElement("article");
    const kind = definition.logKey ? "log" : "device";
    card.className = `telemetry-card telemetry-card-${definition.accent} telemetry-card-${kind}`;
    card.dataset.dashboardCard = cardKey;

    const header = document.createElement("header");
    header.className = "telemetry-card-header";
    const icon = document.createElement("span");
    icon.className = "telemetry-card-icon";
    icon.textContent = definition.icon;
    const identity = document.createElement("div");
    identity.className = "telemetry-card-identity";
    const title = document.createElement("h4");
    title.textContent = definition.title;
    const topic = document.createElement("code");
    topic.textContent = definition.topic || definition.topics.join(" · ");
    topic.title = topic.textContent;
    identity.append(title, topic);
    const activity = document.createElement("span");
    activity.className = "telemetry-card-activity";
    activity.textContent = "Waiting";
    header.append(icon, identity, activity);

    const body = document.createElement("div");
    body.className = "telemetry-card-body";
    card.append(header, body);
    dom.cards.set(cardKey, { activity, body });
    return card;
  }

  function counter(label, initialValue) {
    const element = document.createElement("div");
    const labelElement = document.createElement("span");
    labelElement.textContent = label;
    const value = document.createElement("strong");
    value.textContent = initialValue;
    element.append(labelElement, value);
    return { element, value };
  }

  function renderAll() {
    renderTimer = null;
    Object.keys(cardDefinitions).forEach(renderCard);
    renderGroupStatuses();
    renderSummary();
  }

  function renderCard(cardKey) {
    const definition = cardDefinitions[cardKey];
    const cardDom = dom.cards.get(cardKey);
    if (definition.logKey) {
      renderLogs(definition, cardDom);
      return;
    }

    const payload = state.devices.get(cardKey);
    const lastSeen = state.deviceLastSeen.get(cardKey);
    const live = isRecent(lastSeen);
    cardDom.activity.className = `telemetry-card-activity${live ? " is-live" : ""}`;
    cardDom.activity.textContent = payload ? (live ? "Live" : "Idle") : "Waiting";

    if (!payload) {
      cardDom.body.replaceChildren(waitingState(definition.topic));
      return;
    }

    const content = definition.render(payload, state.histories.get(cardKey) || []);
    const footer = document.createElement("div");
    footer.className = "telemetry-card-footer";
    footer.textContent = `Last telemetry ${formatRelativeTime(lastSeen)}`;
    content.append(footer);
    cardDom.body.replaceChildren(content);
  }

  function renderLogs(definition, cardDom) {
    const entries = state.logs[definition.logKey];
    const latest = entries.at(-1)?.timestamp;
    const live = isRecent(latest);
    cardDom.activity.className = `telemetry-card-activity${live ? " is-live" : ""}`;
    cardDom.activity.textContent = entries.length ? (live ? "Streaming" : "Idle") : "Waiting";
    if (!entries.length) {
      cardDom.body.replaceChildren(waitingState(definition.topics.join(" or ")));
      return;
    }

    const list = document.createElement("div");
    list.className = "telemetry-log-list";
    entries.slice(-8).reverse().forEach((entry) => {
      const row = document.createElement("div");
      row.className = "telemetry-log-row";
      const time = document.createElement("time");
      time.textContent = entry.time;
      time.dateTime = entry.timestamp;
      const severity = document.createElement("span");
      severity.className = `telemetry-log-severity severity-${entry.severity.toLowerCase()}`;
      severity.textContent = entry.severity;
      const message = document.createElement("span");
      message.className = "telemetry-log-message";
      message.textContent = entry.message;
      message.title = entry.message;
      row.append(time, severity, message);
      list.append(row);
    });
    cardDom.body.replaceChildren(list);
  }

  function renderGroupStatuses() {
    groups.forEach((group) => {
      const stats = state.groupStats.get(group.id);
      const groupDom = dom.groups.get(group.id);
      const live = state.connected && isRecent(stats.lastSeen);
      const idle = state.connected && !live;
      groupDom.stateBadge.className = `broker-activity-badge${live ? " is-live" : ""}${idle ? " is-idle" : ""}`;
      groupDom.stateText.textContent = live ? "Live" : (idle ? "Listening" : "Offline");
      groupDom.received.textContent = formatNumber(stats.received);
      groupDom.updated.textContent = stats.lastSeen ? formatRelativeTime(stats.lastSeen) : "—";
    });
  }

  function renderSummary() {
    const reporting = state.devices.size;
    const received = [...state.groupStats.values()]
      .reduce((total, group) => total + group.received, 0);
    deviceCountBadge.textContent = `${reporting}/5`;
    summary.classList.toggle("is-connected", state.connected);
    summary.lastElementChild.textContent = state.connected
      ? `${reporting}/5 devices reporting · ${formatNumber(received)} matching messages`
      : "MQTT disconnected";
  }

  function renderUnavailable() {
    summary.lastElementChild.textContent = "Browser transport unavailable";
    Object.keys(cardDefinitions).forEach((cardKey) => {
      const definition = cardDefinitions[cardKey];
      const topic = definition.topic || definition.topics.join(" or ");
      dom.cards.get(cardKey).body.replaceChildren(waitingState(topic));
    });
  }

  function renderGateway(data, history) {
    const fragment = document.createDocumentFragment();
    fragment.append(
      identityBlock(
        data.device_id || "IX20-01",
        compact([data.network, data.operator, data.band], " · ")
      ),
      metricRow("Signal", signalBars(data.rsrp_dbm)),
      metricRow("CPU temperature", temperature(data.cpu_temp_c, "cpu")),
      metricRow("CPU utilization", gauge(data.cpu_util_pct)),
      metricRow("Memory utilization", gauge(data.mem_util_pct)),
      metricRow("WAN throughput", throughput(data.wan_rx_mbps, data.wan_tx_mbps)),
      metricRow("CPU trend", sparkline(history, "CPU temperature"))
    );
    return fragment;
  }

  function renderSensor(data, history) {
    const fragment = document.createDocumentFragment();
    const door = valueText(data.door_open ? "OPEN" : "Closed", data.door_open ? "bad" : "good");
    const battery = finiteNumber(data.battery_pct);
    fragment.append(
      identityBlock(data.sensor_id || "SS-TEMP-07", data.probe),
      metricRow("Temperature", temperature(data.temperature_c, "cold")),
      metricRow("Humidity", gauge(data.humidity_pct)),
      metricRow("Door", door),
      metricRow(
        "Battery",
        valueText(
          `${formatDecimal(battery, 1, "%")} · ${formatDecimal(data.battery_v, 2, " V")}`,
          battery === null ? "muted" : (battery > 40 ? "good" : (battery > 15 ? "warn" : "bad"))
        )
      ),
      metricRow("Temperature trend", sparkline(history, "Sensor temperature"))
    );
    return fragment;
  }

  function renderRouter(data, history) {
    const fragment = document.createDocumentFragment();
    const loss = finiteNumber(data.packet_loss_pct);
    fragment.append(
      identityBlock(data.device_id || "EDGE-RTR-01", compact([data.model, data.firmware], " · ")),
      metricRow("WAN throughput", throughput(data.wan_rx_mbps, data.wan_tx_mbps)),
      metricRow("LAN clients", valueText(formatInteger(data.lan_clients))),
      metricRow(
        "Latency / loss",
        valueText(
          `${formatDecimal(data.wan_latency_ms, 0, " ms")} · ${formatDecimal(loss, 1, "%")}`,
          loss === null ? "muted" : (loss < 1 ? "good" : (loss < 3 ? "warn" : "bad"))
        )
      ),
      metricRow("CPU temperature", temperature(data.cpu_temp_c, "cpu")),
      metricRow("CPU utilization", gauge(data.cpu_util_pct)),
      metricRow("CPU trend", sparkline(history, "Router CPU temperature"))
    );
    return fragment;
  }

  function renderAccessPoint(data) {
    const fragment = document.createDocumentFragment();
    const retries = finiteNumber(data.retries_pct);
    fragment.append(
      identityBlock(data.ap_id || "WIFI-AP-03", compact([data.band, channelLabel(data.channel)], " · ")),
      metricRow("Stations", valueText(formatInteger(data.stations))),
      metricRow("TX power", valueText(formatDecimal(data.tx_power_dbm, 0, " dBm"))),
      metricRow("Noise floor", valueText(formatDecimal(data.noise_floor_dbm, 0, " dBm"))),
      metricRow(
        "Retries",
        valueText(
          formatDecimal(retries, 1, "%"),
          retries === null ? "muted" : (retries < 6 ? "good" : (retries < 12 ? "warn" : "bad"))
        )
      ),
      metricRow("Throughput", valueText(formatDecimal(data.throughput_mbps, 0, " Mbps")))
    );
    return fragment;
  }

  function renderPower(data, history) {
    const fragment = document.createDocumentFragment();
    fragment.append(
      identityBlock(data.device_id || "PDU-RACK-A1", compact([data.phase, data.firmware], " · ")),
      metricRow(
        "Voltage / frequency",
        valueText(`${formatDecimal(data.voltage_v, 1, " V")} · ${formatDecimal(data.frequency_hz, 2, " Hz")}`)
      ),
      metricRow("Current", valueText(formatDecimal(data.current_a, 1, " A"))),
      metricRow(
        "Power / factor",
        valueText(`${formatDecimal(data.power_w, 0, " W")} · pf ${formatDecimal(data.power_factor, 2)}`)
      ),
      metricRow("Load", gauge(data.load_pct)),
      metricRow("Inlet temperature", temperature(data.temperature_c, "pdu")),
      metricRow("Energy", valueText(formatDecimal(data.energy_kwh, 3, " kWh"))),
      metricRow("Temperature trend", sparkline(history, "PDU inlet temperature"))
    );
    return fragment;
  }

  function identityBlock(primary, secondary) {
    const identity = document.createElement("div");
    identity.className = "telemetry-device-identity";
    const name = document.createElement("strong");
    name.textContent = primary || "Unknown device";
    const detail = document.createElement("span");
    detail.textContent = secondary || "Telemetry online";
    identity.append(name, detail);
    return identity;
  }

  function metricRow(label, value) {
    const row = document.createElement("div");
    row.className = "telemetry-metric-row";
    const name = document.createElement("span");
    name.className = "telemetry-metric-label";
    name.textContent = label;
    const content = document.createElement("div");
    content.className = "telemetry-metric-value";
    content.append(typeof value === "string" ? document.createTextNode(value) : value);
    row.append(name, content);
    return row;
  }

  function valueText(text, tone = "default") {
    const value = document.createElement("span");
    value.className = `telemetry-value tone-${tone}`;
    value.textContent = text;
    return value;
  }

  function gauge(value) {
    const number = finiteNumber(value);
    const percentage = number === null ? 0 : Math.min(100, Math.max(0, number));
    const tone = number === null ? "muted" : (percentage < 60 ? "good" : (percentage < 85 ? "warn" : "bad"));
    const wrapper = document.createElement("div");
    wrapper.className = "telemetry-gauge";
    const track = document.createElement("span");
    track.className = "telemetry-gauge-track";
    const fill = document.createElement("span");
    fill.className = `telemetry-gauge-fill tone-${tone}`;
    fill.style.width = `${percentage}%`;
    track.append(fill);
    wrapper.append(track, valueText(formatDecimal(number, 1, "%"), tone));
    return wrapper;
  }

  function signalBars(value) {
    const rsrp = finiteNumber(value);
    const strength = rsrp === null ? 0 : Math.round(Math.min(1, Math.max(0, (rsrp + 115) / 40)) * 5);
    const tone = strength <= 1 ? "bad" : (strength <= 3 ? "warn" : "good");
    const wrapper = document.createElement("div");
    wrapper.className = "telemetry-signal";
    const bars = document.createElement("span");
    bars.className = `telemetry-signal-bars tone-${tone}`;
    for (let index = 1; index <= 5; index += 1) {
      const bar = document.createElement("i");
      bar.className = index <= strength ? "is-filled" : "";
      bars.append(bar);
    }
    wrapper.append(bars, valueText(formatDecimal(rsrp, 0, " dBm"), tone));
    return wrapper;
  }

  function throughput(received, transmitted) {
    const wrapper = document.createElement("div");
    wrapper.className = "telemetry-throughput";
    const down = document.createElement("span");
    down.className = "throughput-down";
    down.textContent = `↓ ${formatDecimal(received, 0)}`;
    const up = document.createElement("span");
    up.className = "throughput-up";
    up.textContent = `↑ ${formatDecimal(transmitted, 0)}`;
    const unit = document.createElement("small");
    unit.textContent = "Mbps";
    wrapper.append(down, up, unit);
    return wrapper;
  }

  function temperature(value, mode) {
    const number = finiteNumber(value);
    let tone = "muted";
    if (number !== null && mode === "cold") tone = number <= 2 ? "good" : (number <= 5 ? "warn" : "bad");
    if (number !== null && mode === "pdu") tone = number < 40 ? "good" : (number < 50 ? "warn" : "bad");
    if (number !== null && mode === "cpu") tone = number < 60 ? "good" : (number < 72 ? "warn" : "bad");
    return valueText(formatDecimal(number, 1, " °C"), tone);
  }

  function sparkline(values, label) {
    const wrapper = document.createElement("div");
    wrapper.className = "telemetry-sparkline";
    if (!values.length) {
      wrapper.textContent = "—";
      return wrapper;
    }

    const width = 170;
    const height = 32;
    const minimum = Math.min(...values);
    const maximum = Math.max(...values);
    const range = maximum - minimum || 1;
    const points = values.map((value, index) => {
      const x = values.length === 1 ? width : index * (width / (values.length - 1));
      const y = height - 3 - ((value - minimum) / range) * (height - 6);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(" ");

    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("role", "img");
    svg.setAttribute("aria-label", `${label}: ${formatDecimal(values.at(-1), 1, " degrees Celsius")}`);
    const line = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
    line.setAttribute("points", points);
    svg.append(line);
    const current = document.createElement("span");
    current.textContent = formatDecimal(values.at(-1), 1, "°");
    wrapper.append(svg, current);
    return wrapper;
  }

  function waitingState(topic) {
    const waiting = document.createElement("div");
    waiting.className = "telemetry-waiting";
    const icon = document.createElement("span");
    icon.textContent = "⌁";
    icon.setAttribute("aria-hidden", "true");
    const title = document.createElement("strong");
    title.textContent = "Waiting for telemetry";
    const detail = document.createElement("code");
    detail.textContent = topic;
    detail.title = topic;
    waiting.append(icon, title, detail);
    return waiting;
  }

  function scheduleRender() {
    if (renderTimer) return;
    renderTimer = window.setTimeout(renderAll, 80);
  }

  function isRecent(timestamp) {
    if (!timestamp) return false;
    return Date.now() - new Date(timestamp).getTime() <= onlineMilliseconds;
  }

  function validTimestamp(value) {
    const timestamp = new Date(value || Date.now());
    return Number.isNaN(timestamp.getTime()) ? new Date().toISOString() : timestamp.toISOString();
  }

  function formatRelativeTime(value) {
    if (!value) return "—";
    const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
    if (seconds < 5) return "just now";
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
  }

  function formatClock(value) {
    const date = new Date(value || Date.now());
    if (Number.isNaN(date.getTime())) return "--:--:--";
    return date.toLocaleTimeString([], {
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function formatDecimal(value, digits = 1, suffix = "") {
    const number = finiteNumber(value);
    return number === null ? "—" : `${number.toFixed(digits)}${suffix}`;
  }

  function formatInteger(value) {
    const number = finiteNumber(value);
    return number === null ? "—" : formatNumber(Math.round(number));
  }

  function formatNumber(value) {
    return new Intl.NumberFormat().format(value);
  }

  function finiteNumber(value) {
    if (value === null || value === undefined || value === "") return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function compact(values, separator) {
    return values.filter((value) => value !== null && value !== undefined && value !== "").join(separator);
  }

  function channelLabel(value) {
    return value === null || value === undefined || value === "" ? "" : `channel ${value}`;
  }

  function isPlainObject(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  }
})();
