(() => {
  "use strict";

  const messageLimit = Number(document.body.dataset.messageLimit || 1000);
  const deviceOnlineMilliseconds =
    Number(document.body.dataset.deviceOnlineSeconds || 60) * 1000;
  const state = {
    messages: [],
    messageIds: new Set(),
    topics: new Map(),
    devices: new Map(),
    system: new Map(),
    stats: {},
    paused: false,
    pendingWhilePaused: [],
  };

  const elements = {
    globalSearch: document.getElementById("globalSearch"),
    topicSearch: document.getElementById("topicSearch"),
    feedBody: document.getElementById("feedBody"),
    feedEmpty: document.getElementById("feedEmpty"),
    topicBody: document.getElementById("topicBody"),
    topicEmpty: document.getElementById("topicEmpty"),
    deviceGrid: document.getElementById("deviceGrid"),
    deviceEmpty: document.getElementById("deviceEmpty"),
    systemGrid: document.getElementById("systemGrid"),
    systemEmpty: document.getElementById("systemEmpty"),
    mqttIndicator: document.getElementById("mqttIndicator"),
    mqttIndicatorText: document.getElementById("mqttIndicatorText"),
    brokerStatusValue: document.getElementById("brokerStatusValue"),
    brokerStatusDetail: document.getElementById("brokerStatusDetail"),
    brokerCardDot: document.getElementById("brokerCardDot"),
    statusCard: document.querySelector(".status-card"),
    totalMessages: document.getElementById("totalMessages"),
    messagesPerSecond: document.getElementById("messagesPerSecond"),
    uniqueTopics: document.getElementById("uniqueTopics"),
    inferredDevices: document.getElementById("inferredDevices"),
    lastMessage: document.getElementById("lastMessage"),
    lastMessageDate: document.getElementById("lastMessageDate"),
    dashboardUptime: document.getElementById("dashboardUptime"),
    transportWarning: document.getElementById("transportWarning"),
    feedCountBadge: document.getElementById("feedCountBadge"),
    topicCountBadge: document.getElementById("topicCountBadge"),
    deviceCountBadge: document.getElementById("deviceCountBadge"),
    systemCountBadge: document.getElementById("systemCountBadge"),
    pauseButton: document.getElementById("pauseButton"),
    streamState: document.querySelector(".stream-state"),
    topicDrawer: document.getElementById("topicDrawer"),
    drawerBackdrop: document.getElementById("drawerBackdrop"),
    drawerClose: document.getElementById("drawerClose"),
    drawerTitle: document.getElementById("drawerTitle"),
    drawerCount: document.getElementById("drawerCount"),
    drawerSize: document.getElementById("drawerSize"),
    drawerUpdated: document.getElementById("drawerUpdated"),
    drawerEncoding: document.getElementById("drawerEncoding"),
    drawerRaw: document.getElementById("drawerRaw"),
    jsonSection: document.getElementById("jsonSection"),
    jsonViewer: document.getElementById("jsonViewer"),
  };

  let aggregateRenderTimer = null;
  let filteredRenderTimer = null;

  if (typeof window.io !== "function") {
    elements.transportWarning.textContent =
      "The Socket.IO browser client could not be loaded. Check network access to cdn.socket.io.";
    elements.transportWarning.classList.remove("d-none");
    return;
  }

  const socket = window.io({
    transports: ["websocket"],
    upgrade: false,
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 5000,
  });

  socket.on("connect", () => {
    elements.transportWarning.classList.add("d-none");
  });

  socket.on("disconnect", () => {
    elements.transportWarning.classList.remove("d-none");
  });

  socket.on("snapshot", (snapshot) => {
    state.messages = snapshot.messages || [];
    state.messageIds = new Set(state.messages.map((message) => message.id));
    state.topics = new Map((snapshot.topics || []).map((topic) => [topic.topic, topic]));
    state.devices = new Map((snapshot.devices || []).map((device) => [device.name, device]));
    state.system = new Map((snapshot.system || []).map((entry) => [entry.device, entry]));
    state.stats = snapshot.stats || {};
    updateMqttStatus(snapshot.status || { connected: false, detail: "Unknown status" });
    updateStats(state.stats);
    renderAll();
  });

  socket.on("mqtt_status", updateMqttStatus);
  socket.on("stats", (stats) => {
    state.stats = stats;
    updateStats(stats);
    refreshDeviceOnlineStatus();
  });

  socket.on("system", (entries) => {
    if (!Array.isArray(entries)) return;
    state.system = new Map(entries.map((entry) => [entry.device, entry]));
    updateBadges();
    renderSystem();
  });

  socket.on("mqtt_messages", (messages) => {
    if (!Array.isArray(messages) || messages.length === 0) return;
    if (state.paused) {
      state.pendingWhilePaused.push(...messages);
      if (state.pendingWhilePaused.length > messageLimit) {
        state.pendingWhilePaused.splice(0, state.pendingWhilePaused.length - messageLimit);
      }
      elements.pauseButton.textContent = `Resume (${state.pendingWhilePaused.length})`;
      return;
    }
    applyMessages(messages);
  });

  function applyMessages(messages) {
    const uniqueMessages = messages.filter((message) => {
      if (state.messageIds.has(message.id)) return false;
      state.messageIds.add(message.id);
      return true;
    });
    if (uniqueMessages.length === 0) return;

    const newestFirst = [...uniqueMessages].reverse();
    state.messages.unshift(...newestFirst);
    if (state.messages.length > messageLimit) {
      const removed = state.messages.splice(messageLimit);
      removed.forEach((message) => state.messageIds.delete(message.id));
    }

    uniqueMessages.forEach(updateAggregatesFromMessage);
    updateBadges();

    if (activeFilter()) {
      scheduleFilteredRender();
    } else {
      prependFeedRows(newestFirst);
    }
    scheduleAggregateRender();
  }

  function updateAggregatesFromMessage(message) {
    const existingTopic = state.topics.get(message.topic);
    state.topics.set(message.topic, {
      topic: message.topic,
      message_count: (existingTopic?.message_count || 0) + 1,
      last_updated: message.timestamp,
      last_payload: message.payload,
      payload_size: message.payload_size,
      payload_encoding: message.payload_encoding,
      json: message.json,
      is_json: message.is_json,
    });

    const existingDevice = state.devices.get(message.device);
    state.devices.set(message.device, {
      name: message.device,
      last_seen: message.timestamp,
      total_messages: (existingDevice?.total_messages || 0) + 1,
      last_topic: message.topic,
      last_payload: message.payload,
      online: true,
    });
  }

  function updateMqttStatus(status) {
    const connected = Boolean(status.connected);
    elements.mqttIndicator.classList.toggle("is-connected", connected);
    elements.mqttIndicator.classList.toggle("is-disconnected", !connected);
    elements.mqttIndicator.title = status.detail || "";
    elements.mqttIndicatorText.textContent = connected ? "Connected" : "Disconnected";
    elements.brokerStatusValue.textContent = connected ? "Connected" : "Disconnected";
    elements.brokerStatusValue.parentElement.classList.toggle("is-connected", connected);
    elements.statusCard.classList.toggle("is-connected", connected);
    elements.brokerStatusDetail.textContent = status.detail || "No connection detail";
  }

  function updateStats(stats) {
    elements.totalMessages.textContent = formatNumber(stats.total_messages || 0);
    elements.messagesPerSecond.textContent = formatNumber(stats.messages_per_second || 0);
    elements.uniqueTopics.textContent = formatNumber(stats.unique_topics || 0);
    elements.inferredDevices.textContent = formatNumber(stats.inferred_devices || 0);
    elements.dashboardUptime.textContent = formatDuration(stats.uptime_seconds || 0);

    if (stats.last_message_timestamp) {
      const date = new Date(stats.last_message_timestamp);
      elements.lastMessage.textContent = formatTime(date, true);
      elements.lastMessageDate.textContent = date.toLocaleDateString([], {
        month: "short",
        day: "numeric",
        year: "numeric",
      });
    } else {
      elements.lastMessage.textContent = "—";
      elements.lastMessageDate.textContent = "No messages received";
    }
  }

  function renderAll() {
    renderFeed();
    renderTopics();
    renderDevices();
    renderSystem();
    updateBadges();
  }

  function renderFeed() {
    const query = normalizedGlobalQuery();
    const fragment = document.createDocumentFragment();
    const visible = query
      ? state.messages.filter((message) => messageMatches(message, query))
      : state.messages;

    visible.forEach((message) => fragment.append(createFeedRow(message)));
    elements.feedBody.replaceChildren(fragment);
    elements.feedEmpty.hidden = visible.length > 0;
  }

  function prependFeedRows(messages) {
    const fragment = document.createDocumentFragment();
    messages.forEach((message) => fragment.append(createFeedRow(message, true)));
    elements.feedBody.prepend(fragment);
    while (elements.feedBody.children.length > messageLimit) {
      elements.feedBody.lastElementChild.remove();
    }
    elements.feedEmpty.hidden = elements.feedBody.children.length > 0;
  }

  function createFeedRow(message, isNew = false) {
    const row = document.createElement("tr");
    if (isNew) row.classList.add("is-new");
    row.dataset.messageId = String(message.id);

    const timestamp = document.createElement("td");
    timestamp.className = "timestamp-cell";
    timestamp.textContent = formatTimestamp(message.timestamp);
    timestamp.title = message.timestamp;

    const topicCell = document.createElement("td");
    const topicButton = document.createElement("button");
    topicButton.type = "button";
    topicButton.className = "topic-button";
    topicButton.dataset.topic = message.topic;
    topicButton.textContent = message.topic || "(empty topic)";
    topicButton.title = message.topic;
    topicCell.append(topicButton);

    const payload = document.createElement("td");
    payload.className = `payload-cell${message.is_json ? " is-json" : ""}`;
    payload.textContent = message.payload;
    payload.title = message.payload;

    row.append(timestamp, topicCell, payload);
    return row;
  }

  function renderTopics() {
    const globalQuery = normalizedGlobalQuery();
    const topicQuery = elements.topicSearch.value.trim().toLowerCase();
    const topics = [...state.topics.values()]
      .filter((topic) => {
        const searchText = `${topic.topic} ${topic.last_payload}`.toLowerCase();
        return (!globalQuery || searchText.includes(globalQuery))
          && (!topicQuery || searchText.includes(topicQuery));
      })
      .sort((a, b) => b.message_count - a.message_count || a.topic.localeCompare(b.topic));

    const fragment = document.createDocumentFragment();
    topics.forEach((topic) => {
      const row = document.createElement("tr");
      row.dataset.topic = topic.topic;
      row.tabIndex = 0;

      const name = document.createElement("td");
      const topicText = document.createElement("button");
      topicText.type = "button";
      topicText.className = "topic-button";
      topicText.dataset.topic = topic.topic;
      topicText.textContent = topic.topic || "(empty topic)";
      name.append(topicText);

      const count = document.createElement("td");
      count.className = "count-cell";
      count.textContent = formatNumber(topic.message_count);

      const updated = document.createElement("td");
      updated.className = "timestamp-cell";
      updated.textContent = formatTimestamp(topic.last_updated);

      const action = document.createElement("td");
      action.className = "row-action";
      action.textContent = "›";

      row.append(name, count, updated, action);
      fragment.append(row);
    });

    elements.topicBody.replaceChildren(fragment);
    elements.topicEmpty.hidden = topics.length > 0;
  }

  function renderDevices() {
    const query = normalizedGlobalQuery();
    const devices = [...state.devices.values()]
      .filter((device) => {
        if (!query) return true;
        return `${device.name} ${device.last_topic} ${device.last_payload}`
          .toLowerCase()
          .includes(query);
      })
      .sort((a, b) => new Date(b.last_seen) - new Date(a.last_seen));

    const fragment = document.createDocumentFragment();
    devices.forEach((device) => fragment.append(createDeviceCard(device)));
    elements.deviceGrid.replaceChildren(fragment);
    elements.deviceEmpty.hidden = devices.length > 0;
  }

  function createDeviceCard(device) {
    const card = document.createElement("article");
    card.className = "device-card";
    card.dataset.device = device.name;

    const header = document.createElement("div");
    header.className = "device-card-header";
    const name = document.createElement("h3");
    name.className = "device-name";
    name.textContent = device.name;
    name.title = device.name;
    const badge = document.createElement("span");
    badge.className = `online-badge${device.online ? "" : " offline"}`;
    const dot = document.createElement("span");
    dot.className = "status-dot";
    const badgeText = document.createElement("span");
    badgeText.textContent = device.online ? "Online" : "Offline";
    badge.append(dot, badgeText);
    header.append(name, badge);

    const metrics = document.createElement("div");
    metrics.className = "device-metrics";
    metrics.append(
      deviceMetric("Messages", formatNumber(device.total_messages)),
      deviceMetric("Last seen", formatRelativeTime(device.last_seen))
    );

    const topic = document.createElement("div");
    topic.className = "device-field";
    const topicLabel = document.createElement("span");
    topicLabel.textContent = "Last topic";
    const topicValue = document.createElement("code");
    topicValue.textContent = device.last_topic;
    topicValue.title = device.last_topic;
    topic.append(topicLabel, topicValue);

    const payload = document.createElement("div");
    payload.className = "device-field";
    const payloadLabel = document.createElement("span");
    payloadLabel.textContent = "Last payload";
    const payloadValue = document.createElement("div");
    payloadValue.className = "device-payload";
    payloadValue.textContent = oneLine(device.last_payload);
    payloadValue.title = device.last_payload;
    payload.append(payloadLabel, payloadValue);

    card.append(header, metrics, topic, payload);
    return card;
  }

  function deviceMetric(label, value) {
    const metric = document.createElement("div");
    metric.className = "device-metric";
    const labelElement = document.createElement("span");
    labelElement.textContent = label;
    const valueElement = document.createElement("strong");
    valueElement.textContent = value;
    metric.append(labelElement, valueElement);
    return metric;
  }

  function renderSystem() {
    const query = normalizedGlobalQuery();
    const entries = [...state.system.values()]
      .filter((entry) => !query || entry.device.toLowerCase().includes(query))
      .sort((a, b) => new Date(b.last_seen) - new Date(a.last_seen));

    const fragment = document.createDocumentFragment();
    entries.forEach((entry) => fragment.append(createSystemCard(entry)));
    elements.systemGrid.replaceChildren(fragment);
    elements.systemEmpty.hidden = entries.length > 0;
  }

  function createSystemCard(entry) {
    const metrics = entry.metrics || {};
    const card = document.createElement("article");
    card.className = "device-card system-card";
    card.dataset.device = entry.device;

    const header = document.createElement("div");
    header.className = "device-card-header";
    const name = document.createElement("h3");
    name.className = "device-name";
    name.textContent = entry.device;
    name.title = entry.device;
    const badge = document.createElement("span");
    badge.className = `online-badge${entry.online ? "" : " offline"}`;
    const dot = document.createElement("span");
    dot.className = "status-dot";
    const badgeText = document.createElement("span");
    badgeText.textContent = entry.online ? "Online" : "Offline";
    badge.append(dot, badgeText);
    header.append(name, badge);

    const load = metrics.load_avg || {};
    const loadRow = document.createElement("div");
    loadRow.className = "device-metrics system-load";
    loadRow.append(
      deviceMetric("Load 1m", formatMetric(load["1min"])),
      deviceMetric("Load 5m", formatMetric(load["5min"])),
      deviceMetric("Load 15m", formatMetric(load["15min"]))
    );

    const ramRow = document.createElement("div");
    ramRow.className = "device-metrics";
    ramRow.append(deviceMetric("RAM", formatMetric(metrics.ram)));

    const disks = document.createElement("div");
    disks.className = "system-disks";
    const disksLabel = document.createElement("span");
    disksLabel.className = "system-disks-label";
    disksLabel.textContent = "Disk usage";
    disks.append(disksLabel);

    const diskList = metrics.disks || [];
    if (diskList.length === 0) {
      const none = document.createElement("div");
      none.className = "system-disk-row is-empty";
      none.textContent = "No disks reported";
      disks.append(none);
    } else {
      diskList.forEach((disk) => {
        const row = document.createElement("div");
        row.className = "system-disk-row";
        const mount = document.createElement("code");
        mount.textContent = disk.mount;
        mount.title = disk.mount;
        const value = document.createElement("strong");
        value.textContent = formatMetric(disk.value);
        row.append(mount, value);
        disks.append(row);
      });
    }

    card.append(header, loadRow, ramRow, disks);
    return card;
  }

  function formatMetric(value) {
    if (value === null || value === undefined) return "—";
    return formatNumber(value);
  }

  function openTopicDrawer(topicName) {
    const topic = state.topics.get(topicName);
    if (!topic) return;

    elements.drawerTitle.textContent = topic.topic || "(empty topic)";
    elements.drawerTitle.title = topic.topic;
    elements.drawerCount.textContent = formatNumber(topic.message_count);
    elements.drawerSize.textContent = formatBytes(topic.payload_size);
    elements.drawerUpdated.textContent = formatTimestamp(topic.last_updated);
    elements.drawerUpdated.title = topic.last_updated;
    elements.drawerEncoding.textContent = topic.payload_encoding || "utf-8";
    elements.drawerRaw.textContent = topic.last_payload;

    elements.jsonViewer.replaceChildren();
    elements.jsonSection.hidden = !topic.is_json;
    if (topic.is_json) {
      elements.jsonViewer.append(buildJsonTree(topic.json, 0));
    }

    elements.drawerBackdrop.hidden = false;
    elements.topicDrawer.classList.add("is-open");
    elements.topicDrawer.setAttribute("aria-hidden", "false");
    elements.drawerClose.focus();
  }

  function closeTopicDrawer() {
    elements.topicDrawer.classList.remove("is-open");
    elements.topicDrawer.setAttribute("aria-hidden", "true");
    elements.drawerBackdrop.hidden = true;
  }

  function buildJsonTree(value, depth) {
    if (value !== null && typeof value === "object") {
      const details = document.createElement("details");
      details.open = depth < 2;
      const summary = document.createElement("summary");
      const isArray = Array.isArray(value);
      const entries = isArray ? value.map((item, index) => [index, item]) : Object.entries(value);
      summary.append(
        textSpan(isArray ? "[" : "{", "json-bracket"),
        document.createTextNode(` ${entries.length} ${entries.length === 1 ? "item" : "items"} `),
        textSpan(isArray ? "]" : "}", "json-bracket")
      );
      details.append(summary);

      entries.forEach(([key, child]) => {
        const row = document.createElement("div");
        row.className = "json-row";
        row.append(textSpan(`${isArray ? key : `"${key}"`}: `, "json-key"));
        row.append(buildJsonTree(child, depth + 1));
        details.append(row);
      });
      return details;
    }

    if (typeof value === "string") return textSpan(JSON.stringify(value), "json-string");
    if (typeof value === "number") return textSpan(String(value), "json-number");
    if (typeof value === "boolean") return textSpan(String(value), "json-boolean");
    return textSpan("null", "json-null");
  }

  function textSpan(text, className) {
    const span = document.createElement("span");
    span.className = className;
    span.textContent = text;
    return span;
  }

  function messageMatches(message, query) {
    return `${message.topic} ${message.payload} ${message.device}`.toLowerCase().includes(query);
  }

  function activeFilter() {
    return Boolean(normalizedGlobalQuery());
  }

  function normalizedGlobalQuery() {
    return elements.globalSearch.value.trim().toLowerCase();
  }

  function scheduleAggregateRender() {
    if (aggregateRenderTimer) return;
    aggregateRenderTimer = window.setTimeout(() => {
      aggregateRenderTimer = null;
      renderTopics();
      renderDevices();
    }, 150);
  }

  function scheduleFilteredRender() {
    window.clearTimeout(filteredRenderTimer);
    filteredRenderTimer = window.setTimeout(renderFeed, 80);
  }

  function updateBadges() {
    elements.feedCountBadge.textContent = formatNumber(state.messages.length);
    elements.topicCountBadge.textContent = formatNumber(state.topics.size);
    elements.deviceCountBadge.textContent = formatNumber(state.devices.size);
    elements.systemCountBadge.textContent = formatNumber(state.system.size);
  }

  function refreshDeviceOnlineStatus() {
    const now = Date.now();
    let changed = false;
    state.devices.forEach((device) => {
      const online =
        now - new Date(device.last_seen).getTime() <= deviceOnlineMilliseconds;
      if (device.online !== online) {
        device.online = online;
        changed = true;
      }
    });
    if (changed) renderDevices();
  }

  function oneLine(value) {
    return String(value ?? "").replace(/\r?\n/g, " ↵ ");
  }

  function formatNumber(value) {
    return new Intl.NumberFormat().format(value);
  }

  function formatTimestamp(value) {
    if (!value) return "—";
    return new Date(value).toLocaleString([], {
      hour12: false,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      fractionalSecondDigits: 3,
    });
  }

  function formatTime(date, includeSeconds = false) {
    return date.toLocaleTimeString([], {
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: includeSeconds ? "2-digit" : undefined,
    });
  }

  function formatRelativeTime(value) {
    const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
    if (seconds < 5) return "just now";
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
  }

  function formatDuration(totalSeconds) {
    const seconds = Math.max(0, Number(totalSeconds) || 0);
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const remainder = Math.floor(seconds % 60);
    if (days) return `${days}d ${hours}h`;
    if (hours) return `${hours}h ${minutes}m`;
    if (minutes) return `${minutes}m ${remainder}s`;
    return `${remainder}s`;
  }

  function formatBytes(bytes) {
    const value = Number(bytes) || 0;
    if (value < 1024) return `${value} B`;
    if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KB`;
    return `${(value / 1024 ** 2).toFixed(1)} MB`;
  }

  document.querySelectorAll(".view-tab").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".view-tab").forEach((tab) => tab.classList.remove("active"));
      document.querySelectorAll(".dashboard-panel").forEach((panel) => panel.classList.remove("active"));
      button.classList.add("active");
      document.getElementById(button.dataset.panel).classList.add("active");
    });
  });

  elements.globalSearch.addEventListener("input", renderAll);
  elements.topicSearch.addEventListener("input", renderTopics);

  elements.feedBody.addEventListener("click", (event) => {
    const button = event.target.closest("[data-topic]");
    if (button) openTopicDrawer(button.dataset.topic);
  });

  elements.topicBody.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-topic]");
    if (row) openTopicDrawer(row.dataset.topic);
  });

  elements.topicBody.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const row = event.target.closest("tr[data-topic]");
    if (row) openTopicDrawer(row.dataset.topic);
  });

  elements.pauseButton.addEventListener("click", () => {
    state.paused = !state.paused;
    elements.streamState.classList.toggle("is-paused", state.paused);
    if (state.paused) {
      elements.pauseButton.textContent = "Resume";
      return;
    }

    elements.pauseButton.textContent = "Pause";
    if (state.pendingWhilePaused.length) {
      const pending = state.pendingWhilePaused.splice(0);
      applyMessages(pending);
    }
  });

  elements.drawerClose.addEventListener("click", closeTopicDrawer);
  elements.drawerBackdrop.addEventListener("click", closeTopicDrawer);

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && elements.topicDrawer.classList.contains("is-open")) {
      closeTopicDrawer();
      return;
    }
    if (
      event.key === "/"
      && document.activeElement !== elements.globalSearch
      && !["INPUT", "TEXTAREA"].includes(document.activeElement.tagName)
    ) {
      event.preventDefault();
      elements.globalSearch.focus();
    }
  });
})();
