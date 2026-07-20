(() => {
  "use strict";

  const elements = {
    form: document.getElementById("aiDashboardForm"),
    nameInput: document.getElementById("aiDashboardName"),
    promptInput: document.getElementById("aiDashboardPrompt"),
    error: document.getElementById("aiDashboardError"),
    list: document.getElementById("aiDashboardList"),
    empty: document.getElementById("aiDashboardEmpty"),
    countBadge: document.getElementById("aiDashboardCountBadge"),
  };

  if (!elements.form) return;

  const views = new Map();

  function showError(message) {
    elements.error.textContent = message;
    elements.error.hidden = !message;
  }

  async function loadList() {
    const response = await fetch("/api/ai-dashboards");
    const dashboards = await response.json();
    elements.countBadge.textContent = String(dashboards.length);
    elements.empty.hidden = dashboards.length > 0;

    elements.list.replaceChildren();
    dashboards.forEach((dashboard) => elements.list.append(createCard(dashboard)));
  }

  function createCard(dashboard) {
    const card = document.createElement("article");
    card.className = "ai-dashboard-card";
    card.dataset.id = String(dashboard.id);

    const header = document.createElement("div");
    header.className = "ai-dashboard-card-header";
    const title = document.createElement("h3");
    title.textContent = dashboard.name;
    const deleteButton = document.createElement("button");
    deleteButton.type = "button";
    deleteButton.className = "ai-dashboard-delete";
    deleteButton.textContent = "Delete";
    deleteButton.addEventListener("click", () => deleteDashboard(dashboard.id, card));
    header.append(title, deleteButton);

    const prompt = document.createElement("p");
    prompt.className = "ai-dashboard-prompt";
    prompt.textContent = dashboard.prompt;

    const chart = document.createElement("div");
    chart.className = "ai-dashboard-chart";
    chart.id = `aiChart-${dashboard.id}`;

    card.append(header, prompt, chart);
    renderChart(dashboard.id, chart);
    return card;
  }

  async function renderChart(dashboardId, container) {
    const response = await fetch(`/api/ai-dashboards/${dashboardId}`);
    if (!response.ok) return;
    const dashboard = await response.json();

    const result = await window.vegaEmbed(container, dashboard.spec, { actions: false });
    result.view.data("table", dashboard.initial_data || []);
    await result.view.runAsync();
    views.set(dashboardId, result.view);
  }

  function applyAiMetrics(rows) {
    const byDashboard = new Map();
    rows.forEach((row) => {
      if (!byDashboard.has(row.dashboard_id)) byDashboard.set(row.dashboard_id, []);
      byDashboard.get(row.dashboard_id).push(row);
    });

    byDashboard.forEach((dashboardRows, dashboardId) => {
      const view = views.get(dashboardId);
      if (!view) return;

      const changeset = window.vega.changeset();
      dashboardRows.forEach((row) => {
        changeset.remove(
          (datum) => datum.metric === row.metric && datum.group === row.group
        );
        changeset.insert([row]);
      });
      view.change("table", changeset).run();
    });
  }

  async function deleteDashboard(dashboardId, card) {
    await fetch(`/api/ai-dashboards/${dashboardId}`, { method: "DELETE" });
    views.delete(dashboardId);
    card.remove();
    loadList();
  }

  elements.form.addEventListener("submit", async (event) => {
    event.preventDefault();
    showError("");

    const name = elements.nameInput.value.trim();
    const prompt = elements.promptInput.value.trim();
    if (!name || !prompt) return;

    const response = await fetch("/api/ai-dashboards", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, prompt }),
    });

    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      showError(body.error || `Request failed (${response.status})`);
      return;
    }

    elements.form.reset();
    loadList();
  });

  if (window.dashboardSocket) {
    window.dashboardSocket.on("ai_metrics", applyAiMetrics);
  }

  loadList();
})();
