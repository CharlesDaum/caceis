async function loadData() {
  const response = await fetch("./data/prototype_data.json");
  return response.json();
}

const state = {
  data: null,
  selectedEmployeeId: null,
  page: 1,
  pageSize: 25,
  absenceThresholdDays: 30,
};

function optionMarkup(values, includeAll = true) {
  const items = includeAll ? ["All", ...values] : values;
  return items.map((value) => `<option value="${value}">${value}</option>`).join("");
}

function pillClass(band) {
  return band === "High" ? "pill high" : band === "Medium" ? "pill medium" : "pill";
}

function humanizeFeatureName(name) {
  const directMap = {
    COUNTRY: "Country",
    CONTRACT: "Contract type",
    DEGREE: "Degree level",
    REASON: "Entry reason",
    Review_Completed_Flag: "Review completed flag",
    Training_Sessions_Count: "Training sessions count",
    Completed_Training_Sessions: "Completed training sessions",
    Absence_Count: "Absence count",
    Tenure_Years: "Tenure years",
  };
  if (directMap[name]) return directMap[name];
  return name.replaceAll("_", " ");
}

function getFilterValues() {
  return {
    country: document.querySelector("#country-filter").value,
    segment: document.querySelector("#segment-filter").value,
    risk: document.querySelector("#risk-filter").value,
    review: document.querySelector("#review-filter").value,
    search: document.querySelector("#search-filter").value.trim().toLowerCase(),
    sort: document.querySelector("#sort-filter").value,
  };
}

function matchesFilters(profile, filters) {
  const matchesCountry = filters.country === "All" || profile.country === filters.country;
  const matchesSegment = filters.segment === "All" || profile.segment === filters.segment;
  const matchesRisk = filters.risk === "All" || profile.risk_band === filters.risk;
  const matchesReview = filters.review === "All"
    || (filters.review === "Completed" && profile.review_completed === "Yes")
    || (filters.review === "Missing" && profile.review_completed === "No");
  const searchableText = [
    profile.employee_id,
    profile.country,
    profile.segment,
    profile.entity,
    profile.action,
  ].join(" ").toLowerCase();
  const matchesSearch = !filters.search || searchableText.includes(filters.search);
  return matchesCountry && matchesSegment && matchesRisk && matchesReview && matchesSearch;
}

function sortProfiles(profiles, mode) {
  const sorted = [...profiles];
  if (mode === "uplift_desc") {
    sorted.sort((a, b) => b.uplift - a.uplift || b.absence_risk - a.absence_risk);
  } else if (mode === "country_asc") {
    sorted.sort((a, b) => a.country.localeCompare(b.country) || b.absence_risk - a.absence_risk);
  } else if (mode === "training_desc") {
    sorted.sort((a, b) => b.training_hours - a.training_hours || b.absence_risk - a.absence_risk);
  } else {
    sorted.sort((a, b) => b.absence_risk - a.absence_risk || b.uplift - a.uplift);
  }
  return sorted;
}

function renderMeta(meta) {
  document.querySelector("#meta-panel").innerHTML = `
    <p class="eyebrow">Prototype Scope</p>
    <p class="microcopy">Source dataset</p>
    <strong>${meta.source_dataset}</strong>
    <div class="statline"><span>Population</span><span>${meta.population.toLocaleString()}</span></div>
    <div class="statline"><span>Latest snapshot</span><span>${meta.last_period}</span></div>
    <div class="statline"><span>Usage</span><span>Prioritize, segment, intervene</span></div>
  `;
}

function renderMethodology() {
  document.querySelector("#absence-threshold").textContent = `${state.absenceThresholdDays} days`;
  document.querySelector("#absence-band-summary").textContent =
    "Risk bands are based on predicted probability: Low below 35%, Medium from 35% to 65%, and High above 65%.";
}

function renderStaticModels(models) {
  const entries = Object.values(models);
  document.querySelector("#model-grid").innerHTML = entries.map((model) => `
    <article class="model-card">
      <p class="eyebrow">${model.label}</p>
      <p class="microcopy">${model.objective}</p>
      <div class="metric-list">
        <div class="metric-row"><span>ROC AUC</span><strong>${model.metrics.roc_auc}</strong></div>
        <div class="metric-row"><span>Average precision</span><strong>${model.metrics.average_precision}</strong></div>
        <div class="metric-row"><span>F1 at 0.5</span><strong>${model.metrics.f1_at_0_5}</strong></div>
        <div class="metric-row"><span>Support</span><strong>${model.metrics.support.toLocaleString()}</strong></div>
      </div>
      <h3>Top Drivers</h3>
      ${model.top_drivers.map((driver) => `
        <div class="driver-row">
          <span class="driver-label">${humanizeFeatureName(driver.feature)}</span>
          <div class="bar"><span style="width:${Math.min(driver.weight * 12, 100)}%"></span></div>
          <strong class="driver-value">${driver.weight}</strong>
        </div>
      `).join("")}
    </article>
  `).join("");
}

function renderDistributionFromProfiles(profiles) {
  const counts = { High: 0, Medium: 0, Low: 0 };
  profiles.forEach((profile) => {
    counts[profile.risk_band] = (counts[profile.risk_band] || 0) + 1;
  });
  const total = profiles.length || 1;
  const distribution = ["High", "Medium", "Low"].map((band) => ({
    band,
    share: Math.round(((counts[band] || 0) / total) * 1000) / 10,
  }));
  document.querySelector("#risk-distribution").innerHTML = distribution.map((item) => `
    <div class="distribution-row">
      <span>${item.band}</span>
      <div class="bar"><span style="width:${item.share}%"></span></div>
      <strong>${item.share}%</strong>
    </div>
  `).join("");
}

function renderFairness(containerId, rows) {
  if (!rows.length) {
    document.querySelector(containerId).innerHTML = `<div class="empty-state">No population in the current scope.</div>`;
    return;
  }
  document.querySelector(containerId).innerHTML = rows.map((row) => `
    <div class="fairness-row">
      <span>${row.group} <span class="microcopy">(${row.employees})</span></span>
      <span>${row.avg_score}% score</span>
    </div>
  `).join("");
}

function renderKpis(profiles, queue) {
  const total = profiles.length;
  const highRisk = profiles.filter((profile) => profile.risk_band === "High").length;
  const completedReview = profiles.filter((profile) => profile.review_completed === "Yes").length;
  const noTraining = profiles.filter((profile) => profile.training_hours <= 0).length;
  const avgUplift = total ? profiles.reduce((sum, profile) => sum + profile.uplift, 0) / total : 0;
  const topCountries = new Set(profiles.map((profile) => profile.country)).size;
  const kpis = [
    { label: "Employees in scope", value: total.toLocaleString(), detail: "Profiles matching the current filter state" },
    { label: "High absence alerts", value: highRisk.toLocaleString(), detail: "Employees currently in the high-risk band" },
    { label: "Review coverage", value: `${total ? ((completedReview / total) * 100).toFixed(1) : "0.0"}%`, detail: "Completed review rate inside the active scope" },
    { label: "Avg modeled upside", value: `${avgUplift.toFixed(1)} pts`, detail: "Average estimated improvement from the recommended plan" },
    { label: "Priority cases", value: queue.length.toLocaleString(), detail: `${topCountries} countries represented in the current selection` },
    { label: "No-training exposure", value: `${total ? ((noTraining / total) * 100).toFixed(1) : "0.0"}%`, detail: "Employees with zero recorded training hours" },
  ];
  document.querySelector("#kpi-grid").innerHTML = kpis.map((kpi) => `
    <article class="kpi-card">
      <p class="eyebrow">${kpi.label}</p>
      <strong class="kpi-value">${kpi.value}</strong>
      <p class="metric-detail">${kpi.detail}</p>
    </article>
  `).join("");
}

function renderSegments(profiles) {
  const grouped = new Map();
  profiles.forEach((profile) => {
    const entry = grouped.get(profile.segment) || {
      segment: profile.segment,
      employees: 0,
      absence: 0,
      training: 0,
      uplift: 0,
    };
    entry.employees += 1;
    entry.absence += profile.absence_risk;
    entry.training += profile.training_hours;
    entry.uplift += profile.uplift;
    grouped.set(profile.segment, entry);
  });
  const cards = Array.from(grouped.values()).sort((a, b) => b.employees - a.employees);
  const container = document.querySelector("#segment-grid");
  if (!cards.length) {
    container.innerHTML = `<div class="empty-state">No segment remains after the current filters.</div>`;
    return;
  }
  container.innerHTML = cards.map((segment) => `
    <article class="segment-card">
      <p class="eyebrow">${segment.segment}</p>
      <p class="segment-description">Live roll-up for the filtered population, useful to compare where intervention load is concentrating.</p>
      <div class="statline"><span>Employees</span><strong>${segment.employees.toLocaleString()}</strong></div>
      <div class="statline"><span>Avg absence risk</span><strong>${(segment.absence / segment.employees).toFixed(1)}%</strong></div>
      <div class="statline"><span>Avg training hours</span><strong>${(segment.training / segment.employees).toFixed(1)} hrs</strong></div>
      <div class="statline"><span>Avg upside</span><strong>${(segment.uplift / segment.employees).toFixed(1)} pts</strong></div>
    </article>
  `).join("");
}

function renderCountries(profiles) {
  const grouped = new Map();
  profiles.forEach((profile) => {
    const entry = grouped.get(profile.country) || {
      country: profile.country,
      employees: 0,
      risk: 0,
      uplift: 0,
      reviews: 0,
    };
    entry.employees += 1;
    entry.risk += profile.absence_risk;
    entry.uplift += profile.uplift;
    entry.reviews += profile.review_completed === "Yes" ? 1 : 0;
    grouped.set(profile.country, entry);
  });
  const cards = Array.from(grouped.values()).sort((a, b) => b.employees - a.employees).slice(0, 8);
  const container = document.querySelector("#country-grid");
  if (!cards.length) {
    container.innerHTML = `<div class="empty-state">No country summary is available for this filter combination.</div>`;
    return;
  }
  container.innerHTML = cards.map((country) => `
    <article class="country-card">
      <p class="eyebrow">${country.country}</p>
      <div class="statline"><span>Headcount</span><strong>${country.employees.toLocaleString()}</strong></div>
      <div class="statline"><span>Avg absence risk</span><strong>${(country.risk / country.employees).toFixed(1)}%</strong></div>
      <div class="statline"><span>Modeled upside</span><strong>${(country.uplift / country.employees).toFixed(1)} pts</strong></div>
      <div class="statline"><span>Review completion</span><strong>${((country.reviews / country.employees) * 100).toFixed(1)}%</strong></div>
    </article>
  `).join("");
}

function summarizeFairness(profiles, key) {
  const grouped = new Map();
  profiles.forEach((profile) => {
    const group = profile[key] || "Unknown";
    const entry = grouped.get(group) || { group, employees: 0, score: 0 };
    entry.employees += 1;
    entry.score += profile.absence_risk;
    grouped.set(group, entry);
  });
  return Array.from(grouped.values())
    .sort((a, b) => b.employees - a.employees)
    .slice(0, 8)
    .map((entry) => ({
      group: entry.group,
      employees: entry.employees,
      avg_score: (entry.score / entry.employees).toFixed(1),
    }));
}

function renderScope(filters, profiles, queue) {
  const active = [];
  if (filters.country !== "All") active.push(filters.country);
  if (filters.segment !== "All") active.push(filters.segment);
  if (filters.risk !== "All") active.push(`${filters.risk} risk`);
  if (filters.review !== "All") active.push(`${filters.review} reviews`);
  if (filters.search) active.push(`search: ${filters.search}`);
  document.querySelector("#scope-title").textContent = active.length ? active.join(" · ") : "All employees in the prototype";
  document.querySelector("#scope-detail").textContent = `${profiles.length.toLocaleString()} profiles matched, ${queue.length.toLocaleString()} priority cases visible in the queue.`;
}

function renderActiveFilterChips(filters) {
  const chips = [];
  if (filters.country !== "All") chips.push(filters.country);
  if (filters.segment !== "All") chips.push(filters.segment);
  if (filters.risk !== "All") chips.push(`${filters.risk} risk`);
  if (filters.review !== "All") chips.push(`${filters.review} reviews`);
  if (filters.search) chips.push(`Search: ${filters.search}`);
  document.querySelector("#active-filter-chips").innerHTML = chips.length
    ? chips.map((chip) => `<span class="filter-chip">${chip}</span>`).join("")
    : `<span class="filter-chip">No active filters</span>`;
}

function renderQueue(records) {
  document.querySelector("#queue-count").textContent = `${records.length} employees match current filters`;
  const body = document.querySelector("#queue-body");
  if (!records.length) {
    body.innerHTML = `<tr><td colspan="7"><div class="empty-state">No priority case matches the current filters.</div></td></tr>`;
    return;
  }
  body.innerHTML = records.slice(0, 25).map((record) => `
    <tr class="is-clickable ${state.selectedEmployeeId === record.employee_id ? "is-active" : ""}" data-employee-id="${record.employee_id}">
      <td>${record.employee_id}</td>
      <td>${record.country}</td>
      <td>${record.segment}</td>
      <td>${record.absence_days}</td>
      <td><span class="${pillClass(record.risk_band)}">${record.absence_risk}%</span></td>
      <td>${record.uplift}%</td>
      <td>
        <strong>${record.action}</strong>
        <div class="microcopy">${record.rationale}</div>
      </td>
    </tr>
  `).join("");
}

function renderEmployeeSelect(records) {
  const select = document.querySelector("#employee-select");
  if (!records.length) {
    select.innerHTML = `<option value="">No matching employee</option>`;
    select.disabled = true;
    return;
  }
  select.disabled = false;
  select.innerHTML = records.slice(0, 200).map((record) => `
    <option value="${record.employee_id}">${record.employee_id} · ${record.country} · ${record.segment}</option>
  `).join("");
}

function renderEmployeeCard(profile) {
  if (!profile) {
    document.querySelector("#employee-card").innerHTML = `<p class="microcopy">No employee matches the current filters.</p>`;
    return;
  }
  document.querySelector("#employee-card").innerHTML = `
    <div class="employee-head">
      <div>
        <p class="eyebrow">${profile.segment}</p>
        <h3>${profile.employee_id}</h3>
        <p class="microcopy">${profile.country} · ${profile.contract} · ${profile.age_range}</p>
      </div>
      <div><span class="${pillClass(profile.risk_band)}">${profile.risk_band} risk</span></div>
    </div>
    <div class="score-grid">
      <div class="score-card">
        <span class="microcopy">Absence risk</span>
        <strong>${profile.absence_risk}%</strong>
      </div>
      <div class="score-card">
        <span class="microcopy">Current performance potential</span>
        <strong>${profile.current_performance}%</strong>
      </div>
      <div class="score-card">
        <span class="microcopy">Simulated performance potential</span>
        <strong>${profile.simulated_performance}%</strong>
      </div>
    </div>
    <div class="statline"><span>Entity</span><strong>${profile.entity}</strong></div>
    <div class="statline"><span>Current training hours</span><strong>${profile.training_hours}</strong></div>
    <div class="statline"><span>Peer benchmark hours</span><strong>${profile.peer_training_hours}</strong></div>
    <div class="statline"><span>Training gap</span><strong>${profile.training_gap}</strong></div>
    <div class="statline"><span>Tenure</span><strong>${profile.tenure_years} years</strong></div>
    <div class="statline"><span>Recorded absence days</span><strong>${profile.absence_days}</strong></div>
    <div class="statline"><span>Review completed</span><strong>${profile.review_completed}</strong></div>
    <div class="score-card">
      <span class="microcopy">Recommended action</span>
      <strong>${profile.action}</strong>
      <p class="employee-rationale">${profile.rationale}</p>
      <p class="microcopy">Estimated upside: +${profile.uplift} percentage points in high-performer probability.</p>
    </div>
  `;
}

function renderExplorer(profiles, sortMode) {
  const sorted = sortProfiles(profiles, sortMode);
  const pageCount = Math.max(1, Math.ceil(sorted.length / state.pageSize));
  state.page = Math.min(state.page, pageCount);
  state.page = Math.max(1, state.page);
  const start = (state.page - 1) * state.pageSize;
  const pageRows = sorted.slice(start, start + state.pageSize);
  const body = document.querySelector("#explorer-body");

  if (!pageRows.length) {
    body.innerHTML = `<tr><td colspan="8"><div class="empty-state">No employee matches the current filters.</div></td></tr>`;
  } else {
    body.innerHTML = pageRows.map((profile) => `
      <tr class="is-clickable ${state.selectedEmployeeId === profile.employee_id ? "is-active" : ""}" data-employee-id="${profile.employee_id}">
        <td>${profile.employee_id}</td>
        <td>${profile.country}</td>
        <td>${profile.segment}</td>
        <td>${profile.absence_days}</td>
        <td><span class="${pillClass(profile.risk_band)}">${profile.absence_risk}%</span></td>
        <td>${profile.uplift}%</td>
        <td>${profile.training_hours} hrs</td>
        <td>${profile.review_completed}</td>
      </tr>
    `).join("");
  }

  document.querySelector("#page-status").textContent = sorted.length
    ? `Page ${state.page} / ${pageCount} · ${sorted.length.toLocaleString()} employees`
    : "0 employees";
  document.querySelector("#page-prev").disabled = state.page <= 1;
  document.querySelector("#page-next").disabled = state.page >= pageCount;
}

function refreshView() {
  const filters = getFilterValues();
  const filteredProfiles = state.data.employee_profiles.filter((profile) => matchesFilters(profile, filters));
  const allowedIds = new Set(filteredProfiles.map((profile) => profile.employee_id));
  const filteredQueue = state.data.priority_queue.filter((record) => allowedIds.has(record.employee_id));

  renderScope(filters, filteredProfiles, filteredQueue);
  renderActiveFilterChips(filters);
  renderKpis(filteredProfiles, filteredQueue);
  renderDistributionFromProfiles(filteredProfiles);
  renderFairness("#gender-fairness", summarizeFairness(filteredProfiles, "gender"));
  renderFairness("#country-fairness", summarizeFairness(filteredProfiles, "country"));
  renderSegments(filteredProfiles);
  renderCountries(filteredProfiles);
  renderQueue(filteredQueue);
  renderExplorer(filteredProfiles, filters.sort);
  renderEmployeeSelect(filteredProfiles);

  const stillVisible = filteredProfiles.find((profile) => profile.employee_id === state.selectedEmployeeId);
  const active = stillVisible || filteredProfiles[0] || null;
  state.selectedEmployeeId = active ? active.employee_id : null;
  renderEmployeeCard(active);
  if (active) {
    document.querySelector("#employee-select").value = active.employee_id;
  }
}

function applyQuickFilter(mode) {
  if (mode === "high-risk") {
    document.querySelector("#risk-filter").value = "High";
    document.querySelector("#sort-filter").value = "risk_desc";
  } else if (mode === "missing-review") {
    document.querySelector("#review-filter").value = "Missing";
    document.querySelector("#sort-filter").value = "risk_desc";
  } else if (mode === "high-upside") {
    document.querySelector("#sort-filter").value = "uplift_desc";
    document.querySelector("#risk-filter").value = "All";
    document.querySelector("#review-filter").value = "All";
  } else {
    document.querySelector("#country-filter").value = "All";
    document.querySelector("#segment-filter").value = "All";
    document.querySelector("#risk-filter").value = "All";
    document.querySelector("#review-filter").value = "All";
    document.querySelector("#search-filter").value = "";
    document.querySelector("#sort-filter").value = "risk_desc";
  }
  state.page = 1;
  refreshView();
}

function bindInteractions() {
  ["#country-filter", "#segment-filter", "#risk-filter", "#review-filter", "#search-filter"].forEach((selector) => {
    const element = document.querySelector(selector);
    element.addEventListener("input", () => {
      state.page = 1;
      refreshView();
    });
    element.addEventListener("change", () => {
      state.page = 1;
      refreshView();
    });
  });

  document.querySelector("#sort-filter").addEventListener("change", () => {
    state.page = 1;
    refreshView();
  });

  document.querySelector("#employee-select").addEventListener("change", (event) => {
    state.selectedEmployeeId = event.target.value;
    refreshView();
  });

  document.querySelector("#page-prev").addEventListener("click", () => {
    state.page -= 1;
    refreshView();
  });

  document.querySelector("#page-next").addEventListener("click", () => {
    state.page += 1;
    refreshView();
  });

  document.querySelector("#reset-filters").addEventListener("click", () => {
    document.querySelector("#country-filter").value = "All";
    document.querySelector("#segment-filter").value = "All";
    document.querySelector("#risk-filter").value = "All";
    document.querySelector("#review-filter").value = "All";
    document.querySelector("#search-filter").value = "";
    document.querySelector("#sort-filter").value = "risk_desc";
    state.selectedEmployeeId = null;
    state.page = 1;
    refreshView();
  });

  document.querySelectorAll("[data-quick-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      applyQuickFilter(button.dataset.quickFilter);
    });
  });

  document.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-employee-id]");
    if (!row) return;
    state.selectedEmployeeId = row.dataset.employeeId;
    refreshView();
  });
}

loadData().then((data) => {
  state.data = data;
  renderMeta(data.meta);
  renderMethodology();
  renderStaticModels(data.models);

  document.querySelector("#country-filter").innerHTML = optionMarkup(data.filters.countries);
  document.querySelector("#segment-filter").innerHTML = optionMarkup(data.filters.segments);
  document.querySelector("#risk-filter").innerHTML = optionMarkup(data.filters.risk_bands);
  document.querySelector("#review-filter").innerHTML = optionMarkup(data.filters.review_status, false);

  bindInteractions();
  refreshView();
});
