/**
 * Git Pattern Analysis — Interactive Dashboard Logic
 * Zero-dependency pure JavaScript client for artifact exploration.
 */

document.addEventListener("DOMContentLoaded", () => {
  let dashboardData = null;
  let activeClusterFilter = "all";
  let activeLanguageFilter = "all";
  let activeSearchQuery = "";
  let sortColumn = "repo_name";
  let sortDirection = "asc";

  const CLUSTER_COLORS = {
    0: "#06b6d4", // Cyan
    1: "#10b981", // Emerald
    2: "#f59e0b", // Amber
    3: "#8b5cf6", // Violet
  };

  // DOM Elements
  const elValRepos = document.getElementById("val-repos");
  const elValCommits = document.getElementById("val-commits");
  const elValEnriched = document.getElementById("val-enriched");
  const elValClusters = document.getElementById("val-clusters");
  const elClusterCards = document.getElementById("cluster-cards-container");
  const elSvgChart = document.getElementById("cluster-svg-chart");
  const elChartLegend = document.getElementById("cluster-chart-legend");
  const elTableBody = document.getElementById("table-body");
  const elInputSearch = document.getElementById("input-search");
  const elSelectCluster = document.getElementById("select-cluster-filter");
  const elSelectLang = document.getElementById("select-lang-filter");
  const elBtnReset = document.getElementById("btn-reset-filters");
  const elBtnRefresh = document.getElementById("btn-refresh");
  const elFlowContainer = document.getElementById("pipeline-flow-container");

  // Fetch live dashboard data from local API
  async function loadData() {
    try {
      elTableBody.innerHTML = `<tr><td colspan="11" class="empty-state">Loading data from pipeline artifacts…</td></tr>`;
      const response = await fetch("/api/data");
      if (!response.ok) {
        throw new Error(`HTTP error ${response.status}: ${response.statusText}`);
      }
      dashboardData = await response.json();
      renderAll();
    } catch (err) {
      console.error("Failed to load dashboard data:", err);
      elTableBody.innerHTML = `
        <tr>
          <td colspan="11" class="empty-state" style="color: var(--accent-rose);">
            <div class="empty-state-icon">⚠️</div>
            <strong>Failed to load pipeline data:</strong> ${escapeHtml(err.message)}<br>
            <small style="color: var(--text-muted); margin-top: 6px; display: inline-block;">
              Ensure <code>dashboard/server.py</code> is running and artifact files exist in <code>data/</code>.
            </small>
          </td>
        </tr>`;
    }
  }

  function renderAll() {
    if (!dashboardData) return;
    renderOverview();
    renderClusterCards();
    renderClusterChart();
    populateLanguageFilter();
    renderTable();
    renderPipelineFlow();
  }

  // Section A: Overview KPIs
  function renderOverview() {
    const ov = dashboardData.overview || {};
    elValRepos.textContent = ov.total_repositories != null ? ov.total_repositories : "--";
    elValCommits.textContent = ov.total_commits != null ? Number(ov.total_commits).toLocaleString() : "--";
    elValEnriched.textContent = ov.total_enriched != null ? Number(ov.total_enriched).toLocaleString() : "--";
    elValClusters.textContent = ov.repositories_clustered != null ? ov.repositories_clustered : "--";

    const subEnriched = document.getElementById("val-enriched-sub");
    if (subEnriched && ov.total_enriched) {
      subEnriched.innerHTML = `
        <span class="badge-authentic" title="Authentic GitHub API records">${ov.authentic_enriched} Real (${(ov.authentic_enriched / ov.total_enriched * 100).toFixed(1)}%)</span>
        <span class="badge-synthetic" title="Deterministic mock-enriched records">${ov.synthetic_enriched} Mock (${ov.synthetic_percentage}%)</span>
      `;
    }
  }

  // Section B: Cluster Exploration Cards
  function renderClusterCards() {
    const summary = dashboardData.cluster_summary || {};
    const profiles = summary.profiles || {};
    const clusterIds = summary.cluster_ids || [];

    if (!clusterIds.length) {
      elClusterCards.innerHTML = `<div class="empty-state">No cluster assignments available. Run <code>python3 spark/cluster_repos.py</code>.</div>`;
      return;
    }

    elClusterCards.innerHTML = clusterIds.map(cid => {
      const p = profiles[cid] || {};
      const color = CLUSTER_COLORS[cid] || "#94a3b8";
      const isActive = String(activeClusterFilter) === String(cid);

      return `
        <div class="cluster-card ${isActive ? 'active' : ''}" data-cluster="${cid}" style="--cluster-color: ${color};" role="button" tabindex="0" title="Click to filter by Cluster ${cid}">
          <div class="cluster-card-head">
            <span class="cluster-pill">Cluster ${cid}</span>
            <span class="cluster-count">${p.count || 0} repositories</span>
          </div>
          <div class="cluster-stats">
            <div class="cluster-stat-item">
              <span class="stat-label">Avg Additions</span>
              <span class="stat-val">+${p.avg_additions != null ? p.avg_additions : 'N/A'}</span>
            </div>
            <div class="cluster-stat-item">
              <span class="stat-label">Avg Deletions</span>
              <span class="stat-val">-${p.avg_deletions != null ? p.avg_deletions : 'N/A'}</span>
            </div>
            <div class="cluster-stat-item">
              <span class="stat-label">Avg Lines</span>
              <span class="stat-val">${p.avg_lines_changed != null ? p.avg_lines_changed : 'N/A'}</span>
            </div>
            <div class="cluster-stat-item">
              <span class="stat-label">Merge Ratio</span>
              <span class="stat-val">${p.avg_merge_ratio != null ? (p.avg_merge_ratio * 100).toFixed(1) + '%' : 'N/A'}</span>
            </div>
          </div>
        </div>
      `;
    }).join("");

    // Add click listeners to cards
    elClusterCards.querySelectorAll(".cluster-card").forEach(card => {
      card.addEventListener("click", () => {
        const cid = card.getAttribute("data-cluster");
        if (activeClusterFilter === cid) {
          activeClusterFilter = "all";
          elSelectCluster.value = "all";
        } else {
          activeClusterFilter = cid;
          elSelectCluster.value = cid;
        }
        renderClusterCards();
        renderTable();
      });
    });
  }

  // Section B: Responsive SVG Comparison Chart
  function renderClusterChart() {
    const summary = dashboardData.cluster_summary || {};
    const profiles = summary.profiles || {};
    const clusterIds = summary.cluster_ids || [];

    if (!clusterIds.length) {
      elSvgChart.innerHTML = "";
      return;
    }

    const metrics = [
      { key: "avg_additions", label: "Additions" },
      { key: "avg_deletions", label: "Deletions" },
      { key: "avg_lines_changed", label: "Lines" },
      { key: "avg_files_changed", label: "Files" },
      { key: "avg_merge_ratio", label: "Merge Ratio", isRatio: true },
    ];

    // Compute max for each metric to normalize bars
    const maxVals = {};
    metrics.forEach(m => {
      maxVals[m.key] = Math.max(...clusterIds.map(cid => (profiles[cid] ? profiles[cid][m.key] || 0 : 0)), 0.001);
    });

    const width = 800;
    const height = 200;
    const padding = { top: 20, right: 30, bottom: 40, left: 100 };
    const chartW = width - padding.left - padding.right;
    const chartH = height - padding.top - padding.bottom;

    const groupW = chartW / metrics.length;
    const barW = Math.max(6, (groupW * 0.7) / clusterIds.length);

    let barsSvg = "";
    let labelsSvg = "";

    metrics.forEach((m, mIdx) => {
      const groupX = padding.left + mIdx * groupW + groupW * 0.15;
      labelsSvg += `
        <text x="${padding.left + (mIdx + 0.5) * groupW}" y="${height - 12}"
              text-anchor="middle" fill="#94a3b8" font-size="11" font-family="sans-serif">
          ${m.label}
        </text>`;

      clusterIds.forEach((cid, cIdx) => {
        const val = profiles[cid] ? profiles[cid][m.key] || 0 : 0;
        const normH = Math.min((val / maxVals[m.key]) * (chartH - 20), chartH - 20);
        const barX = groupX + cIdx * barW;
        const barY = padding.top + (chartH - normH);
        const color = CLUSTER_COLORS[cid] || "#94a3b8";
        const formatted = m.isRatio ? (val * 100).toFixed(1) + "%" : val.toFixed(1);

        barsSvg += `
          <rect x="${barX}" y="${barY}" width="${barW - 2}" height="${normH}"
                fill="${color}" rx="2" opacity="0.85">
            <title>Cluster ${cid} - ${m.label}: ${formatted}</title>
          </rect>
          <text x="${barX + (barW - 2) / 2}" y="${barY - 4}"
                text-anchor="middle" fill="#cbd5e1" font-size="9" font-family="monospace">
            ${formatted}
          </text>
        `;
      });
    });

    elSvgChart.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" style="width: 100%; height: auto; display: block;">
        <!-- Grid horizontal lines -->
        <line x1="${padding.left}" y1="${padding.top + chartH}" x2="${width - padding.right}" y2="${padding.top + chartH}" stroke="rgba(255,255,255,0.1)" />
        ${barsSvg}
        ${labelsSvg}
      </svg>
    `;

    // Render legend
    elChartLegend.innerHTML = clusterIds.map(cid => `
      <div class="legend-item">
        <span class="legend-box" style="background: ${CLUSTER_COLORS[cid]};"></span>
        <span>Cluster ${cid} (${profiles[cid]?.count || 0} repos)</span>
      </div>
    `).join("");
  }

  // Populate dynamic language filter options
  function populateLanguageFilter() {
    const repos = dashboardData.repositories || [];
    const langs = new Set();
    repos.forEach(r => {
      if (r.language) langs.add(r.language);
    });

    const sortedLangs = Array.from(langs).sort();
    const currentVal = elSelectLang.value;

    elSelectLang.innerHTML = `<option value="all">All Languages</option>` +
      sortedLangs.map(l => `<option value="${escapeHtml(l)}">${escapeHtml(l)}</option>`).join("");

    if (sortedLangs.includes(currentVal)) {
      elSelectLang.value = currentVal;
    }
  }

  // Section C: Repository Table Rendering with Sorting & Filtering
  function renderTable() {
    let list = (dashboardData.repositories || []).slice();

    // 1. Filter by cluster
    if (activeClusterFilter !== "all") {
      const cid = parseInt(activeClusterFilter, 10);
      list = list.filter(r => r.cluster === cid);
    }

    // 2. Filter by language
    if (activeLanguageFilter !== "all") {
      list = list.filter(r => (r.language || "").toLowerCase() === activeLanguageFilter.toLowerCase());
    }

    // 3. Search query
    if (activeSearchQuery) {
      const q = activeSearchQuery.toLowerCase();
      list = list.filter(r =>
        (r.repo_name || "").toLowerCase().includes(q) ||
        (r.language || "").toLowerCase().includes(q)
      );
    }

    // 4. Sort
    list.sort((a, b) => {
      let va = a[sortColumn];
      let vb = b[sortColumn];

      if (va == null) return 1;
      if (vb == null) return -1;

      if (typeof va === "string") {
        return sortDirection === "asc" ? va.localeCompare(vb) : vb.localeCompare(va);
      }
      return sortDirection === "asc" ? va - vb : vb - va;
    });

    // Update table header sort indicators
    document.querySelectorAll(".data-table th").forEach(th => {
      th.classList.remove("sorted-asc", "sorted-desc");
      if (th.getAttribute("data-col") === sortColumn) {
        th.classList.add(sortDirection === "asc" ? "sorted-asc" : "sorted-desc");
      }
    });

    if (list.length === 0) {
      elTableBody.innerHTML = `
        <tr>
          <td colspan="11" class="empty-state">
            <div class="empty-state-icon">🔍</div>
            No repositories matched the selected filters.
          </td>
        </tr>`;
      return;
    }

    elTableBody.innerHTML = list.map(r => {
      const clusterBadge = r.cluster != null
        ? `<span class="badge-cluster badge-cluster-${r.cluster}">Cluster ${r.cluster}</span>`
        : `<span class="stat-label">Unassigned</span>`;

      const langBadge = r.language
        ? `<span class="badge-lang">${escapeHtml(r.language)}</span>`
        : `<span class="stat-label">N/A</span>`;

      const mergePct = r.merge_ratio != null ? (r.merge_ratio * 100).toFixed(1) : "0.0";
      const starsFmt = r.stars != null ? Number(r.stars).toLocaleString() : "N/A";
      const commitsFmt = r.commit_count != null ? Number(r.commit_count) : "N/A";
      const authorsFmt = r.distinct_authors != null ? Number(r.distinct_authors) : "N/A";
      const avgAdd = r.avg_additions != null ? `+${r.avg_additions.toFixed(1)}` : "N/A";
      const avgDel = r.avg_deletions != null ? `-${r.avg_deletions.toFixed(1)}` : "N/A";
      const avgLines = r.avg_lines_changed != null ? r.avg_lines_changed.toFixed(1) : "N/A";
      const avgFiles = r.avg_files_changed != null ? r.avg_files_changed.toFixed(1) : "N/A";

      return `
        <tr>
          <td class="repo-cell" title="${escapeHtml(r.repo_name)}">${escapeHtml(r.repo_name)}</td>
          <td>${langBadge}</td>
          <td class="mono-cell">${starsFmt}</td>
          <td class="mono-cell">${commitsFmt}</td>
          <td class="mono-cell">${authorsFmt}</td>
          <td>
            <div class="merge-progress">
              <span class="mono-cell">${mergePct}%</span>
              <div class="progress-track">
                <div class="progress-fill" style="width: ${Math.min(parseFloat(mergePct), 100)}%;"></div>
              </div>
            </div>
          </td>
          <td class="mono-cell" style="color: #34d399;">${avgAdd}</td>
          <td class="mono-cell" style="color: #f87171;">${avgDel}</td>
          <td class="mono-cell">${avgLines}</td>
          <td class="mono-cell">${avgFiles}</td>
          <td>${clusterBadge}</td>
        </tr>
      `;
    }).join("");
  }

  // Section D: Pipeline Provenance Flow Diagram
  function renderPipelineFlow() {
    const status = dashboardData.artifacts_status || {};
    const stages = [
      {
        num: "Stage 1",
        title: "Repository Discovery",
        artifact: "data/sample/repositories.jsonl",
        ready: status.repositories_found,
        meta: "20 repos with stars > 1,000",
      },
      {
        num: "Stage 2",
        title: "Commit Collection",
        artifact: "data/sample/commits.jsonl",
        ready: status.commits_found,
        meta: "1,000 recent commit records",
      },
      {
        num: "Stage 3",
        title: "Commit Enrichment",
        artifact: "data/sample/enriched_commits.jsonl",
        ready: status.enriched_found,
        meta: "15 Real / 985 Mock (synthetic=True)",
      },
      {
        num: "Stage 4",
        title: "MapReduce Features",
        artifact: status.features_source === "distributed_mr" ? "repo_features (HDFS YARN)" : "data/output/repo_features_local.jsonl",
        ready: status.features_found,
        meta: "20 repo feature vectors",
      },
      {
        num: "Stage 5",
        title: "Spark MLlib Clustering",
        artifact: "data/output/repo_clusters.json",
        ready: status.clusters_found,
        meta: "K=4 clusters, seed=42",
      },
    ];

    elFlowContainer.innerHTML = stages.map(s => `
      <div class="stage-card">
        <div class="stage-num">${s.num}</div>
        <div class="stage-title">${s.title}</div>
        <div class="stage-artifact">${s.artifact}</div>
        <div class="stage-status" style="color: ${s.ready ? '#34d399' : '#f87171'};">
          <span>${s.ready ? '✓' : '✗'}</span>
          <span>${s.ready ? 'Verified & Loaded' : 'Missing'}</span>
        </div>
        <div style="font-size: 0.6875rem; color: var(--text-muted); margin-top: 4px;">${s.meta}</div>
      </div>
    `).join("");
  }

  // Event Listeners
  elInputSearch.addEventListener("input", (e) => {
    activeSearchQuery = e.target.value.trim();
    renderTable();
  });

  elSelectCluster.addEventListener("change", (e) => {
    activeClusterFilter = e.target.value;
    renderClusterCards();
    renderTable();
  });

  elSelectLang.addEventListener("change", (e) => {
    activeLanguageFilter = e.target.value;
    renderTable();
  });

  elBtnReset.addEventListener("click", () => {
    activeClusterFilter = "all";
    activeLanguageFilter = "all";
    activeSearchQuery = "";
    elInputSearch.value = "";
    elSelectCluster.value = "all";
    elSelectLang.value = "all";
    renderClusterCards();
    renderTable();
  });

  elBtnRefresh.addEventListener("click", () => {
    loadData();
  });

  // Table header sort handler
  document.querySelectorAll(".data-table th").forEach(th => {
    th.addEventListener("click", () => {
      const col = th.getAttribute("data-col");
      if (!col) return;
      if (sortColumn === col) {
        sortDirection = sortDirection === "asc" ? "desc" : "asc";
      } else {
        sortColumn = col;
        sortDirection = "asc";
      }
      renderTable();
    });
  });

  // Utility
  function escapeHtml(str) {
    if (str == null) return "";
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  // Initial fetch
  loadData();
});
