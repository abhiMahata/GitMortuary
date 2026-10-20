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

  let activeRunId = "sample";

  // DOM Elements
  const elSelectActiveRun = document.getElementById("select-active-run");
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

  // Load available runs from API
  async function loadRuns() {
    try {
      const response = await fetch("/api/runs");
      if (!response.ok) return;
      const json = await response.json();
      const runs = json.runs || [];
      if (!elSelectActiveRun) return;

      elSelectActiveRun.innerHTML = runs.map(r => {
        return `<option value="${escapeHtml(r.run_id)}">${escapeHtml(r.label)}</option>`;
      }).join("");

      elSelectActiveRun.value = activeRunId || "sample";
    } catch (err) {
      console.warn("Could not load runs list:", err);
    }
  }

  // Fetch live dashboard data from local API
  async function loadData(runId = null) {
    if (runId !== null) {
      activeRunId = runId;
    }
    const currentRun = activeRunId || "sample";

    try {
      elTableBody.innerHTML = `<tr><td colspan="11" class="empty-state">Loading data from pipeline artifacts…</td></tr>`;
      const url = currentRun === "sample" ? "/api/data" : `/api/data?run=${encodeURIComponent(currentRun)}`;
      const response = await fetch(url);
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
              Ensure <code>dashboard/server.py</code> is running and artifacts exist for run <code>${escapeHtml(currentRun)}</code>.
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

    const summary = dashboardData.cluster_summary || {};
    const subClusters = document.getElementById("val-clusters-sub");
    if (summary.stage_status === "skipped" || summary.skipped) {
      elValClusters.textContent = "Skipped";
      if (subClusters) subClusters.textContent = "Requires ≥ 2 repositories";
    } else {
      elValClusters.textContent = ov.repositories_clustered != null ? ov.repositories_clustered : "--";
      if (subClusters) subClusters.textContent = `Across ${ov.cluster_count || 0} K-Means clusters`;
    }

    const subEnriched = document.getElementById("val-enriched-sub");
    if (subEnriched && ov.total_enriched) {
      if (ov.synthetic_enriched === 0) {
        subEnriched.innerHTML = `
          <span class="badge-authentic" title="Authentic GitHub API records">100% Real Live Data (${ov.authentic_enriched})</span>
        `;
      } else {
        subEnriched.innerHTML = `
          <span class="badge-authentic" title="Authentic GitHub API records">${ov.authentic_enriched} Real (${(ov.authentic_enriched / ov.total_enriched * 100).toFixed(1)}%)</span>
          <span class="badge-synthetic" title="Deterministic mock-enriched records">${ov.synthetic_enriched} Mock (${ov.synthetic_percentage}%)</span>
        `;
      }
    }

    // Provenance Banner
    const prov = dashboardData.provenance_notice || {};
    const elBanner = document.getElementById("provenance-banner");
    const elBannerText = document.getElementById("provenance-notice-text");
    if (elBanner && elBannerText) {
      if (prov.is_authentic_live) {
        elBanner.style.background = "linear-gradient(90deg, rgba(16, 185, 129, 0.14) 0%, rgba(17, 24, 39, 0.75) 100%)";
        elBanner.style.borderColor = "rgba(16, 185, 129, 0.4)";
        const icon = elBanner.querySelector(".banner-icon");
        if (icon) icon.textContent = "🛡️";
        const h4 = elBanner.querySelector("h4");
        if (h4) {
          h4.textContent = `Authentic Live GitHub Run: ${dashboardData.run_info?.run_id || ""}`;
          h4.style.color = "#34d399";
        }
        elBannerText.innerHTML = `
          <strong>100% Authentic Live Data:</strong> All commit statistics and diff metrics were retrieved directly from the live GitHub REST API.
          <strong>Unsupervised Clustering Notice:</strong> ${escapeHtml(prov.unsupervised_notice || "")}
        `;
      } else {
        elBanner.style.background = "linear-gradient(90deg, rgba(245, 158, 11, 0.12) 0%, rgba(17, 24, 39, 0.7) 100%)";
        elBanner.style.borderColor = "rgba(245, 158, 11, 0.35)";
        const icon = elBanner.querySelector(".banner-icon");
        if (icon) icon.textContent = "⚠️";
        const h4 = elBanner.querySelector("h4");
        if (h4) {
          h4.textContent = "Dataset Provenance & Quality Notice";
          h4.style.color = "#fbbf24";
        }
        elBannerText.innerHTML = `
          <strong>98.5% Synthetic Enrichment:</strong> 985 of the 1,000 sampled commit records were generated via deterministic mock mode (<code>--mock</code>) using SHA-256 hash seeds to exercise the data engineering pipeline without requiring live GitHub API quotas. Only 15 records reflect live GitHub API diff data.
          <strong>Unsupervised Clustering Notice:</strong> Spark K-Means cluster assignments reflect geometric feature proximity across commit counts, additions, deletions, lines, files, and merge ratios. Clusters are not performance rankings or repository health scores.
        `;
      }
    }
  }

  // Section B: Cluster Exploration Cards
  function renderClusterCards() {
    const summary = dashboardData.cluster_summary || {};
    const profiles = summary.profiles || {};
    const clusterIds = summary.cluster_ids || [];

    if (summary.stage_status === "skipped" || summary.skipped) {
      elClusterCards.innerHTML = `
        <div class="empty-state" style="background: rgba(255, 255, 255, 0.03); border: 1px dashed var(--border-glass); padding: 22px; border-radius: var(--radius-md); text-align: left; width: 100%;">
          <div style="font-size: 0.9375rem; font-weight: 600; color: #38bdf8; margin-bottom: 6px;">ℹ Spark MLlib Clustering Skipped</div>
          <div style="color: var(--text-secondary); font-size: 0.8125rem; line-height: 1.5;">${escapeHtml(summary.stage_reason || "Clustering requires at least 2 repositories.")}</div>
          <div style="color: var(--text-muted); font-size: 0.75rem; margin-top: 8px;">Single-repository clustering is mathematically degenerate and not presented as meaningful analysis. Feature aggregation from Hadoop MapReduce is preserved in the table below.</div>
        </div>
      `;
      return;
    }

    if (!clusterIds.length) {
      elClusterCards.innerHTML = `<div class="empty-state">No cluster assignments available for this run.</div>`;
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

    if (!clusterIds.length || summary.stage_status === "skipped" || summary.skipped) {
      elSvgChart.innerHTML = '<div style="color: var(--text-muted); text-align: center; padding: 48px 0; font-size: 0.8125rem;">Feature comparison chart unavailable (clustering skipped or no clusters assigned).</div>';
      elChartLegend.innerHTML = "";
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
      const isSkipped = dashboardData.cluster_summary && (dashboardData.cluster_summary.skipped || dashboardData.cluster_summary.stage_status === "skipped");
      const clusterBadge = r.cluster != null
        ? `<span class="badge-cluster badge-cluster-${r.cluster}">Cluster ${r.cluster}</span>`
        : `<span class="stat-label" style="opacity: 0.7;">${isSkipped ? 'N/A (Skipped)' : 'Unassigned'}</span>`;

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
    const isSample = !dashboardData.run_info || dashboardData.run_info.is_sample;
    const runInfo = dashboardData.run_info || {};

    let stages;
    if (isSample) {
      stages = [
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
    } else {
      const runStages = runInfo.stages || {};
      const mrMode = runStages.mapreduce?.mode === "hadoop_streaming" ? "Hadoop Streaming" : "Local Streaming";
      const sparkSkipped = runStages.spark?.status === "skipped" || (dashboardData.cluster_summary && dashboardData.cluster_summary.skipped);
      const sparkReady = status.clusters_found && !sparkSkipped;

      stages = [
        {
          num: "Stage 1",
          title: "Dynamic Ingestion",
          artifact: `data/runs/${runInfo.run_id}/`,
          ready: status.repositories_found && status.commits_found,
          meta: `${dashboardData.overview.total_repositories || 1} repo(s), ${dashboardData.overview.total_commits || 0} commits`,
        },
        {
          num: "Stage 2",
          title: "Enriched Commits",
          artifact: "enriched_commits.jsonl",
          ready: status.enriched_found,
          meta: `${dashboardData.overview.total_enriched || 0} authentic diff records (100% real)`,
        },
        {
          num: "Stage 3",
          title: "MapReduce Features",
          artifact: "repo_features.jsonl",
          ready: status.features_found,
          meta: `${mrMode} (${dashboardData.overview.total_repositories || 1} vectors)`,
        },
        {
          num: "Stage 4",
          title: "Spark MLlib Clustering",
          artifact: "repo_clusters.json",
          ready: sparkReady,
          meta: sparkSkipped ? "Skipped (<2 repos)" : `${dashboardData.overview.cluster_count || 0} clusters`,
        },
      ];
    }

    elFlowContainer.innerHTML = stages.map(s => `
      <div class="stage-card">
        <div class="stage-num">${s.num}</div>
        <div class="stage-title">${s.title}</div>
        <div class="stage-artifact">${s.artifact}</div>
        <div class="stage-status" style="color: ${s.ready ? '#34d399' : '#f87171'};">
          <span>${s.ready ? '✓' : (s.meta.includes('Skipped') ? 'ℹ' : '✗')}</span>
          <span>${s.ready ? 'Verified & Loaded' : (s.meta.includes('Skipped') ? 'Skipped Safeguard' : 'Missing')}</span>
        </div>
        <div style="font-size: 0.6875rem; color: var(--text-muted); margin-top: 4px;">${s.meta}</div>
      </div>
    `).join("");
  }

  // Event Listeners
  if (elSelectActiveRun) {
    elSelectActiveRun.addEventListener("change", (e) => {
      loadData(e.target.value);
    });
  }

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
    loadRuns();
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
  loadRuns();
  loadData();
});
