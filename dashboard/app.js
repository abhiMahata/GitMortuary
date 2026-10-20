/**
 * Git Pattern Analysis — Interactive Dashboard Logic
 * Zero-dependency pure JavaScript client for live GitHub repository analysis,
 * temporal trends, author contributions, and distributed clustering exploration.
 */

document.addEventListener("DOMContentLoaded", () => {
  let dashboardData = null;
  let activeRunId = "sample";

  // Granularity for single-repo time-series chart: 'daily' | 'weekly' | 'monthly'
  let activeGranularity = "daily";

  // Single-repo commit filters
  let commitSearchQuery = "";
  let commitAuthorFilter = "all";
  let commitClusterFilter = "all";
  let commitMergeFilter = "all";
  let commitSortColumn = "date";
  let commitSortDirection = "desc";

  // Multi-repo filters
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

  // DOM Elements - Global
  const elSelectActiveRun = document.getElementById("select-active-run");
  const elBtnRefresh = document.getElementById("btn-refresh");
  const elStatusPill = document.getElementById("pipeline-status-pill");
  const elStatusText = document.getElementById("status-text");

  // DOM Elements - Analysis Form
  const elFormAnalyze = document.getElementById("form-analyze-repo");
  const elInputRepoUrl = document.getElementById("input-repo-url");
  const elRadioModeLimit = document.getElementById("radio-mode-limit");
  const elRadioModeFull = document.getElementById("radio-mode-full");
  const elInputCommitLimit = document.getElementById("input-commit-limit");
  const elLimitContainer = document.getElementById("limit-input-container");
  const elBtnStartAnalysis = document.getElementById("btn-start-analysis");
  const elBtnAnalyzeText = document.getElementById("btn-analyze-text");
  const elAnalysisProgressBox = document.getElementById("analysis-progress-box");
  const elProgressStageTitle = document.getElementById("progress-stage-title");
  const elProgressStageMsg = document.getElementById("progress-stage-msg");
  const elProgressPercentVal = document.getElementById("progress-percent-val");
  const elProgressBarFill = document.getElementById("progress-bar-fill");
  const elAnalysisErrorBox = document.getElementById("analysis-error-box");
  const elAnalysisErrorMsg = document.getElementById("analysis-error-msg");

  // DOM Elements - View Containers
  const elSingleRepoContainer = document.getElementById("single-repo-view-container");
  const elMultiRepoContainer = document.getElementById("multi-repo-view-container");

  // DOM Elements - Multi-Repo
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
  const elFlowContainer = document.getElementById("pipeline-flow-container");

  // DOM Elements - Single-Repo
  const elSingleRepoName = document.getElementById("single-repo-name");
  const elSingleGithubLink = document.getElementById("single-repo-github-link");
  const elSingleLang = document.getElementById("single-repo-lang");
  const elSingleStars = document.getElementById("single-repo-stars");
  const elSingleForks = document.getElementById("single-repo-forks");
  const elSingleCoverageBadge = document.getElementById("single-repo-coverage-badge");
  const elSingleStatCommits = document.getElementById("single-stat-commits");
  const elSingleStatHistoryMode = document.getElementById("single-stat-history-mode");
  const elSingleStatDateRange = document.getElementById("single-stat-daterange");
  const elSingleStatDuration = document.getElementById("single-stat-duration");
  const elSingleStatAdd = document.getElementById("single-stat-add");
  const elSingleStatDel = document.getElementById("single-stat-del");
  const elSingleStatLines = document.getElementById("single-stat-lines");
  const elSingleStatFilesMerges = document.getElementById("single-stat-files-merges");
  const elSingleStatMergeRatio = document.getElementById("single-stat-merge-ratio");
  const elSingleStatAuthors = document.getElementById("single-stat-authors");
  const elSingleMissingStatsAlert = document.getElementById("single-repo-missing-stats-alert");
  const elValMissingStatsCount = document.getElementById("val-missing-stats-count");

  // Granularity buttons
  const elBtnDaily = document.getElementById("btn-granularity-daily");
  const elBtnWeekly = document.getElementById("btn-granularity-weekly");
  const elBtnMonthly = document.getElementById("btn-granularity-monthly");
  const elSingleTimeseriesSvg = document.getElementById("single-repo-timeseries-svg");

  // Single-repo Authors
  const elSingleAuthorTableBody = document.getElementById("single-author-table-body");

  // Single-repo Clustering
  const elValCommitSilhouette = document.getElementById("val-commit-silhouette");
  const elCommitClusteringNotice = document.getElementById("commit-clustering-notice");
  const elCommitClusterCards = document.getElementById("commit-cluster-cards-container");

  // Single-repo Files
  const elSingleFilesTableBody = document.getElementById("single-files-table-body");

  // Single-repo Commits Explorer
  const elInputCommitSearch = document.getElementById("input-commit-search");
  const elSelectCommitAuthor = document.getElementById("select-commit-author-filter");
  const elSelectCommitCluster = document.getElementById("select-commit-cluster-filter");
  const elSelectCommitMerge = document.getElementById("select-commit-merge-filter");
  const elBtnResetCommitFilters = document.getElementById("btn-reset-commit-filters");
  const elCommitsDetailTableBody = document.getElementById("commits-detail-table-body");

  // Export buttons
  const elBtnExportCommitsCsv = document.getElementById("btn-export-commits-csv");
  const elBtnExportRepoJson = document.getElementById("btn-export-repo-json");
  const elBtnExportMultiCsv = document.getElementById("btn-export-multi-csv");
  const elBtnExportMultiJson = document.getElementById("btn-export-multi-json");

  // =========================================================================
  // 1. ANALYSIS SUBMISSION & PROGRESS POLLING
  // =========================================================================

  // Handle radio toggle
  if (elRadioModeLimit && elRadioModeFull && elLimitContainer) {
    elRadioModeLimit.addEventListener("change", () => {
      if (elRadioModeLimit.checked) {
        elLimitContainer.style.opacity = "1";
        elLimitContainer.style.pointerEvents = "auto";
      }
    });
    elRadioModeFull.addEventListener("change", () => {
      if (elRadioModeFull.checked) {
        elLimitContainer.style.opacity = "0.35";
        elLimitContainer.style.pointerEvents = "none";
      }
    });
  }

  // Handle form submission
  if (elFormAnalyze) {
    elFormAnalyze.addEventListener("submit", async (e) => {
      e.preventDefault();
      const rawInput = (elInputRepoUrl.value || "").trim();
      if (!rawInput) {
        showAnalysisError("Please enter a GitHub repository URL or owner/repository identifier.");
        return;
      }

      hideAnalysisError();
      const mode = elRadioModeFull && elRadioModeFull.checked ? "full_history" : "recent_limit";
      let limit = 10;
      if (mode === "recent_limit") {
        limit = parseInt(elInputCommitLimit.value, 10);
        if (isNaN(limit) || limit <= 0) limit = 10;
      } else {
        limit = null;
      }

      // UI state: in progress
      setAnalysisBusy(true, "Validating…", "Validating repository and checking GitHub API accessibility…", 10);

      try {
        const resp = await fetch("/api/analyze", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: jsonStringifySafe({
            repository: rawInput,
            mode: mode,
            limit: limit,
          }),
        });

        const result = await resp.json();
        if (!resp.ok) {
          throw new Error(result.error || `HTTP ${resp.status}: ${resp.statusText}`);
        }

        const runId = result.run_id;
        // Start polling progress
        pollAnalysisTask(runId);
      } catch (err) {
        setAnalysisBusy(false);
        showAnalysisError(err.message);
      }
    });
  }

  function setAnalysisBusy(busy, stageText = "Processing…", msgText = "Please wait…", percent = 10) {
    if (busy) {
      if (elBtnStartAnalysis) elBtnStartAnalysis.disabled = true;
      if (elBtnAnalyzeText) elBtnAnalyzeText.textContent = "Analyzing…";
      if (elAnalysisProgressBox) elAnalysisProgressBox.style.display = "block";
      updateProgressDisplay(stageText, msgText, percent);
    } else {
      if (elBtnStartAnalysis) elBtnStartAnalysis.disabled = false;
      if (elBtnAnalyzeText) elBtnAnalyzeText.textContent = "Analyze Repository";
    }
  }

  function updateProgressDisplay(stage, msg, percent) {
    if (elProgressStageTitle) elProgressStageTitle.textContent = stage;
    if (elProgressStageMsg) elProgressStageMsg.textContent = msg;
    if (elProgressPercentVal) elProgressPercentVal.textContent = `${percent}%`;
    if (elProgressBarFill) elProgressBarFill.style.width = `${percent}%`;
  }

  function showAnalysisError(msg) {
    if (elAnalysisErrorBox && elAnalysisErrorMsg) {
      elAnalysisErrorMsg.textContent = msg;
      elAnalysisErrorBox.style.display = "flex";
    }
  }

  function hideAnalysisError() {
    if (elAnalysisErrorBox) {
      elAnalysisErrorBox.style.display = "none";
    }
  }

  function pollAnalysisTask(runId) {
    let pollInterval = setInterval(async () => {
      try {
        const resp = await fetch(`/api/analyze/status?run_id=${encodeURIComponent(runId)}`);
        if (!resp.ok) {
          clearInterval(pollInterval);
          setAnalysisBusy(false);
          showAnalysisError(`Error polling task status: HTTP ${resp.status}`);
          return;
        }

        const task = await resp.json();
        updateProgressDisplay(
          capitalize(task.stage || "processing"),
          task.message || "Working…",
          task.progress_percent || 20
        );

        if (task.status === "complete" || task.stage === "complete") {
          clearInterval(pollInterval);
          updateProgressDisplay("Complete", "Analysis successfully completed! Loading view…", 100);
          setTimeout(async () => {
            setAnalysisBusy(false);
            if (elAnalysisProgressBox) elAnalysisProgressBox.style.display = "none";
            await loadRuns();
            await loadData(runId);
          }, 600);
        } else if (task.status === "failed") {
          clearInterval(pollInterval);
          setAnalysisBusy(false);
          if (elAnalysisProgressBox) elAnalysisProgressBox.style.display = "none";
          showAnalysisError(task.error || task.message || "Analysis failed.");
        }
      } catch (err) {
        clearInterval(pollInterval);
        setAnalysisBusy(false);
        showAnalysisError(`Polling connection error: ${err.message}`);
      }
    }, 800);
  }

  // =========================================================================
  // 2. RUN SELECTION & DATA LOADING
  // =========================================================================

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

  async function loadData(runId = null) {
    if (runId !== null) {
      activeRunId = runId;
    }
    const currentRun = activeRunId || "sample";

    try {
      if (elTableBody) {
        elTableBody.innerHTML = `<tr><td colspan="11" class="empty-state">Loading data from pipeline artifacts…</td></tr>`;
      }
      if (elCommitsDetailTableBody) {
        elCommitsDetailTableBody.innerHTML = `<tr><td colspan="10" class="empty-state">Loading commit records…</td></tr>`;
      }

      const url = currentRun === "sample" ? "/api/data" : `/api/data?run=${encodeURIComponent(currentRun)}`;
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`HTTP error ${response.status}: ${response.statusText}`);
      }
      dashboardData = await response.json();
      renderAll();
    } catch (err) {
      console.error("Failed to load dashboard data:", err);
      const errHtml = `
        <tr>
          <td colspan="11" class="empty-state" style="color: var(--accent-rose);">
            <div class="empty-state-icon">⚠️</div>
            <strong>Failed to load pipeline data:</strong> ${escapeHtml(err.message)}<br>
            <small style="color: var(--text-muted); margin-top: 6px; display: inline-block;">
              Ensure <code>dashboard/server.py</code> is running and artifacts exist for run <code>${escapeHtml(currentRun)}</code>.
            </small>
          </td>
        </tr>`;
      if (elTableBody) elTableBody.innerHTML = errHtml;
    }
  }

  function renderAll() {
    if (!dashboardData) return;

    // Run Status Alert
    const runAlert = document.getElementById("run-status-alert");
    const runInfo = dashboardData.run_info || {};

    if (runAlert) {
      if (runInfo.status === "failed") {
        runAlert.style.display = "block";
        runAlert.style.background = "rgba(244, 63, 94, 0.14)";
        runAlert.style.borderColor = "rgba(244, 63, 94, 0.4)";
        const errText = runInfo.errors && runInfo.errors.length ? runInfo.errors.join("; ") : "This analysis run did not finish successfully.";
        runAlert.innerHTML = `<strong>⚠️ Pipeline Run Failed:</strong> ${escapeHtml(errText)}`;
      } else if (runInfo.status === "empty") {
        runAlert.style.display = "block";
        runAlert.style.background = "rgba(245, 158, 11, 0.14)";
        runAlert.style.borderColor = "rgba(245, 158, 11, 0.4)";
        runAlert.innerHTML = `<strong>ℹ Empty Run:</strong> This analysis directory contains no repository or commit artifact data.`;
      } else if (runInfo.status === "malformed") {
        runAlert.style.display = "block";
        runAlert.style.background = "rgba(245, 158, 11, 0.14)";
        runAlert.style.borderColor = "rgba(245, 158, 11, 0.4)";
        runAlert.innerHTML = `<strong>⚠️ Malformed Manifest:</strong> The run manifest could not be decoded.`;
      } else if (runInfo.status === "incomplete") {
        runAlert.style.display = "block";
        runAlert.style.background = "rgba(245, 158, 11, 0.14)";
        runAlert.style.borderColor = "rgba(245, 158, 11, 0.4)";
        runAlert.innerHTML = `<strong>ℹ Incomplete Run:</strong> This run directory is missing a completed pipeline manifest.`;
      } else {
        runAlert.style.display = "none";
        runAlert.innerHTML = "";
      }
    }

    renderProvenanceBanner();

    // Duality branch: Single-Repository vs Multi-Repository View
    if (dashboardData.is_single_repo && dashboardData.single_repo) {
      if (elSingleRepoContainer) elSingleRepoContainer.style.display = "block";
      if (elMultiRepoContainer) elMultiRepoContainer.style.display = "none";

      renderSingleRepoHeader();
      renderSingleRepoTemporal();
      renderSingleRepoAuthors();
      renderSingleRepoClustering();
      renderSingleRepoFiles();
      populateCommitAuthorFilter();
      renderSingleRepoCommitsTable();
    } else {
      if (elSingleRepoContainer) elSingleRepoContainer.style.display = "none";
      if (elMultiRepoContainer) elMultiRepoContainer.style.display = "block";

      renderOverview();
      renderClusterCards();
      renderClusterChart();
      populateLanguageFilter();
      renderTable();
      renderPipelineFlow();
    }
  }

  // =========================================================================
  // 3. PROVENANCE BANNER
  // =========================================================================

  function renderProvenanceBanner() {
    const prov = dashboardData.provenance_notice || {};
    const elBanner = document.getElementById("provenance-banner");
    const elBannerText = document.getElementById("provenance-notice-text");
    if (!elBanner || !elBannerText) return;

    const runId = dashboardData.run_info?.run_id || "";
    let truncMsg = "";
    if (prov.has_truncated_history && prov.truncated_notice) {
      truncMsg = `<strong>Partial Sample Notice:</strong> ${escapeHtml(prov.truncated_notice)} `;
    }

    if (prov.is_authentic_live) {
      elBanner.style.background = "linear-gradient(90deg, rgba(16, 185, 129, 0.14) 0%, rgba(17, 24, 39, 0.75) 100%)";
      elBanner.style.borderColor = "rgba(16, 185, 129, 0.4)";
      const icon = elBanner.querySelector(".banner-icon");
      if (icon) icon.textContent = "🛡️";
      const h4 = elBanner.querySelector("h4");
      if (h4) {
        h4.textContent = `Authentic Live GitHub Run: ${runId}`;
        h4.style.color = "#34d399";
      }
      elBannerText.innerHTML = `
        <strong>100% Authentic Live Data:</strong> All commit statistics and diff metrics were retrieved directly from the live GitHub REST API.
        ${truncMsg}
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
        ${truncMsg}
        <strong>Unsupervised Clustering Notice:</strong> ${escapeHtml(prov.unsupervised_notice || "")}
      `;
    }
  }

  // =========================================================================
  // 4. SINGLE-REPOSITORY VIEWS & VISUALIZATIONS
  // =========================================================================

  function renderSingleRepoHeader() {
    const sr = dashboardData.single_repo;
    if (!sr) return;

    const summary = sr.summary || {};
    const meta = sr.repo_metadata || {};

    if (elSingleRepoName) elSingleRepoName.textContent = sr.repository;
    if (elSingleGithubLink) {
      elSingleGithubLink.href = `https://github.com/${sr.repository}`;
      elSingleGithubLink.style.display = "inline-flex";
    }

    if (elSingleLang) elSingleLang.textContent = `Language: ${meta.language || "Unknown"}`;
    if (elSingleStars) elSingleStars.textContent = `Stars: ${meta.stars != null ? Number(meta.stars).toLocaleString() : "--"}`;
    if (elSingleForks) elSingleForks.textContent = `Forks: ${meta.forks != null ? Number(meta.forks).toLocaleString() : "--"}`;

    if (elSingleCoverageBadge) {
      if (summary.is_truncated) {
        elSingleCoverageBadge.className = "badge-truncated";
        elSingleCoverageBadge.textContent = `Truncated (${summary.total_commits} of ${summary.requested_limit || "limit"})`;
      } else {
        elSingleCoverageBadge.className = "meta-pill";
        elSingleCoverageBadge.style.color = "#34d399";
        elSingleCoverageBadge.textContent = `✓ Complete History (${summary.total_commits} commits)`;
      }
    }

    // Stats Grid
    if (elSingleStatCommits) elSingleStatCommits.textContent = Number(summary.total_commits || 0).toLocaleString();
    if (elSingleStatHistoryMode) {
      const modeText = summary.history_mode === "full_history" ? "All Available History" : `Recent Limit (${summary.requested_limit || summary.total_commits})`;
      elSingleStatHistoryMode.textContent = modeText;
    }

    const dr = summary.date_range || {};
    if (elSingleStatDateRange) {
      if (dr.earliest && dr.latest) {
        const d1 = dr.earliest.slice(0, 10);
        const d2 = dr.latest.slice(0, 10);
        elSingleStatDateRange.textContent = `${d1} → ${d2}`;
      } else {
        elSingleStatDateRange.textContent = "N/A";
      }
    }
    if (elSingleStatDuration) {
      elSingleStatDuration.textContent = `Duration: ${dr.duration_days || 0} days`;
    }

    if (elSingleStatAdd) elSingleStatAdd.textContent = `+${Number(summary.total_additions || 0).toLocaleString()}`;
    if (elSingleStatDel) elSingleStatDel.textContent = `-${Number(summary.total_deletions || 0).toLocaleString()}`;
    if (elSingleStatLines) elSingleStatLines.textContent = `Net Lines Changed: ${Number(summary.total_lines_changed || 0).toLocaleString()}`;

    if (elSingleStatFilesMerges) {
      elSingleStatFilesMerges.textContent = `${summary.total_files_changed || 0} files / ${summary.merge_count || 0} merges`;
    }
    if (elSingleStatMergeRatio) {
      elSingleStatMergeRatio.textContent = `Merge Ratio: ${(summary.merge_ratio * 100).toFixed(1)}%`;
    }

    if (elSingleStatAuthors) elSingleStatAuthors.textContent = summary.distinct_authors_count || 0;

    // Missing stats notice
    if (elSingleMissingStatsAlert && elValMissingStatsCount) {
      if (summary.missing_stats_count > 0) {
        elSingleMissingStatsAlert.style.display = "block";
        elValMissingStatsCount.textContent = summary.missing_stats_count;
      } else {
        elSingleMissingStatsAlert.style.display = "none";
      }
    }
  }

  // Temporal Time-Series Chart (SVG)
  function renderSingleRepoTemporal() {
    const sr = dashboardData.single_repo;
    if (!sr || !elSingleTimeseriesSvg) return;

    let dataset = [];
    let dateKey = "date";
    if (activeGranularity === "daily") {
      dataset = sr.daily_activity || [];
      dateKey = "date";
    } else if (activeGranularity === "weekly") {
      dataset = sr.weekly_activity || [];
      dateKey = "week";
    } else {
      dataset = sr.monthly_activity || [];
      dateKey = "month";
    }

    if (!dataset.length) {
      elSingleTimeseriesSvg.innerHTML = `
        <div style="padding: 40px; text-align: center; color: var(--text-muted);">
          No commit timestamps available for time-series visualization.
        </div>`;
      return;
    }

    // Chart dimensions
    const width = 1000;
    const height = 260;
    const margin = { top: 20, right: 30, bottom: 45, left: 50 };
    const chartW = width - margin.left - margin.right;
    const chartH = height - margin.top - margin.bottom;

    const maxCommits = Math.max(1, ...dataset.map(d => d.commits || 0));

    const n = dataset.length;
    const barWidth = Math.max(4, Math.min(36, (chartW / n) * 0.7));
    const step = chartW / n;

    // Generate bars and axis labels
    const barsSvg = dataset.map((d, i) => {
      const commits = d.commits || 0;
      const barH = (commits / maxCommits) * chartH;
      const x = margin.left + i * step + (step - barWidth) / 2;
      const y = margin.top + (chartH - barH);

      const label = d[dateKey] || "";
      const showLabel = n <= 15 || i % Math.ceil(n / 10) === 0 || i === n - 1;

      return `
        <g class="chart-bar-group">
          <title>${label}: ${commits} commits, +${d.additions || 0} additions, -${d.deletions || 0} deletions</title>
          <rect x="${x}" y="${y}" width="${barWidth}" height="${barH}" fill="url(#cyanGrad)" rx="3" />
          ${showLabel ? `
            <text x="${x + barWidth / 2}" y="${height - 12}" fill="#94a3b8" font-size="10" text-anchor="middle" font-family="sans-serif">
              ${escapeHtml(label)}
            </text>
          ` : ""}
        </g>
      `;
    }).join("");

    // Horizontal grid lines
    const gridLines = [0, 0.25, 0.5, 0.75, 1.0].map(ratio => {
      const y = margin.top + chartH * (1 - ratio);
      const val = Math.round(maxCommits * ratio);
      return `
        <line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(255,255,255,0.08)" stroke-dasharray="3 3" />
        <text x="${margin.left - 8}" y="${y + 4}" fill="#64748b" font-size="10" text-anchor="end" font-family="sans-serif">${val}</text>
      `;
    }).join("");

    elSingleTimeseriesSvg.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" style="width: 100%; height: auto; overflow: visible;">
        <defs>
          <linearGradient id="cyanGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="#06b6d4" />
            <stop offset="100%" stop-color="#0891b2" stop-opacity="0.8" />
          </linearGradient>
        </defs>
        ${gridLines}
        ${barsSvg}
      </svg>
    `;
  }

  // Author Contributions
  function renderSingleRepoAuthors() {
    const sr = dashboardData.single_repo;
    if (!sr || !elSingleAuthorTableBody) return;

    const authors = sr.author_contributions || [];
    if (!authors.length) {
      elSingleAuthorTableBody.innerHTML = `<tr><td colspan="8" class="empty-state">No author contributions available.</td></tr>`;
      return;
    }

    elSingleAuthorTableBody.innerHTML = authors.map(a => {
      const firstTs = a.first_commit ? a.first_commit.slice(0, 10) : "--";
      const lastTs = a.last_commit ? a.last_commit.slice(0, 10) : "--";
      const activeSpan = firstTs === lastTs ? firstTs : `${firstTs} → ${lastTs}`;

      return `
        <tr>
          <td style="font-weight: 600; color: #f8fafc;">${escapeHtml(a.author)}</td>
          <td style="font-family: var(--font-mono); font-weight: 600;">${a.commits}</td>
          <td>
            <div style="display: flex; align-items: center; gap: 8px;">
              <span style="font-family: var(--font-mono); font-size: 0.75rem;">${a.percentage}%</span>
              <div style="width: 60px; height: 5px; background: rgba(255,255,255,0.08); border-radius: 99px; overflow: hidden;">
                <div style="width: ${a.percentage}%; height: 100%; background: var(--accent-cyan); border-radius: 99px;"></div>
              </div>
            </div>
          </td>
          <td style="font-family: var(--font-mono); color: var(--accent-emerald);">+${Number(a.additions || 0).toLocaleString()}</td>
          <td style="font-family: var(--font-mono); color: var(--accent-rose);">-${Number(a.deletions || 0).toLocaleString()}</td>
          <td style="font-family: var(--font-mono);">${Number(a.lines_changed || 0).toLocaleString()}</td>
          <td style="font-family: var(--font-mono);">${a.merges || 0}</td>
          <td style="font-size: 0.75rem; color: var(--text-secondary);">${activeSpan}</td>
        </tr>
      `;
    }).join("");
  }

  // Commit-Level Clustering
  function renderSingleRepoClustering() {
    const sr = dashboardData.single_repo;
    if (!sr) return;

    const clustering = sr.commit_clustering || {};
    const sil = clustering.silhouette;

    if (elValCommitSilhouette) {
      if (sil != null && Number.isFinite(sil)) {
        elValCommitSilhouette.innerHTML = `
          ${sil.toFixed(4)}
          <span class="badge-valid-silhouette" style="margin-left: 6px;">Valid</span>
        `;
      } else {
        elValCommitSilhouette.innerHTML = `<span class="badge-unavailable-silhouette">Skipped</span>`;
      }
    }

    if (clustering.status === "skipped") {
      if (elCommitClusteringNotice) {
        elCommitClusteringNotice.style.display = "block";
        elCommitClusteringNotice.innerHTML = `
          <strong>ℹ Commit-Level Clustering Skipped:</strong> ${escapeHtml(clustering.reason || "Requires at least 3 commits with distinct diff feature vectors.")}
        `;
      }
      if (elCommitClusterCards) elCommitClusterCards.innerHTML = "";
    } else {
      if (elCommitClusteringNotice) elCommitClusteringNotice.style.display = "none";

      const profiles = clustering.profiles || {};
      const clusterKeys = Object.keys(profiles).sort();

      if (elCommitClusterCards) {
        if (!clusterKeys.length) {
          elCommitClusterCards.innerHTML = `<div style="color: var(--text-muted); padding: 12px;">No cluster profiles generated.</div>`;
        } else {
          elCommitClusterCards.innerHTML = clusterKeys.map(k => {
            const p = profiles[k];
            const color = CLUSTER_COLORS[k] || "#06b6d4";
            return `
              <div class="cluster-card" style="border-top: 3px solid ${color};">
                <div class="cluster-card-header">
                  <span class="cluster-badge" style="background: ${color}25; color: ${color}; border: 1px solid ${color}60;">
                    Commit Cluster ${k}
                  </span>
                  <span class="cluster-repo-count">${p.count} commits</span>
                </div>
                <div class="cluster-metric-list" style="margin-top: 10px;">
                  <div class="cluster-metric-row">
                    <span>Avg Additions:</span>
                    <strong style="color: var(--accent-emerald);">+${p.avg_additions}</strong>
                  </div>
                  <div class="cluster-metric-row">
                    <span>Avg Deletions:</span>
                    <strong style="color: var(--accent-rose);">-${p.avg_deletions}</strong>
                  </div>
                  <div class="cluster-metric-row">
                    <span>Avg Lines Changed:</span>
                    <strong>${p.avg_lines_changed}</strong>
                  </div>
                  <div class="cluster-metric-row">
                    <span>Avg Files Touched:</span>
                    <strong>${p.avg_files_changed}</strong>
                  </div>
                  <div class="cluster-metric-row">
                    <span>Merge Commits:</span>
                    <strong>${p.merges} (${(p.merge_ratio * 100).toFixed(1)}%)</strong>
                  </div>
                </div>
              </div>
            `;
          }).join("");
        }
      }
    }
  }

  // Frequently Changed Files
  function renderSingleRepoFiles() {
    const sr = dashboardData.single_repo;
    if (!sr || !elSingleFilesTableBody) return;

    const files = sr.frequently_changed_files || [];
    if (!files.length) {
      elSingleFilesTableBody.innerHTML = `
        <tr>
          <td colspan="2" class="empty-state" style="padding: 20px;">
            File-level change paths were not included in commit listings or no files were modified.
          </td>
        </tr>`;
      return;
    }

    elSingleFilesTableBody.innerHTML = files.map(f => `
      <tr>
        <td><code>${escapeHtml(f.path)}</code></td>
        <td style="font-family: var(--font-mono); font-weight: 600;">${f.count}</td>
      </tr>
    `).join("");
  }

  // Single-repo Commit Filters & Table
  function populateCommitAuthorFilter() {
    const sr = dashboardData.single_repo;
    if (!sr || !elSelectCommitAuthor) return;

    const authors = sr.author_contributions || [];
    const prev = elSelectCommitAuthor.value;

    elSelectCommitAuthor.innerHTML = '<option value="all">All Authors</option>' +
      authors.map(a => `<option value="${escapeHtml(a.author)}">${escapeHtml(a.author)} (${a.commits})</option>`).join("");

    if (prev && [...elSelectCommitAuthor.options].some(o => o.value === prev)) {
      elSelectCommitAuthor.value = prev;
    }
  }

  function renderSingleRepoCommitsTable() {
    const sr = dashboardData.single_repo;
    if (!sr || !elCommitsDetailTableBody) return;

    let records = sr.commits_detail || [];

    // Filter by search
    if (commitSearchQuery) {
      const q = commitSearchQuery.toLowerCase();
      records = records.filter(c =>
        (c.sha && c.sha.toLowerCase().includes(q)) ||
        (c.message && c.message.toLowerCase().includes(q)) ||
        (c.author && c.author.toLowerCase().includes(q))
      );
    }

    // Filter by author
    if (commitAuthorFilter !== "all") {
      records = records.filter(c => c.author === commitAuthorFilter);
    }

    // Filter by cluster
    if (commitClusterFilter !== "all") {
      records = records.filter(c => String(c.cluster) === String(commitClusterFilter));
    }

    // Filter by merge
    if (commitMergeFilter === "merges_only") {
      records = records.filter(c => c.is_merge);
    } else if (commitMergeFilter === "regular_only") {
      records = records.filter(c => !c.is_merge);
    }

    // Sort
    records.sort((a, b) => {
      let vA = a[commitSortColumn];
      let vB = b[commitSortColumn];
      if (vA == null) vA = "";
      if (vB == null) vB = "";
      if (typeof vA === "string") {
        return commitSortDirection === "asc" ? vA.localeCompare(vB) : vB.localeCompare(vA);
      }
      return commitSortDirection === "asc" ? vA - vB : vB - vA;
    });

    if (!records.length) {
      elCommitsDetailTableBody.innerHTML = `<tr><td colspan="10" class="empty-state">No commits match current filters.</td></tr>`;
      return;
    }

    elCommitsDetailTableBody.innerHTML = records.map(c => {
      const dateStr = c.date ? c.date.replace("T", " ").replace("Z", "") : "--";
      const addStr = c.additions != null ? `<span style="color: var(--accent-emerald);">+${c.additions}</span>` : `<span style="color: var(--text-muted);">N/A</span>`;
      const delStr = c.deletions != null ? `<span style="color: var(--accent-rose);">-${c.deletions}</span>` : `<span style="color: var(--text-muted);">N/A</span>`;
      const linesStr = c.lines_changed != null ? c.lines_changed : "N/A";
      const filesStr = c.files_changed != null ? c.files_changed : "N/A";

      const mergeBadge = c.is_merge
        ? `<span class="col-tag tag-desc" style="background: rgba(139, 92, 246, 0.2);">Merge</span>`
        : `<span style="color: var(--text-muted); font-size: 0.75rem;">No</span>`;

      const clusterBadge = c.cluster != null
        ? `<span class="col-tag tag-group" style="background: ${CLUSTER_COLORS[c.cluster] || '#10b981'}25; color: ${CLUSTER_COLORS[c.cluster] || '#34d399'};">C${c.cluster}</span>`
        : `<span style="color: var(--text-muted); font-size: 0.75rem;">N/A</span>`;

      return `
        <tr>
          <td><code title="${escapeHtml(c.sha)}">${escapeHtml(c.sha_short)}</code></td>
          <td style="font-size: 0.75rem; color: var(--text-secondary); white-space: nowrap;">${dateStr}</td>
          <td style="font-weight: 500;">${escapeHtml(c.author)}</td>
          <td title="${escapeHtml(c.message)}">${escapeHtml(c.message_subject)}</td>
          <td style="font-family: var(--font-mono);">${addStr}</td>
          <td style="font-family: var(--font-mono);">${delStr}</td>
          <td style="font-family: var(--font-mono);">${linesStr}</td>
          <td style="font-family: var(--font-mono);">${filesStr}</td>
          <td>${mergeBadge}</td>
          <td>${clusterBadge}</td>
        </tr>
      `;
    }).join("");
  }

  // =========================================================================
  // 5. MULTI-REPOSITORY VIEWS & VISUALIZATIONS (EXISTING)
  // =========================================================================

  function renderOverview() {
    const ov = dashboardData.overview || {};
    if (elValRepos) elValRepos.textContent = ov.total_repositories != null ? ov.total_repositories : "--";
    if (elValCommits) elValCommits.textContent = ov.total_commits != null ? Number(ov.total_commits).toLocaleString() : "--";
    if (elValEnriched) elValEnriched.textContent = ov.total_enriched != null ? Number(ov.total_enriched).toLocaleString() : "--";

    const summary = dashboardData.cluster_summary || {};
    const subClusters = document.getElementById("val-clusters-sub");
    if (summary.stage_status === "skipped" || summary.skipped) {
      if (elValClusters) elValClusters.textContent = "Skipped";
      if (subClusters) subClusters.textContent = "Requires ≥ 2 repositories";
    } else {
      if (elValClusters) elValClusters.textContent = ov.repositories_clustered != null ? ov.repositories_clustered : "--";
      if (subClusters) {
        const silText = summary.silhouette != null ? ` (Silhouette: ${summary.silhouette.toFixed(3)})` : " (Silhouette: N/A)";
        subClusters.textContent = `Across ${ov.cluster_count || 0} K-Means clusters${silText}`;
      }
    }

    const metaCommits = document.getElementById("val-commits-meta");
    if (metaCommits) {
      if (ov.has_truncated_history) {
        metaCommits.innerHTML = `<span style="color: #fbbf24;">Partial Sample (${ov.truncated_repositories_count} truncated)</span>`;
      } else {
        metaCommits.textContent = "Sampled commit history";
      }
    }

    const subEnriched = document.getElementById("val-enriched-sub");
    if (subEnriched && ov.total_enriched) {
      if (ov.synthetic_enriched === 0) {
        subEnriched.innerHTML = `<span class="badge-authentic" title="Authentic GitHub API records">100% Real Live Data (${ov.authentic_enriched})</span>`;
      } else {
        subEnriched.innerHTML = `
          <span class="badge-authentic" title="Authentic GitHub API records">${ov.authentic_enriched} Real (${(ov.authentic_enriched / ov.total_enriched * 100).toFixed(1)}%)</span>
          <span class="badge-synthetic" title="Deterministic mock-enriched records">${ov.synthetic_enriched} Mock (${ov.synthetic_percentage}%)</span>
        `;
      }
    }
  }

  function renderClusterCards() {
    const summary = dashboardData.cluster_summary || {};
    const profiles = summary.profiles || {};
    const clusterIds = summary.cluster_ids || [];
    const ov = dashboardData.overview || {};

    const clusterSubtitle = document.getElementById("cluster-subtitle");
    if (clusterSubtitle) {
      if (summary.stage_status === "skipped" || summary.skipped) {
        clusterSubtitle.textContent = "Clustering skipped: Requires ≥ 2 repositories";
      } else {
        const kVal = summary.k || ov.cluster_count || 0;
        const seedVal = summary.seed != null ? summary.seed : 42;
        clusterSubtitle.textContent = `Unsupervised clustering on 6 normalized repository-level commit features (K=${kVal}, Seed=${seedVal})`;
      }
    }

    const valSil = document.getElementById("val-silhouette");
    const valSilMeasure = document.getElementById("val-silhouette-measure");
    if (valSil) {
      if (summary.stage_status === "skipped" || summary.skipped) {
        valSil.innerHTML = `<span class="badge-unavailable-silhouette">Skipped</span>`;
        if (valSilMeasure) {
          valSilMeasure.textContent = summary.stage_reason || "Clustering requires at least 2 repositories.";
        }
      } else if (summary.silhouette != null && Number.isFinite(summary.silhouette)) {
        valSil.innerHTML = `
          ${summary.silhouette.toFixed(4)}
          <span class="badge-valid-silhouette" style="margin-left: 6px;">Valid</span>
        `;
        if (valSilMeasure) {
          valSilMeasure.textContent = "Measure: Squared Euclidean Distance • Normalized Range: [-1.0, +1.0]";
        }
      } else {
        valSil.innerHTML = `<span class="badge-unavailable-silhouette">N/A</span>`;
      }
    }

    if (!elClusterCards) return;

    if (summary.stage_status === "skipped" || summary.skipped) {
      elClusterCards.innerHTML = `
        <div style="grid-column: 1 / -1; padding: 20px; background: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.3); border-radius: var(--radius-md); color: #fde68a;">
          <strong>ℹ Clustering Skipped:</strong> ${escapeHtml(summary.stage_reason || "Requires at least 2 repositories.")}
        </div>`;
      return;
    }

    if (!clusterIds.length) {
      elClusterCards.innerHTML = `<div class="empty-state">No cluster profiles available</div>`;
      return;
    }

    elClusterCards.innerHTML = clusterIds.map(cid => {
      const p = profiles[cid] || {};
      const isSelected = activeClusterFilter === String(cid);
      const color = CLUSTER_COLORS[cid] || "#06b6d4";

      return `
        <div class="cluster-card ${isSelected ? 'selected' : ''}" data-cluster="${cid}" style="border-top: 3px solid ${color};">
          <div class="cluster-card-header">
            <span class="cluster-badge" style="background: ${color}25; color: ${color}; border: 1px solid ${color}60;">
              Cluster ${cid}
            </span>
            <span class="cluster-repo-count">${p.count || 0} repositories</span>
          </div>
          <div class="cluster-metric-list">
            <div class="cluster-metric-row">
              <span>Avg Additions:</span>
              <strong>${p.avg_additions != null ? Number(p.avg_additions).toLocaleString() : '--'}</strong>
            </div>
            <div class="cluster-metric-row">
              <span>Avg Deletions:</span>
              <strong>${p.avg_deletions != null ? Number(p.avg_deletions).toLocaleString() : '--'}</strong>
            </div>
            <div class="cluster-metric-row">
              <span>Avg Lines Changed:</span>
              <strong>${p.avg_lines_changed != null ? Number(p.avg_lines_changed).toLocaleString() : '--'}</strong>
            </div>
            <div class="cluster-metric-row">
              <span>Avg Files Changed:</span>
              <strong>${p.avg_files_changed != null ? p.avg_files_changed : '--'}</strong>
            </div>
            <div class="cluster-metric-row">
              <span>Avg Merge Ratio:</span>
              <strong>${p.avg_merge_ratio != null ? (p.avg_merge_ratio * 100).toFixed(1) + '%' : '--'}</strong>
            </div>
          </div>
        </div>
      `;
    }).join("");

    elClusterCards.querySelectorAll(".cluster-card").forEach(card => {
      card.addEventListener("click", () => {
        const cid = card.getAttribute("data-cluster");
        if (activeClusterFilter === cid) {
          activeClusterFilter = "all";
          if (elSelectCluster) elSelectCluster.value = "all";
        } else {
          activeClusterFilter = cid;
          if (elSelectCluster) elSelectCluster.value = cid;
        }
        renderClusterCards();
        renderTable();
      });
    });
  }

  function renderClusterChart() {
    if (!elSvgChart) return;
    const summary = dashboardData.cluster_summary || {};
    const profiles = summary.profiles || {};
    const clusterIds = summary.cluster_ids || [];

    if (summary.stage_status === "skipped" || summary.skipped || !clusterIds.length) {
      elSvgChart.innerHTML = `
        <div style="padding: 30px; text-align: center; color: var(--text-muted); font-size: 0.8125rem;">
          Feature comparison chart unavailable (Clustering skipped for single-repository run).
        </div>`;
      if (elChartLegend) elChartLegend.innerHTML = "";
      return;
    }

    const metrics = [
      { key: "avg_additions", label: "Avg Additions" },
      { key: "avg_deletions", label: "Avg Deletions" },
      { key: "avg_lines_changed", label: "Avg Lines" },
      { key: "avg_files_changed", label: "Avg Files" },
      { key: "avg_merge_ratio", label: "Merge Ratio" },
    ];

    const width = 800;
    const height = 200;
    const padding = { top: 20, right: 20, bottom: 40, left: 110 };
    const chartW = width - padding.left - padding.right;
    const chartH = height - padding.top - padding.bottom;

    const metricMax = {};
    metrics.forEach(m => {
      let maxVal = 0;
      clusterIds.forEach(cid => {
        const val = profiles[cid]?.[m.key] || 0;
        if (val > maxVal) maxVal = val;
      });
      metricMax[m.key] = maxVal > 0 ? maxVal : 1;
    });

    const numMetrics = metrics.length;
    const numClusters = clusterIds.length;
    const groupHeight = chartH / numMetrics;
    const barHeight = Math.max(3, (groupHeight * 0.75) / numClusters);

    let barsSvg = "";
    metrics.forEach((m, mIdx) => {
      const groupY = padding.top + mIdx * groupHeight;
      const labelY = groupY + groupHeight / 2 + 4;
      barsSvg += `
        <text x="${padding.left - 10}" y="${labelY}" fill="#94a3b8" font-size="11" text-anchor="end" font-family="sans-serif">
          ${escapeHtml(m.label)}
        </text>
        <line x1="${padding.left}" y1="${groupY + groupHeight}" x2="${width - padding.right}" y2="${groupY + groupHeight}" stroke="rgba(255,255,255,0.05)" />
      `;

      clusterIds.forEach((cid, cIdx) => {
        const val = profiles[cid]?.[m.key] || 0;
        const normWidth = (val / metricMax[m.key]) * chartW;
        const barY = groupY + (groupHeight - (numClusters * barHeight)) / 2 + cIdx * barHeight;
        const color = CLUSTER_COLORS[cid] || "#06b6d4";

        barsSvg += `
          <rect x="${padding.left}" y="${barY}" width="${Math.max(2, normWidth)}" height="${barHeight - 2}" fill="${color}" rx="2">
            <title>Cluster ${cid}: ${val}</title>
          </rect>
        `;
      });
    });

    elSvgChart.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" style="width: 100%; height: auto; display: block;">
        ${barsSvg}
      </svg>
    `;

    if (elChartLegend) {
      elChartLegend.innerHTML = clusterIds.map(cid => {
        const color = CLUSTER_COLORS[cid] || "#06b6d4";
        return `
          <div class="legend-item">
            <span class="legend-color-dot" style="background: ${color};"></span>
            <span>Cluster ${cid}</span>
          </div>
        `;
      }).join("");
    }
  }

  function populateLanguageFilter() {
    if (!elSelectLang) return;
    const repos = dashboardData.repositories || [];
    const langs = new Set();
    repos.forEach(r => {
      if (r.language) langs.add(r.language);
    });

    const sortedLangs = Array.from(langs).sort();
    const prevVal = elSelectLang.value;

    elSelectLang.innerHTML = '<option value="all">All Languages</option>' +
      sortedLangs.map(l => `<option value="${escapeHtml(l)}">${escapeHtml(l)}</option>`).join("");

    if (prevVal && sortedLangs.includes(prevVal)) {
      elSelectLang.value = prevVal;
    }
  }

  function renderTable() {
    if (!elTableBody) return;
    let repos = dashboardData.repositories || [];

    if (activeClusterFilter !== "all") {
      repos = repos.filter(r => String(r.cluster) === activeClusterFilter);
    }

    if (activeLanguageFilter !== "all") {
      repos = repos.filter(r => r.language === activeLanguageFilter);
    }

    if (activeSearchQuery) {
      const q = activeSearchQuery.toLowerCase();
      repos = repos.filter(r =>
        (r.repo_name && r.repo_name.toLowerCase().includes(q)) ||
        (r.language && r.language.toLowerCase().includes(q))
      );
    }

    repos.sort((a, b) => {
      let vA = a[sortColumn];
      let vB = b[sortColumn];
      if (vA == null) vA = "";
      if (vB == null) vB = "";
      if (typeof vA === "string") {
        return sortDirection === "asc" ? vA.localeCompare(vB) : vB.localeCompare(vA);
      }
      return sortDirection === "asc" ? vA - vB : vB - vA;
    });

    if (!repos.length) {
      elTableBody.innerHTML = `<tr><td colspan="11" class="empty-state">No repositories match active filters.</td></tr>`;
      return;
    }

    elTableBody.innerHTML = repos.map(r => {
      const cid = r.cluster;
      const color = CLUSTER_COLORS[cid] || "#94a3b8";
      const clusterBadge = cid != null
        ? `<span class="col-tag tag-group" style="background: ${color}25; color: ${color};">Cluster ${cid}</span>`
        : `<span style="color: var(--text-muted); font-size: 0.75rem;">N/A</span>`;

      const truncBadge = r.is_truncated ? `<span class="badge-truncated" title="Truncated at limit">Truncated</span>` : "";

      return `
        <tr>
          <td style="font-weight: 600;">
            <a href="https://github.com/${escapeHtml(r.repo_name)}" target="_blank" rel="noopener noreferrer" style="color: #f8fafc; text-decoration: none;">
              ${escapeHtml(r.repo_name)}
            </a>
            ${truncBadge}
          </td>
          <td>${r.language ? `<span class="meta-pill">${escapeHtml(r.language)}</span>` : '<span style="color: var(--text-muted);">--</span>'}</td>
          <td style="font-family: var(--font-mono);">${r.stars != null ? Number(r.stars).toLocaleString() : '--'}</td>
          <td style="font-family: var(--font-mono); font-weight: 600;">${r.commit_count != null ? r.commit_count : '--'}</td>
          <td style="font-family: var(--font-mono);">${r.distinct_authors != null ? r.distinct_authors : '--'}</td>
          <td style="font-family: var(--font-mono);">${r.merge_ratio != null ? (r.merge_ratio * 100).toFixed(1) + '%' : '--'}</td>
          <td style="font-family: var(--font-mono);">${r.avg_additions != null ? Number(r.avg_additions).toLocaleString() : '--'}</td>
          <td style="font-family: var(--font-mono);">${r.avg_deletions != null ? Number(r.avg_deletions).toLocaleString() : '--'}</td>
          <td style="font-family: var(--font-mono);">${r.avg_lines_changed != null ? Number(r.avg_lines_changed).toLocaleString() : '--'}</td>
          <td style="font-family: var(--font-mono);">${r.avg_files_changed != null ? r.avg_files_changed : '--'}</td>
          <td>${clusterBadge}</td>
        </tr>
      `;
    }).join("");
  }

  function renderPipelineFlow() {
    if (!elFlowContainer) return;
    const runInfo = dashboardData.run_info || {};
    const status = dashboardData.artifacts_status || {};

    let stages = [];
    if (runInfo.is_sample) {
      stages = [
        { num: "Stage 1", title: "Repository Discovery", artifact: "data/sample/repositories.jsonl", ready: status.repositories_found, meta: "20 repos with stars > 1,000" },
        { num: "Stage 2", title: "Commit Collection", artifact: "data/sample/commits.jsonl", ready: status.commits_found, meta: "1,000 recent commit records" },
        { num: "Stage 3", title: "Commit Enrichment", artifact: "data/sample/enriched_commits.jsonl", ready: status.enriched_found, meta: "15 Real / 985 Mock (synthetic=True)" },
        { num: "Stage 4", title: "MapReduce Features", artifact: status.features_source === "distributed_mr" ? "repo_features (HDFS YARN)" : "data/output/repo_features_local.jsonl", ready: status.features_found, meta: "20 repo feature vectors" },
        { num: "Stage 5", title: "Spark MLlib Clustering", artifact: "data/output/repo_clusters.json", ready: status.clusters_found, meta: "K=4 clusters, seed=42" },
      ];
    } else {
      const runStages = runInfo.stages || {};
      const mrMode = runStages.mapreduce?.mode === "hadoop_streaming" ? "Hadoop Streaming" : "Local Streaming";
      const sparkSkipped = runStages.spark?.status === "skipped" || (dashboardData.cluster_summary && dashboardData.cluster_summary.skipped);
      const sparkReady = status.clusters_found && !sparkSkipped;

      stages = [
        { num: "Stage 1", title: "Dynamic Ingestion", artifact: `data/runs/${runInfo.run_id}/`, ready: status.repositories_found && status.commits_found, meta: `${dashboardData.overview.total_repositories || 1} repo(s), ${dashboardData.overview.total_commits || 0} commits` },
        { num: "Stage 2", title: "Enriched Commits", artifact: "enriched_commits.jsonl", ready: status.enriched_found, meta: `${dashboardData.overview.total_enriched || 0} authentic diff records` },
        { num: "Stage 3", title: "MapReduce Features", artifact: "repo_features.jsonl", ready: status.features_found, meta: `${mrMode} (${dashboardData.overview.total_repositories || 1} vectors)` },
        { num: "Stage 4", title: "Spark MLlib Clustering", artifact: "repo_clusters.json", ready: sparkReady, meta: sparkSkipped ? "Skipped (<2 repos)" : `${dashboardData.overview.cluster_count || 0} clusters` },
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

  // =========================================================================
  // 6. EVENT LISTENERS
  // =========================================================================

  if (elSelectActiveRun) {
    elSelectActiveRun.addEventListener("change", (e) => {
      loadData(e.target.value);
    });
  }

  if (elBtnRefresh) {
    elBtnRefresh.addEventListener("click", () => {
      loadRuns();
      loadData();
    });
  }

  // Granularity toggles
  if (elBtnDaily && elBtnWeekly && elBtnMonthly) {
    elBtnDaily.addEventListener("click", () => {
      activeGranularity = "daily";
      elBtnDaily.classList.add("active");
      elBtnWeekly.classList.remove("active");
      elBtnMonthly.classList.remove("active");
      renderSingleRepoTemporal();
    });
    elBtnWeekly.addEventListener("click", () => {
      activeGranularity = "weekly";
      elBtnDaily.classList.remove("active");
      elBtnWeekly.classList.add("active");
      elBtnMonthly.classList.remove("active");
      renderSingleRepoTemporal();
    });
    elBtnMonthly.addEventListener("click", () => {
      activeGranularity = "monthly";
      elBtnDaily.classList.remove("active");
      elBtnWeekly.classList.remove("active");
      elBtnMonthly.classList.add("active");
      renderSingleRepoTemporal();
    });
  }

  // Single-repo commit search & filters
  if (elInputCommitSearch) {
    elInputCommitSearch.addEventListener("input", (e) => {
      commitSearchQuery = e.target.value.trim();
      renderSingleRepoCommitsTable();
    });
  }
  if (elSelectCommitAuthor) {
    elSelectCommitAuthor.addEventListener("change", (e) => {
      commitAuthorFilter = e.target.value;
      renderSingleRepoCommitsTable();
    });
  }
  if (elSelectCommitCluster) {
    elSelectCommitCluster.addEventListener("change", (e) => {
      commitClusterFilter = e.target.value;
      renderSingleRepoCommitsTable();
    });
  }
  if (elSelectCommitMerge) {
    elSelectCommitMerge.addEventListener("change", (e) => {
      commitMergeFilter = e.target.value;
      renderSingleRepoCommitsTable();
    });
  }
  if (elBtnResetCommitFilters) {
    elBtnResetCommitFilters.addEventListener("click", () => {
      commitSearchQuery = "";
      commitAuthorFilter = "all";
      commitClusterFilter = "all";
      commitMergeFilter = "all";
      if (elInputCommitSearch) elInputCommitSearch.value = "";
      if (elSelectCommitAuthor) elSelectCommitAuthor.value = "all";
      if (elSelectCommitCluster) elSelectCommitCluster.value = "all";
      if (elSelectCommitMerge) elSelectCommitMerge.value = "all";
      renderSingleRepoCommitsTable();
    });
  }

  // Commits detail table header sort
  document.querySelectorAll("#commits-detail-table th").forEach(th => {
    th.addEventListener("click", () => {
      const col = th.getAttribute("data-col");
      if (!col) return;
      if (commitSortColumn === col) {
        commitSortDirection = commitSortDirection === "asc" ? "desc" : "asc";
      } else {
        commitSortColumn = col;
        commitSortDirection = "asc";
      }
      renderSingleRepoCommitsTable();
    });
  });

  // Multi-repo search & filters
  if (elInputSearch) {
    elInputSearch.addEventListener("input", (e) => {
      activeSearchQuery = e.target.value.trim();
      renderTable();
    });
  }
  if (elSelectCluster) {
    elSelectCluster.addEventListener("change", (e) => {
      activeClusterFilter = e.target.value;
      renderClusterCards();
      renderTable();
    });
  }
  if (elSelectLang) {
    elSelectLang.addEventListener("change", (e) => {
      activeLanguageFilter = e.target.value;
      renderTable();
    });
  }
  if (elBtnReset) {
    elBtnReset.addEventListener("click", () => {
      activeClusterFilter = "all";
      activeLanguageFilter = "all";
      activeSearchQuery = "";
      if (elInputSearch) elInputSearch.value = "";
      if (elSelectCluster) elSelectCluster.value = "all";
      if (elSelectLang) elSelectLang.value = "all";
      renderClusterCards();
      renderTable();
    });
  }

  // Multi-repo table header sort
  document.querySelectorAll("#repos-table th").forEach(th => {
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

  // Export handlers
  function triggerExport(format) {
    const runId = activeRunId || "sample";
    window.open(`/api/export?run=${encodeURIComponent(runId)}&format=${encodeURIComponent(format)}`, "_blank");
  }

  if (elBtnExportCommitsCsv) elBtnExportCommitsCsv.addEventListener("click", () => triggerExport("csv"));
  if (elBtnExportRepoJson) elBtnExportRepoJson.addEventListener("click", () => triggerExport("json"));
  if (elBtnExportMultiCsv) elBtnExportMultiCsv.addEventListener("click", () => triggerExport("csv"));
  if (elBtnExportMultiJson) elBtnExportMultiJson.addEventListener("click", () => triggerExport("json"));

  // Helpers
  function escapeHtml(str) {
    if (str == null) return "";
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function capitalize(str) {
    if (!str) return "";
    return str.charAt(0).toUpperCase() + str.slice(1);
  }

  function jsonStringifySafe(obj) {
    try {
      return JSON.stringify(obj);
    } catch {
      return "{}";
    }
  }

  // Initial load
  loadRuns();
  loadData();
});
