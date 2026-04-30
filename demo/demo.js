(() => {
  const BACKEND = "https://cairn.kaldera.dev";

  const listEl = document.getElementById("query-list");
  const titleEl = document.getElementById("panel-title");
  const urlEl = document.getElementById("panel-url");
  const jsonEl = document.getElementById("panel-json");
  const statusEl = document.getElementById("panel-status");
  const formEl = document.getElementById("custom-form");
  const customEndpointEl = document.getElementById("custom-endpoint");
  const customParamsEl = document.getElementById("custom-params");

  const map = L.map("map", {
    zoomControl: true,
    attributionControl: true,
  }).setView([47.166, 9.555], 9);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 18,
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  }).addTo(map);

  let activeMarkers = [];
  let activePolygons = [];

  function clearMarkers() {
    activeMarkers.forEach((m) => map.removeLayer(m));
    activeMarkers = [];
    activePolygons.forEach((p) => map.removeLayer(p));
    activePolygons = [];
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function syntaxHighlight(value) {
    const json = JSON.stringify(value, null, 2);
    return escapeHtml(json).replace(
      /("(\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?)/g,
      (match) => {
        let cls = "json-number";
        if (/^"/.test(match)) {
          cls = /:$/.test(match) ? "json-key" : "json-string";
        } else if (/true|false/.test(match)) {
          cls = "json-boolean";
        } else if (/null/.test(match)) {
          cls = "json-null";
        }
        return `<span class="${cls}">${match}</span>`;
      },
    );
  }

  function pointsFromBody(body) {
    const out = [];
    if (!body || typeof body !== "object") return out;

    // Standard search / structured / reverse envelope.
    if (Array.isArray(body.results)) {
      for (const r of body.results) {
        if (Number.isFinite(r.lat) && Number.isFinite(r.lon)) {
          out.push({
            lat: r.lat,
            lon: r.lon,
            label: r.name,
            kind: r.kind,
            gid: r.gid,
          });
        }
      }
    }

    // /v1/place returns a GeoJSON FeatureCollection.
    if (Array.isArray(body.features)) {
      for (const f of body.features) {
        const c = f && f.geometry && f.geometry.coordinates;
        const p = f && f.properties;
        if (Array.isArray(c) && c.length >= 2) {
          const [lon, lat] = c;
          out.push({
            lat,
            lon,
            label: (p && (p.name || p.label)) || "?",
            kind: (p && p.kind) || "place",
            gid: p && p.gid,
          });
        }
      }
    }

    // /v1/buildings rows.
    if (Array.isArray(body.buildings)) {
      for (const b of body.buildings) {
        if (Array.isArray(b.centroid) && b.centroid.length >= 2) {
          const [lon, lat] = b.centroid;
          out.push({
            lat,
            lon,
            label: b.id || "building",
            kind: "building",
          });
        }
      }
    }

    if (Number.isFinite(body.lat) && Number.isFinite(body.lon)) {
      out.push({
        lat: body.lat,
        lon: body.lon,
        label: "query",
        kind: "probe",
        probe: true,
      });
    }
    return out;
  }

  /// Building outer rings ship as `[[lon, lat], …]`. Leaflet's
  /// `L.polygon` expects `[[lat, lon], …]`. Returns null when the
  /// ring is malformed so the caller can skip the layer cleanly.
  function ringAsLatLng(ring) {
    if (!Array.isArray(ring) || ring.length < 3) return null;
    const out = [];
    for (const v of ring) {
      if (!Array.isArray(v) || v.length < 2) return null;
      const [lon, lat] = v;
      if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
      out.push([lat, lon]);
    }
    return out;
  }

  function polygonsFromBody(body, bounds) {
    if (!body || !Array.isArray(body.buildings)) return;
    for (const b of body.buildings) {
      const ll = ringAsLatLng(b.outer_ring);
      if (!ll) continue;
      const heightLabel = Number.isFinite(b.height) ? `${b.height} m` : "—";
      const poly = L.polygon(ll, {
        color: "#c96442",
        weight: 1.5,
        fillColor: "#c96442",
        fillOpacity: 0.18,
      })
        .addTo(map)
        .bindPopup(
          `<b>${escapeHtml(b.id || "?")}</b><br>height: ${escapeHtml(heightLabel)}`,
        );
      activePolygons.push(poly);
      ll.forEach((p) => bounds.extend(p));
    }
  }

  function renderBody(title, urlPath, body, statusLabel) {
    titleEl.textContent = title;
    urlEl.textContent = `GET ${BACKEND}${urlPath}`;
    statusEl.textContent = statusLabel;
    statusEl.dataset.kind = statusLabel.startsWith("error")
      ? "error"
      : statusLabel === "live"
        ? "live"
        : "stale";
    jsonEl.innerHTML = syntaxHighlight(body);

    clearMarkers();
    const points = pointsFromBody(body);
    const bounds = L.latLngBounds([]);
    polygonsFromBody(body, bounds);
    points.forEach((p) => {
      const isProbe = p.probe === true;
      const isBuilding = p.kind === "building";
      const popupGid = p.gid
        ? `<br><code style="font-size:11px">${escapeHtml(p.gid)}</code>`
        : "";
      const marker = L.circleMarker([p.lat, p.lon], {
        radius: isProbe ? 9 : isBuilding ? 5 : 7,
        color: isProbe ? "#f59e0b" : isBuilding ? "#c96442" : "#84cc16",
        weight: 2,
        fillColor: isProbe ? "#f59e0b" : isBuilding ? "#c96442" : "#65a30d",
        fillOpacity: 0.85,
      })
        .addTo(map)
        .bindPopup(
          `<b>${escapeHtml(p.label || "?")}</b><br>${escapeHtml(p.kind || "")}<br>${p.lat.toFixed(4)}, ${p.lon.toFixed(4)}${popupGid}`,
        );
      activeMarkers.push(marker);
      bounds.extend([p.lat, p.lon]);
    });
    if (bounds.isValid()) {
      map.fitBounds(bounds, { padding: [40, 40], maxZoom: 17 });
    }
  }

  async function runQuery({ title, urlPath, fallback }) {
    document.querySelectorAll("#query-list button").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.url === urlPath);
    });
    statusEl.textContent = "loading…";
    statusEl.dataset.kind = "loading";
    try {
      const resp = await fetch(`${BACKEND}${urlPath}`, { cache: "no-store" });
      const body = await resp.json();
      renderBody(title, urlPath, body, resp.ok ? "live" : `error ${resp.status}`);
    } catch (err) {
      if (fallback) {
        renderBody(
          title,
          urlPath,
          fallback,
          `offline (cached) — ${err.message}`,
        );
      } else {
        renderBody(
          title,
          urlPath,
          { error: String(err) },
          `error — ${err.message}`,
        );
      }
    }
  }

  function buildQueryList(fixtures) {
    listEl.innerHTML = "";
    for (const fx of fixtures) {
      const li = document.createElement("li");
      const btn = document.createElement("button");
      btn.dataset.label = fx.label;
      btn.dataset.url = fx.url;
      btn.textContent = fx.title;
      btn.addEventListener("click", () =>
        runQuery({ title: fx.title, urlPath: fx.url, fallback: fx.body }),
      );
      li.appendChild(btn);
      listEl.appendChild(li);
    }
  }

  function setupCustomForm() {
    formEl.addEventListener("submit", (e) => {
      e.preventDefault();
      const endpoint = customEndpointEl.value;
      const params = customParamsEl.value.trim();
      const urlPath = params ? `${endpoint}?${params}` : endpoint;
      runQuery({
        title: `Custom: ${endpoint}`,
        urlPath,
        fallback: null,
      });
    });
  }

  fetch("./fixtures.json", { cache: "no-store" })
    .then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    })
    .then((fixtures) => {
      buildQueryList(fixtures);
      setupCustomForm();
      if (fixtures.length) {
        runQuery({
          title: fixtures[0].title,
          urlPath: fixtures[0].url,
          fallback: fixtures[0].body,
        });
      }
    })
    .catch((err) => {
      titleEl.textContent = "Failed to load fixtures";
      urlEl.textContent = String(err);
      setupCustomForm();
    });
})();
