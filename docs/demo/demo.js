(() => {
  const listEl = document.getElementById("query-list");
  const titleEl = document.getElementById("panel-title");
  const urlEl = document.getElementById("panel-url");
  const jsonEl = document.getElementById("panel-json");

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
  let activeQuery = null;

  function clearMarkers() {
    activeMarkers.forEach((m) => map.removeLayer(m));
    activeMarkers = [];
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
    if (Array.isArray(body.results)) {
      for (const r of body.results) {
        if (Number.isFinite(r.lat) && Number.isFinite(r.lon)) {
          out.push({
            lat: r.lat,
            lon: r.lon,
            label: r.name,
            kind: r.kind,
          });
        }
      }
    }
    if (Number.isFinite(body.lat) && Number.isFinite(body.lon)) {
      // /v1/reverse echoes the query point at the top level.
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

  function renderFixture(fx) {
    activeQuery = fx.label;
    document.querySelectorAll("#query-list button").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.label === fx.label);
    });

    titleEl.textContent = fx.title;
    urlEl.textContent = `GET ${fx.url}`;
    jsonEl.innerHTML = syntaxHighlight(fx.body);

    clearMarkers();
    const points = pointsFromBody(fx.body);
    if (!points.length) {
      return;
    }
    const bounds = L.latLngBounds([]);
    points.forEach((p, i) => {
      const isProbe = p.probe === true;
      const marker = L.circleMarker([p.lat, p.lon], {
        radius: isProbe ? 9 : 7,
        color: isProbe ? "#f59e0b" : "#84cc16",
        weight: 2,
        fillColor: isProbe ? "#f59e0b" : "#65a30d",
        fillOpacity: 0.85,
      })
        .addTo(map)
        .bindPopup(
          `<b>${escapeHtml(p.label || "?")}</b><br>${escapeHtml(p.kind || "")}<br>${p.lat.toFixed(4)}, ${p.lon.toFixed(4)}`,
        );
      activeMarkers.push(marker);
      bounds.extend([p.lat, p.lon]);
    });
    if (bounds.isValid()) {
      map.fitBounds(bounds, { padding: [40, 40], maxZoom: 14 });
    }
  }

  function buildQueryList(fixtures) {
    listEl.innerHTML = "";
    for (const fx of fixtures) {
      const li = document.createElement("li");
      const btn = document.createElement("button");
      btn.dataset.label = fx.label;
      btn.textContent = fx.title;
      btn.addEventListener("click", () => renderFixture(fx));
      li.appendChild(btn);
      listEl.appendChild(li);
    }
  }

  fetch("./fixtures.json", { cache: "no-store" })
    .then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    })
    .then((fixtures) => {
      buildQueryList(fixtures);
      if (fixtures.length) {
        renderFixture(fixtures[0]);
      }
    })
    .catch((err) => {
      titleEl.textContent = "Failed to load fixtures";
      urlEl.textContent = String(err);
    });
})();
