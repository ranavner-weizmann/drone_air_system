#!/usr/bin/env python3
"""
Live spectrometer viewer.

Tails the latest spectro_full_*.csv under output/ and serves a live
wavelength-vs-intensity plot in the browser (no GUI/display needed).

Usage:
    python3 spectro_live.py [--dir output] [--port 8050]

Then open http://<host>:8050 (VSCode forwards the port automatically).
"""

import argparse
import json
import os
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

BASE_DIR = Path(__file__).resolve().parent
TAIL_BYTES = 128 * 1024  # enough for a few full rows (~12KB each)


def find_latest_csv(output_dir):
    """Newest spectro_full CSV across all run directories, or None."""
    runs = sorted((d for d in output_dir.iterdir() if d.is_dir()), reverse=True)
    for run in runs:
        files = sorted((run / 'csv').glob('spectro_full_*.csv'), reverse=True)
        if files:
            return files[0]
    return None


class SpectroTailer:
    """Reads the last complete row of the current spectro_full CSV."""

    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.path = None
        self.wavelengths = []

    def _load_header(self, path):
        with open(path) as f:
            header = f.readline().strip().split(',')
        self.wavelengths = [float(w) for w in header[1:]]
        self.path = path

    def read_latest(self):
        path = find_latest_csv(self.output_dir)
        if path is None:
            return {'error': 'no spectro_full CSV found under %s' % self.output_dir}
        if path != self.path:
            self._load_header(path)

        ncols = len(self.wavelengths) + 1
        with open(path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - TAIL_BYTES))
            lines = f.read().decode(errors='replace').split('\n')

        # Walk back past the header and any partially written last line
        for line in reversed(lines):
            fields = line.split(',')
            if len(fields) == ncols and fields[0] != 'Timestamp':
                try:
                    intensities = [float(v) for v in fields[1:]]
                except ValueError:
                    continue
                age = None
                try:
                    ts = datetime.fromisoformat(fields[0])
                    age = round(time.time() - ts.timestamp(), 1)
                except ValueError:
                    pass
                return {
                    'file': path.name,
                    'timestamp': fields[0],
                    'age_s': age,
                    'intensities': intensities,
                }
        return {'error': 'no complete data rows in %s yet' % path.name}


class Handler(BaseHTTPRequestHandler):
    tailer = None  # set in main()

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == '/':
            self._send(200, PAGE.encode(), 'text/html; charset=utf-8')
        elif url.path == '/data':
            data = self.tailer.read_latest()
            # Wavelengths only when the client doesn't have this file's yet
            known = parse_qs(url.query).get('known', [''])[0]
            if 'error' not in data and known != data['file']:
                data['wavelengths'] = self.tailer.wavelengths
            self._send(200, json.dumps(data).encode(), 'application/json')
        else:
            self._send(404, b'not found', 'text/plain')

    def log_message(self, fmt, *args):
        pass  # keep the console quiet


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Spectrometer — live</title>
<style>
  :root {
    color-scheme: light;
    --surface: #fcfcfb; --panel: #f2f1ef;
    --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #7a7873;
    --grid: rgba(0,0,0,.08); --axis: rgba(0,0,0,.28);
    --series-1: #2a78d6; --stale: #e34948;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      color-scheme: dark;
      --surface: #1a1a19; --panel: #242423;
      --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #8b897f;
      --grid: rgba(255,255,255,.09); --axis: rgba(255,255,255,.3);
      --series-1: #3987e5; --stale: #e66767;
    }
  }
  * { box-sizing: border-box; margin: 0; }
  body {
    background: var(--surface); color: var(--text-primary);
    font: 14px/1.45 system-ui, sans-serif;
    padding: 20px; max-width: 1200px; margin: 0 auto;
  }
  header { display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap; }
  h1 { font-size: 17px; font-weight: 600; }
  #meta { color: var(--text-muted); font-size: 12.5px; }
  #meta .stale { color: var(--stale); font-weight: 600; }
  .controls { display: flex; gap: 16px; align-items: center; margin: 10px 0 6px;
              color: var(--text-secondary); font-size: 13px; }
  .controls label { display: flex; gap: 6px; align-items: center; cursor: pointer; }
  .controls button {
    background: var(--panel); color: var(--text-primary);
    border: 1px solid var(--grid); border-radius: 6px;
    padding: 4px 14px; font: inherit; cursor: pointer;
  }
  .stats { display: flex; gap: 24px; margin: 6px 0 10px; flex-wrap: wrap; }
  .stat .v { font-size: 20px; font-weight: 600; font-variant-numeric: tabular-nums; }
  .stat .k { font-size: 11.5px; color: var(--text-muted); text-transform: uppercase;
             letter-spacing: .04em; }
  #wrap { position: relative; }
  canvas { width: 100%; height: 460px; display: block; }
  #tip {
    position: absolute; pointer-events: none; display: none;
    background: var(--panel); border: 1px solid var(--grid); border-radius: 6px;
    padding: 5px 9px; font-size: 12.5px; white-space: nowrap;
    font-variant-numeric: tabular-nums; box-shadow: 0 2px 8px rgba(0,0,0,.18);
  }
  #tip b { color: var(--text-primary); }
  #tip span { color: var(--text-secondary); }
</style>
</head>
<body>
<header>
  <h1>Spectrometer — live spectrum</h1>
  <div id="meta">connecting…</div>
</header>
<div class="controls">
  <button id="pause">Pause</button>
  <label><input type="checkbox" id="autoy" checked> Auto-scale Y</label>
</div>
<div class="stats">
  <div class="stat"><div class="v" id="peakI">–</div><div class="k">Peak intensity</div></div>
  <div class="stat"><div class="v" id="peakW">–</div><div class="k">Peak wavelength</div></div>
  <div class="stat"><div class="v" id="frameT">–</div><div class="k">Frame time</div></div>
</div>
<div id="wrap">
  <canvas id="c"></canvas>
  <div id="tip"></div>
</div>

<script>
const POLL_MS = 500;
const cv = document.getElementById('c'), tip = document.getElementById('tip');
const ctx = cv.getContext('2d');
let wl = [], inten = [], file = '', paused = false, hoverX = null;
let yMin = 0, yMax = 1;
const PAD = { l: 64, r: 16, t: 12, b: 40 };

const css = n => getComputedStyle(document.body).getPropertyValue(n).trim();

document.getElementById('pause').onclick = e => {
  paused = !paused;
  e.target.textContent = paused ? 'Resume' : 'Pause';
};

async function poll() {
  if (!paused) {
    try {
      const r = await fetch('/data?known=' + encodeURIComponent(file));
      const d = await r.json();
      if (d.error) {
        document.getElementById('meta').textContent = d.error;
      } else {
        if (d.wavelengths) { wl = d.wavelengths; file = d.file; }
        inten = d.intensities;
        updateHeader(d);
        draw();
      }
    } catch (e) {
      document.getElementById('meta').textContent = 'server unreachable';
    }
  }
  setTimeout(poll, POLL_MS);
}

function updateHeader(d) {
  const stale = d.age_s !== null && d.age_s > 5;
  document.getElementById('meta').innerHTML =
    d.file + ' &middot; ' + (stale
      ? '<span class="stale">stalled — last row ' + d.age_s + 's ago</span>'
      : 'live, row age ' + (d.age_s === null ? '?' : d.age_s + 's'));
  let pi = -Infinity, pw = 0;
  for (let i = 0; i < inten.length; i++)
    if (inten[i] > pi) { pi = inten[i]; pw = wl[i]; }
  document.getElementById('peakI').textContent = pi.toLocaleString();
  document.getElementById('peakW').textContent = pw.toFixed(1) + ' nm';
  document.getElementById('frameT').textContent = d.timestamp.slice(11, 19);
}

function niceTicks(lo, hi, n) {
  const span = hi - lo || 1, raw = span / n,
        mag = Math.pow(10, Math.floor(Math.log10(raw))),
        step = [1, 2, 5, 10].map(m => m * mag).find(s => s >= raw);
  const t = [];
  for (let v = Math.ceil(lo / step) * step; v <= hi + 1e-9; v += step) t.push(v);
  return t;
}

function draw() {
  if (!wl.length || !inten.length) return;
  const dpr = window.devicePixelRatio || 1,
        W = cv.clientWidth, H = cv.clientHeight;
  if (cv.width !== W * dpr) { cv.width = W * dpr; cv.height = H * dpr; }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);

  const xLo = wl[0], xHi = wl[wl.length - 1];
  if (document.getElementById('autoy').checked) {
    let lo = Infinity, hi = -Infinity;
    for (const v of inten) { if (v < lo) lo = v; if (v > hi) hi = v; }
    const pad = (hi - lo || 1) * 0.06;
    yMin = lo - pad; yMax = hi + pad;
  }
  const X = w => PAD.l + (w - xLo) / (xHi - xLo) * (W - PAD.l - PAD.r);
  const Y = v => H - PAD.b - (v - yMin) / (yMax - yMin) * (H - PAD.t - PAD.b);

  // grid + axes (recessive)
  ctx.font = '11px system-ui, sans-serif';
  ctx.strokeStyle = css('--grid'); ctx.fillStyle = css('--text-secondary');
  ctx.lineWidth = 1;
  ctx.textAlign = 'right'; ctx.textBaseline = 'middle';
  for (const v of niceTicks(yMin, yMax, 6)) {
    ctx.beginPath(); ctx.moveTo(PAD.l, Y(v)); ctx.lineTo(W - PAD.r, Y(v)); ctx.stroke();
    ctx.fillText(v.toLocaleString(), PAD.l - 8, Y(v));
  }
  ctx.textAlign = 'center'; ctx.textBaseline = 'top';
  for (const w of niceTicks(xLo, xHi, 10))
    ctx.fillText(w.toFixed(0), X(w), H - PAD.b + 8);
  ctx.strokeStyle = css('--axis');
  ctx.beginPath(); ctx.moveTo(PAD.l, PAD.t); ctx.lineTo(PAD.l, H - PAD.b);
  ctx.lineTo(W - PAD.r, H - PAD.b); ctx.stroke();
  ctx.fillStyle = css('--text-muted');
  ctx.fillText('wavelength (nm)', PAD.l + (W - PAD.l - PAD.r) / 2, H - 16);
  ctx.save();
  ctx.translate(14, PAD.t + (H - PAD.t - PAD.b) / 2); ctx.rotate(-Math.PI / 2);
  ctx.fillText('intensity (counts)', 0, 0); ctx.restore();

  // spectrum
  ctx.strokeStyle = css('--series-1'); ctx.lineWidth = 2;
  ctx.lineJoin = 'round';
  ctx.beginPath();
  for (let i = 0; i < wl.length; i++) {
    const x = X(wl[i]), y = Math.max(PAD.t, Math.min(H - PAD.b, Y(inten[i])));
    i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
  }
  ctx.stroke();

  // crosshair + nearest-point marker
  if (hoverX !== null && hoverX >= PAD.l && hoverX <= W - PAD.r) {
    const frac = (hoverX - PAD.l) / (W - PAD.l - PAD.r),
          i = Math.max(0, Math.min(wl.length - 1,
              Math.round(frac * (wl.length - 1))));
    ctx.strokeStyle = css('--axis');
    ctx.setLineDash([3, 3]);
    ctx.beginPath(); ctx.moveTo(X(wl[i]), PAD.t); ctx.lineTo(X(wl[i]), H - PAD.b);
    ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle = css('--series-1');
    ctx.strokeStyle = css('--surface'); ctx.lineWidth = 2;
    ctx.beginPath(); ctx.arc(X(wl[i]), Y(inten[i]), 4.5, 0, 7); ctx.fill(); ctx.stroke();
    tip.style.display = 'block';
    tip.innerHTML = '<b>' + inten[i].toLocaleString() + '</b> counts<br>' +
                    '<span>' + wl[i].toFixed(2) + ' nm</span>';
    const tx = Math.min(X(wl[i]) + 14, cv.clientWidth - tip.offsetWidth - 4);
    tip.style.left = tx + 'px';
    tip.style.top = Math.max(0, Y(inten[i]) - tip.offsetHeight - 12) + 'px';
  } else {
    tip.style.display = 'none';
  }
}

cv.addEventListener('mousemove', e => {
  hoverX = e.offsetX; draw();
});
cv.addEventListener('mouseleave', () => { hoverX = null; draw(); });
window.addEventListener('resize', draw);
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', draw);

poll();
</script>
</body>
</html>
"""


def main():
    ap = argparse.ArgumentParser(description='Live spectrometer spectrum viewer')
    ap.add_argument('--dir', default=str(BASE_DIR / 'output'),
                    help='output directory containing run folders (default: %(default)s)')
    ap.add_argument('--port', type=int, default=8050)
    args = ap.parse_args()

    Handler.tailer = SpectroTailer(args.dir)
    srv = ThreadingHTTPServer(('0.0.0.0', args.port), Handler)
    print('Watching %s' % args.dir)
    print('Open http://localhost:%d  (or http://<pi-ip>:%d)' % (args.port, args.port))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
