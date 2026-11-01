/** Round tick values covering [lo, hi] at a 1/2/2.5/5 x 10^n step, about `count` intervals. */
export function niceTicks(lo: number, hi: number, count: number): number[] {
  const raw = (hi - lo) / count;
  if (!Number.isFinite(raw) || raw <= 0) return [0, 1];
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw) ?? 10 * mag;
  const start = Math.floor(lo / step) * step;
  const end = Math.ceil(hi / step) * step;
  const out: number[] = [];
  for (let v = start; v <= end + 1e-9; v += step) out.push(Number(v.toFixed(6)));
  return out;
}

/** Index of the point nearest (x, y) within `radius`, or null. */
export function nearestPoint(points: readonly { cx: number; cy: number }[], x: number, y: number, radius: number): number | null {
  let best: number | null = null;
  let bestDistance = radius * radius;
  points.forEach((point, index) => {
    const distance = (point.cx - x) ** 2 + (point.cy - y) ** 2;
    if (distance <= bestDistance) {
      best = index;
      bestDistance = distance;
    }
  });
  return best;
}
