import type { LatencyHistoryPoint } from "../types";

/** Hand-rolled SVG sparkline -- no charting library needed for one line of points. */
export function LatencyChart({ points, service }: { points: LatencyHistoryPoint[]; service: string }) {
  const width = 480;
  const height = 120;
  const padding = 8;

  const values = points.map((p) => p.median_latency_ms ?? 0);
  const max = Math.max(...values, 1);

  const stepX = points.length > 1 ? (width - padding * 2) / (points.length - 1) : 0;
  const coords = points.map((p, i) => {
    const x = padding + i * stepX;
    const v = p.median_latency_ms ?? 0;
    const y = height - padding - (v / max) * (height - padding * 2);
    return { x, y, point: p };
  });

  const pathD = coords.map((c, i) => `${i === 0 ? "M" : "L"}${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(" ");

  return (
    <div className="latency-chart">
      <div className="latency-chart-title">
        {service} -- median latency, last {points.length * 10}s
      </div>
      <svg width={width} height={height} role="img" aria-label={`${service} latency history`}>
        <path d={pathD} fill="none" stroke="currentColor" strokeWidth={2} />
        {coords.map((c, i) =>
          c.point.sample_count > 0 ? <circle key={i} cx={c.x} cy={c.y} r={2.5} fill="currentColor" /> : null,
        )}
      </svg>
      <div className="latency-chart-scale">0 -- {max.toFixed(0)}ms</div>
    </div>
  );
}
