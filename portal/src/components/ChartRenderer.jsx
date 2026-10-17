import { useMemo } from 'react'
import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, LineElement, PointElement, ArcElement, Title, Tooltip, Legend, Filler } from 'chart.js'
import { Bar, Line, Pie } from 'react-chartjs-2'

ChartJS.register(CategoryScale, LinearScale, BarElement, LineElement, PointElement, ArcElement, Title, Tooltip, Legend, Filler)

// Curated palette that looks great on dark backgrounds
const PALETTE = [
  'rgba(0, 212, 255, 0.85)',
  'rgba(124, 58, 237, 0.85)',
  'rgba(16, 185, 129, 0.85)',
  'rgba(245, 158, 11, 0.85)',
  'rgba(239, 68, 68, 0.85)',
  'rgba(99, 102, 241, 0.85)',
  'rgba(236, 72, 153, 0.85)',
  'rgba(168, 162, 158, 0.85)',
]

const BG_PALETTE = PALETTE.map(c => c.replace('0.85', '0.2'))

const BASE_OPTIONS = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: {
      labels: { color: '#94a3b8', font: { family: 'Inter', size: 11 } },
    },
    tooltip: {
      backgroundColor: '#1a2236',
      titleColor: '#f0f4f8',
      bodyColor: '#94a3b8',
      borderColor: 'rgba(0, 212, 255, 0.2)',
      borderWidth: 1,
      cornerRadius: 8,
      bodyFont: { family: 'Inter' },
      titleFont: { family: 'Inter', weight: 600 },
    },
  },
  scales: {
    x: {
      ticks: { color: '#64748b', font: { family: 'Inter', size: 10 } },
      grid: { color: 'rgba(148, 163, 184, 0.06)' },
    },
    y: {
      ticks: { color: '#64748b', font: { family: 'Inter', size: 10 } },
      grid: { color: 'rgba(148, 163, 184, 0.06)' },
    },
  },
}

function buildDatasets(chart) {
  return (chart.datasets || []).map((ds, i) => ({
    label: ds.label,
    data: ds.data,
    backgroundColor: chart.type === 'pie' ? PALETTE.slice(0, ds.data.length) : BG_PALETTE[i % BG_PALETTE.length],
    borderColor: PALETTE[i % PALETTE.length],
    borderWidth: chart.type === 'line' ? 2 : 1,
    tension: 0.35,
    fill: chart.type === 'line',
    pointRadius: 3,
    pointBackgroundColor: PALETTE[i % PALETTE.length],
  }))
}

export default function ChartRenderer({ chart }) {
  const data = useMemo(() => ({
    labels: chart.labels || [],
    datasets: buildDatasets(chart),
  }), [chart])

  if (chart.type === 'metric') {
    return (
      <div style={{ textAlign: 'center', padding: '20px' }}>
        <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: 4 }}>{chart.label}</div>
        <div style={{ fontSize: '2rem', fontWeight: 700, background: 'var(--accent-gradient)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
          {typeof chart.value === 'number' ? chart.value.toLocaleString() : chart.value}
        </div>
      </div>
    )
  }

  const options = { ...BASE_OPTIONS }

  if (chart.type === 'pie') {
    delete options.scales
    return (
      <div style={{ height: 280 }}>
        <Pie data={data} options={options} />
      </div>
    )
  }

  if (chart.type === 'stacked_bar') {
    options.scales.x.stacked = true
    options.scales.y.stacked = true
    return (
      <div style={{ height: 280 }}>
        <Bar data={data} options={options} />
      </div>
    )
  }

  if (chart.type === 'line') {
    return (
      <div style={{ height: 280 }}>
        <Line data={data} options={options} />
      </div>
    )
  }

  // Default: bar
  return (
    <div style={{ height: 280 }}>
      <Bar data={data} options={options} />
    </div>
  )
}
