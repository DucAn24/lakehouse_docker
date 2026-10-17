export default function DQBadge({ score }) {
  if (score === null || score === undefined) {
    return <span className="dq-badge unknown">No DQ</span>
  }

  let cls = 'good'
  let icon = '✓'
  if (score < 70) { cls = 'bad'; icon = '✗' }
  else if (score < 90) { cls = 'warn'; icon = '⚠' }

  return (
    <span className={`dq-badge ${cls}`}>
      {icon} {score}%
    </span>
  )
}
