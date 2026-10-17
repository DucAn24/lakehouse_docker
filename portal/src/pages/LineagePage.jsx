import { useState, useEffect } from 'react'
import { useSearchParams, useNavigate } from 'react-router-dom'
import { catalogAPI } from '../api'
import LineageGraph from '../components/LineageGraph'

const LAYER_COLORS = {
  source: { bg: '#7c3aed', label: 'Source (Postgres)' },
  bronze: { bg: '#cd7f32', label: 'Bronze' },
  silver: { bg: '#94a3b8', label: 'Silver' },
  gold:   { bg: '#f59e0b', label: 'Gold' },
}

export default function LineagePage() {
  const [graph, setGraph] = useState(null)
  const [loading, setLoading] = useState(true)
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const focusTable = searchParams.get('focus')

  useEffect(() => {
    setLoading(true)
    const fetcher = focusTable
      ? catalogAPI.tableLineage(focusTable)
      : catalogAPI.lineage()

    fetcher
      .then(data => {
        // Mark focused/upstream/downstream nodes
        if (focusTable && data.focus) {
          data.nodes = data.nodes.map(n => ({
            ...n,
            highlight: data.focus?.includes(n.id),
          }))
        }
        setGraph(data)
      })
      .catch(err => console.error(err))
      .finally(() => setLoading(false))
  }, [focusTable])

  const handleNodeClick = (nodeData) => {
    if (nodeData.table && nodeData.layer !== 'source') {
      navigate(`/catalog/${nodeData.layer}/${nodeData.table}`)
    }
  }

  return (
    <div>
      <div className="page-header">
        <h1><span className="gradient-text">Data Lineage</span></h1>
        <p>
          {focusTable
            ? <>Lineage for <strong>{focusTable}</strong> — <a href="/lineage" style={{ cursor: 'pointer' }} onClick={e => { e.preventDefault(); navigate('/lineage') }}>View all</a></>
            : 'Visualize how data flows from sources through bronze, silver, and gold layers'
          }
        </p>
      </div>

      {/* Legend */}
      <div className="lineage-legend">
        {Object.entries(LAYER_COLORS).map(([key, { bg, label }]) => (
          <div className="legend-item" key={key}>
            <div className="legend-dot" style={{ background: bg }} />
            <span>{label}</span>
          </div>
        ))}
      </div>

      {/* Graph */}
      {loading ? (
        <div className="loading-container">
          <div className="spinner" />
          <span>Building lineage graph…</span>
        </div>
      ) : (
        <div className="lineage-container">
          <LineageGraph graph={graph} onNodeClick={handleNodeClick} />
        </div>
      )}
    </div>
  )
}
