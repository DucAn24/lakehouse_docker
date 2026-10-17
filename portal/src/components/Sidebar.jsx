import { NavLink, useLocation } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { catalogAPI } from '../api'

// SVG Icons as inline components
const CatalogIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M4 19.5v-15A2.5 2.5 0 0 1 6.5 2H20v20H6.5a2.5 2.5 0 0 1 0-5H20" />
  </svg>
)

const LineageIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="18" cy="5" r="3" /><circle cx="6" cy="12" r="3" /><circle cx="18" cy="19" r="3" />
    <line x1="8.59" y1="13.51" x2="15.42" y2="17.49" />
    <line x1="15.41" y1="6.51" x2="8.59" y2="10.49" />
  </svg>
)

const ChatIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
  </svg>
)

export default function Sidebar() {
  const [stats, setStats] = useState(null)
  const location = useLocation()

  useEffect(() => {
    catalogAPI.stats().then(setStats).catch(() => {})
  }, [])

  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <h1><span className="gradient-text">Lakehouse</span> Portal</h1>
        <p>Data Catalog · Lineage · Chat</p>
      </div>

      <nav className="sidebar-nav">
        <NavLink to="/catalog" className={({ isActive }) =>
          `sidebar-link${isActive || location.pathname.startsWith('/catalog') ? ' active' : ''}`
        }>
          <CatalogIcon />
          Data Catalog
        </NavLink>
        <NavLink to="/lineage" className={({ isActive }) =>
          `sidebar-link${isActive ? ' active' : ''}`
        }>
          <LineageIcon />
          Lineage
        </NavLink>
        <NavLink to="/chat" className={({ isActive }) =>
          `sidebar-link${isActive ? ' active' : ''}`
        }>
          <ChatIcon />
          AI Chat
        </NavLink>
      </nav>

      {stats && (
        <div className="sidebar-stats">
          <div className="sidebar-stat">
            <span>Tables</span>
            <span>{stats.total_tables ?? '—'}</span>
          </div>
          {stats.layers && Object.entries(stats.layers).map(([layer, count]) => (
            <div className="sidebar-stat" key={layer}>
              <span style={{ textTransform: 'capitalize' }}>{layer}</span>
              <span>{count}</span>
            </div>
          ))}
        </div>
      )}
    </aside>
  )
}
