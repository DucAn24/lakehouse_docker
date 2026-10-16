import { Routes, Route, Navigate } from 'react-router-dom'
import Sidebar from './components/Sidebar'
import CatalogPage from './pages/CatalogPage'
import TablePage from './pages/TablePage'
import LineagePage from './pages/LineagePage'
import ChatPage from './pages/ChatPage'
import './App.css'

export default function App() {
  return (
    <div className="app-layout">
      <Sidebar />
      <main className="app-main">
        <Routes>
          <Route path="/" element={<Navigate to="/catalog" replace />} />
          <Route path="/catalog" element={<CatalogPage />} />
          <Route path="/catalog/:layer/:table" element={<TablePage />} />
          <Route path="/lineage" element={<LineagePage />} />
          <Route path="/chat" element={<ChatPage />} />
        </Routes>
      </main>
    </div>
  )
}
