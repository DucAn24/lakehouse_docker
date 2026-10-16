import { useState, useRef, useEffect, useCallback } from 'react'
import { chatAPI } from '../api'
import QueryResultBlock from '../components/QueryResultBlock'

const DEFAULT_SUGGESTIONS = [
  "What's the total revenue?",
  "Show me the top 5 product categories by sales",
  "How many customers do we have?",
  "Show conversion funnel analysis",
  "Revenue breakdown by device type",
  "What's the average review score?",
]

function parseMarkdown(text) {
  // Simple markdown: **bold**, *italic*, `code`, newlines
  // Escape first: the result is injected as HTML and the text may echo query rows / user input
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`(.+?)`/g, '<code>$1</code>')
    .replace(/\n/g, '<br/>')
}

export default function ChatPage() {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [suggestions, setSuggestions] = useState(DEFAULT_SUGGESTIONS)
  const [conversationId] = useState(() => `portal-${Date.now()}`)
  const messagesEndRef = useRef(null)
  const textareaRef = useRef(null)

  useEffect(() => {
    chatAPI.suggestions()
      .then(res => {
        if (res?.suggestions?.length) {
          setSuggestions(res.suggestions)
        }
      })
      .catch(() => {})
  }, [])

  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [])

  useEffect(() => {
    scrollToBottom()
  }, [messages, loading, scrollToBottom])

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight, 120) + 'px'
    }
  }, [input])

  const sendMessage = async (text) => {
    const msg = text || input.trim()
    if (!msg || loading) return

    setInput('')
    setMessages(prev => [...prev, { role: 'user', content: msg }])
    setLoading(true)

    try {
      const res = await chatAPI.send(msg, conversationId)
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: res.response,
        queries: res.queries || [],
      }])
    } catch (err) {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: "Sorry, I'm having trouble connecting right now. Please try again in a moment. 😔",
      }])
    } finally {
      setLoading(false)
    }
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  const resetChat = () => {
    chatAPI.reset(conversationId).catch(() => {})
    setMessages([])
  }

  const isEmpty = messages.length === 0

  return (
    <div className="chat-container">
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <div className="page-header" style={{ marginBottom: 0 }}>
          <h1><span className="gradient-text">LakeBot</span></h1>
          <p>AI-powered analytics assistant for your lakehouse</p>
        </div>
        {messages.length > 0 && (
          <button className="btn" onClick={resetChat}>
            ✦ New Chat
          </button>
        )}
      </div>

      {/* Messages */}
      <div className="chat-messages">
        {isEmpty && (
          <div className="chat-welcome">
            <div style={{ fontSize: '2.5rem' }}>🤖</div>
            <h2>Hello! I'm <span className="gradient-text">LakeBot</span></h2>
            <p>
              I can help you explore your data lakehouse — ask me about revenue,
              customers, products, clickstream analytics, and more. I'll write
              the SQL and visualize the results for you.
            </p>
            <div className="chat-suggestions">
              {suggestions.map((s, i) => (
                <button
                  key={i}
                  className="suggestion-chip"
                  onClick={() => sendMessage(s)}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={`chat-msg ${msg.role}`}>
            <div className="chat-msg-avatar">
              {msg.role === 'user' ? '👤' : '🤖'}
            </div>
            <div className="chat-msg-body">
              <div dangerouslySetInnerHTML={{ __html: parseMarkdown(msg.content || '') }} />
              {msg.queries?.map((q, j) => (
                <QueryResultBlock key={j} result={q} />
              ))}
            </div>
          </div>
        ))}

        {loading && (
          <div className="chat-msg assistant">
            <div className="chat-msg-avatar">🤖</div>
            <div className="chat-msg-body">
              <div className="typing-indicator">
                <span /><span /><span />
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="chat-input-wrapper">
        <div className="chat-input">
          <textarea
            ref={textareaRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about your data…"
            rows={1}
            disabled={loading}
          />
          <button
            className="chat-send-btn"
            onClick={() => sendMessage()}
            disabled={!input.trim() || loading}
            title="Send message"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="22" y1="2" x2="11" y2="13" />
              <polygon points="22 2 15 22 11 13 2 9 22 2" />
            </svg>
          </button>
        </div>
      </div>
    </div>
  )
}
