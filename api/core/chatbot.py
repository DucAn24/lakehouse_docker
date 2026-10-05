import logging
import os
import traceback
from pathlib import Path
from typing import Dict, Any, List

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

from config import get_available_sql_files
from core.sql_query import execute_sql_with_filters
from core.database import execute_query, execute_raw_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

MIMO_API_KEY = os.getenv("MIMO_API_KEY")
MIMO_BASE_URL = os.getenv("MIMO_BASE_URL", "https://api.xiaomimimo.com/v1")
MIMO_MODEL = os.getenv("MIMO_MODEL", "MiMo-V2.5-Pro")

logger.info(f"MIMO_API_KEY loaded: {'Yes' if MIMO_API_KEY else 'No'}")

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@tool
def list_tables() -> Dict[str, Any]:
    """List all tables available in the gold layer of the Data Lakehouse (delta.gold schema)."""
    try:
        result = execute_query("SHOW TABLES FROM delta.gold")
        tables = [row[list(row.keys())[0]] for row in result["data"]]
        return {"tables": tables, "schema": "delta.gold"}
    except Exception as e:
        return {"error": str(e)}


@tool
def describe_table(table_name: str) -> Dict[str, Any]:
    """Get column names and data types for a table in delta.gold.

    Args:
        table_name: Table name without schema prefix, e.g. 'fact_orders' or 'dim_customer'
    """
    try:
        result = execute_query(f"DESCRIBE delta.gold.{table_name}")
        columns = [
            {"column": row.get("Column", row.get("column", "")), "type": row.get("Type", row.get("type", ""))}
            for row in result["data"]
        ]
        return {"table": f"delta.gold.{table_name}", "columns": columns}
    except Exception as e:
        return {"error": str(e)}


@tool
def run_select_query(sql: str, limit: int = 50) -> Dict[str, Any]:
    """Execute a custom SELECT query against the Trino Data Lakehouse.

    Only SELECT statements are allowed. INSERT, UPDATE, DELETE, DROP, CREATE,
    ALTER, TRUNCATE, MERGE, REPLACE, GRANT, REVOKE are all blocked.
    A LIMIT clause will be added automatically if not present.
    Always use fully-qualified table names: delta.gold.<table_name>.

    Args:
        sql: A valid Trino SELECT query.
        limit: Max rows to return (default 50, max 1000).
    """
    try:
        limit = min(limit, 1000)
        result = execute_raw_query(sql, limit=limit)
        return {
            "query_executed": result["query_executed"],
            "count": result["count"],
            "columns": result["columns"],
            "data": result["data"],
        }
    except ValueError as e:
        # Safety guard triggered
        return {"error": str(e), "blocked": True}
    except Exception as e:
        return {"error": str(e)}


@tool
def get_data(api_name: str, limit: int = 50) -> Dict[str, Any]:
    """Fetch data using a pre-built SQL template by name.

    Available templates:
      Olist:        cus_cnt, prd_cnt, revenue_by_region,
                    top_products_by_category, review_analysis.
      Clickstream:  funnel_analysis, session_analytics,
                    device_performance, click_revenue_by_country.

    Args:
        api_name: Template name (without .sql extension).
        limit: Maximum records to return (default 50).
    """
    available = get_available_sql_files()
    if api_name not in available:
        return {"error": f"Template '{api_name}' not found. Available: {available}"}

    from core.sql_query import load_sql_file

    try:
        base_sql = load_sql_file(api_name)
        if "LIMIT" not in base_sql.upper():
            base_sql += f" LIMIT {limit}"
        query_executed = base_sql
    except Exception:
        query_executed = f"SELECT * FROM delta.gold.{api_name} LIMIT {limit}"

    data = execute_sql_with_filters(api_name, {}, limit)
    return {"api": api_name, "query_executed": query_executed, "count": len(data), "data": data}


# ---------------------------------------------------------------------------
# System prompt — full star schema knowledge baked in
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a friendly and professional Data Analyst assistant for a Data Lakehouse built on Apache Trino + Delta Lake.
Your name is **LakeBot**. You specialize in answering questions about data, analytics, and business insights using the lakehouse.

════════════════════════════════════════════════════
YOUR PERSONALITY & BEHAVIOR
════════════════════════════════════════════════════

• You are polite, helpful, and conversational — like a knowledgeable colleague.
• You greet users warmly when they say hello or start a conversation.
• You always respond with empathy and professionalism.
• You NEVER expose raw error messages, stack traces, or technical jargon to users.

════════════════════════════════════════════════════
TOPIC GUARDRAILS — VERY IMPORTANT
════════════════════════════════════════════════════

You ONLY answer questions related to:
  ✅ Data in the lakehouse (tables, columns, records, schemas)
  ✅ Business analytics & insights (revenue, customers, products, reviews, clickstream, sessions, funnels)
  ✅ SQL queries against the lakehouse
  ✅ Explaining data concepts related to the business
  ✅ How to use the data/API/chatbot features

You MUST politely decline questions about:
  ❌ General knowledge (history, science, geography, etc.)
  ❌ Personal advice (health, relationships, legal, financial advice)
  ❌ Coding help unrelated to querying the lakehouse
  ❌ Creative writing, jokes, stories
  ❌ Current events, news, politics
  ❌ Any topic not related to this data lakehouse or business analytics

When declining, respond warmly. Example:
  "Great question, but that's a bit outside my area of expertise! 😊
   I'm specialized in analyzing the data in our lakehouse — things like
   revenue trends, customer insights, product analytics, and clickstream data.
   Feel free to ask me anything about those topics!"

════════════════════════════════════════════════════
HANDLING DIFFICULT OR UNANSWERABLE QUESTIONS
════════════════════════════════════════════════════

If you cannot find the data or the question is too complex to answer:
  • NEVER say "error" or "I failed" or show technical error messages.
  • Instead, respond helpfully. Examples:
    - "I wasn't able to find that specific data in our lakehouse. The tables I have access to cover [brief summary]. Could you rephrase or try a related question?"
    - "That's a great question! Unfortunately, our current data doesn't seem to cover that angle. Here's what I *can* tell you about [related topic]..."
    - "Hmm, I looked but couldn't find a direct answer. Let me suggest an alternative: [suggestion]"

If a query returns empty results:
  • Say something like: "The query ran successfully, but returned no results for those filters. This might mean [possible reason]. Would you like to try with different criteria?"

════════════════════════════════════════════════════
GALAXY SCHEMA (Constellation) — delta.gold layer
════════════════════════════════════════════════════
Two star schemas sharing dim_date as a conformed dimension.

┌─────────────────────────────────────────────────┐
│  STAR 1 — Olist E-Commerce (Brazil)             │
└─────────────────────────────────────────────────┘

FACT TABLES
───────────
• fact_orders   — one row per order item
    order_id, customer_sk, product_sk, date_sk,
    total_item_value, freight_value, payment_value,
    order_status, seller_sk

• fact_reviews   — product reviews linked to orders
    review_id, order_id, product_sk, date_sk,
    review_score, review_comment_title, review_comment_message

DIMENSION TABLES
────────────────
• dim_customer  — Olist customer master
    customer_sk (PK), customer_id,
    customer_city, customer_state, customer_region

• dim_product   — Olist product master
    product_sk (PK), product_id,
    product_category_name, product_category_name_english,
    product_weight_g, product_length_cm

JOIN KEYS (Olist)
─────────────────
  fact_orders.customer_sk  → dim_customer.customer_sk
  fact_orders.product_sk   → dim_product.product_sk
  fact_orders.date_sk      → dim_date.date_sk
  fact_reviews.product_sk  → dim_product.product_sk
  fact_reviews.date_sk     → dim_date.date_sk

┌─────────────────────────────────────────────────┐
│  STAR 2 — Clickstream Analytics (Global)        │
└─────────────────────────────────────────────────┘

FACT TABLES
───────────
• fact_click_orders — one row per clickstream order
    order_sk (PK), order_id,
    customer_sk (FK→dim_click_customer), date_sk (FK→dim_date),
    device_sk (FK→dim_device), source_sk (FK→dim_traffic_source),
    payment_method, discount_pct, subtotal_usd, total_usd,
    country, order_timestamp

• fact_sessions — one row per user session
    session_sk (PK), session_id,
    customer_sk (FK→dim_click_customer), date_sk (FK→dim_date),
    device_sk (FK→dim_device), source_sk (FK→dim_traffic_source),
    country, session_start_timestamp

• fact_clickstream_events — one row per user event (PAGE_VIEW, ADD_TO_CART, PURCHASE, …)
    event_sk (PK), event_id,
    session_sk (FK→fact_sessions), customer_sk (FK→dim_click_customer),
    date_sk (FK→dim_date), product_sk (FK→dim_click_product),
    event_type, quantity, payment_method, event_timestamp

• fact_click_reviews — one row per product review
    review_sk (PK), review_id,
    order_sk (FK→fact_click_orders), product_sk (FK→dim_click_product),
    date_sk (FK→dim_date),
    rating (1-5), review_text, sentiment (POSITIVE/NEUTRAL/NEGATIVE),
    review_timestamp

DIMENSION TABLES
────────────────
• dim_click_customer — clickstream customer master
    customer_sk (PK), customer_id,
    customer_name, email, country, age,
    signup_date, marketing_opt_in

• dim_click_product — clickstream product master
    product_sk (PK), product_id,
    product_category, product_name,
    price_usd, cost_usd, margin_usd

• dim_device — device type dimension
    device_sk (PK), device (MOBILE/DESKTOP/TABLET/…)

• dim_traffic_source — traffic acquisition source
    source_sk (PK), traffic_source (ORGANIC/PAID/EMAIL/…)

JOIN KEYS (Clickstream)
───────────────────────
  fact_click_orders.customer_sk    → dim_click_customer.customer_sk
  fact_click_orders.device_sk      → dim_device.device_sk
  fact_click_orders.source_sk      → dim_traffic_source.source_sk
  fact_click_orders.date_sk        → dim_date.date_sk
  fact_sessions.customer_sk        → dim_click_customer.customer_sk
  fact_sessions.device_sk          → dim_device.device_sk
  fact_sessions.source_sk          → dim_traffic_source.source_sk
  fact_sessions.date_sk            → dim_date.date_sk
  fact_clickstream_events.session_sk  → fact_sessions.session_sk
  fact_clickstream_events.customer_sk → dim_click_customer.customer_sk
  fact_clickstream_events.product_sk  → dim_click_product.product_sk
  fact_clickstream_events.date_sk     → dim_date.date_sk
  fact_click_reviews.order_sk      → fact_click_orders.order_sk
  fact_click_reviews.product_sk    → dim_click_product.product_sk
  fact_click_reviews.date_sk       → dim_date.date_sk

┌─────────────────────────────────────────────────┐
│  SHARED / CONFORMED DIMENSION                   │
└─────────────────────────────────────────────────┘
• dim_date — calendar dimension (shared by both stars)
    date_sk (PK), full_date, year, month, day,
    quarter, day_of_week, week_of_year

⚠ IMPORTANT: Olist and Clickstream use SEPARATE customer/product
  dimensions. Do NOT join dim_customer with clickstream facts or
  dim_click_customer with Olist facts.

════════════════════════════════════════════
PRE-BUILT TEMPLATES (shortcuts)
════════════════════════════════════════════

Olist:
  cus_cnt                   — total Olist customer count
  prd_cnt                   — distinct Olist product count
  revenue_by_region         — Olist revenue grouped by region/state
  top_products_by_category  — top Olist categories by sales
  review_analysis           — Olist sentiment breakdown by category

Clickstream:
  funnel_analysis           — conversion funnel (PAGE_VIEW→ADD_TO_CART→PURCHASE)
  session_analytics         — sessions & actions by device × traffic source
  device_performance        — sales performance by device type
  click_revenue_by_country  — clickstream revenue by country

════════════════════════════════════════════
RULES YOU MUST FOLLOW (CRITICAL & STRICT)
════════════════════════════════════════════
1. ONLY generate SELECT queries. Never generate INSERT, UPDATE, DELETE,
   DROP, CREATE, ALTER, TRUNCATE, MERGE, REPLACE, GRANT, or REVOKE.
   If the user asks for a write operation, politely refuse and explain why.

2. Always use fully-qualified table names: delta.gold.<table_name>

3. When unsure about columns, call describe_table first.

4. Default LIMIT is 50. Apply LIMIT unless the user explicitly asks for all rows.

5. MANDATORY QUERY INSPECTOR REQUIREMENT:
   Whenever you run a query using `run_select_query` or load a pre-built template using `get_data`, you MUST include the exact executed SQL query in a ```sql markdown code block in your response.
   Do NOT omit it! The frontend UI detects this ```sql block to render an interactive clickable accordion dropdown (query inspector).
   Example format:
   ---
   ### SQL Query Executed
   ```sql
   SELECT ...
   ```
   ### Results
   ...
   ---

6. Provide brief, meaningful insights after returning the data.

7. Language Preference: Always respond in the language the user asked in. If the user asks in Vietnamese ("năm có doanh thu cao nhất"), reply in Vietnamese! If the user asks in English, reply in English! Keep technical SQL code blocks in SQL.
"""


# ---------------------------------------------------------------------------
# Chatbot class
# ---------------------------------------------------------------------------


class DataChatbot:
    def __init__(self):
        if not MIMO_API_KEY:
            raise ValueError("MIMO_API_KEY is not set. Please configure it in .env file.")

        self.llm = ChatOpenAI(
            model=MIMO_MODEL,
            api_key=MIMO_API_KEY,
            base_url=MIMO_BASE_URL,
            temperature=0.3,
            timeout=60,
            max_retries=2,
            extra_body={"thinking": {"type": "disabled"}},
        )

        self.tools = [list_tables, describe_table, run_select_query, get_data]

        self.agent = create_react_agent(self.llm, self.tools)

        self.chat_histories: Dict[str, List] = {}
        logger.info("DataChatbot initialized successfully")

    @staticmethod
    def _conversation_key(conversation_id: str) -> str:
        conversation_id = str(conversation_id or "default").strip()
        if not conversation_id:
            return "default"
        if len(conversation_id) > 128:
            raise ValueError("Conversation ID is too long.")
        return conversation_id

    def chat(self, user_message: str, conversation_id: str = "default") -> str:
        try:
            conversation_key = self._conversation_key(conversation_id)
            logger.info(f"Received message: {user_message[:100]}...")

            messages = [SystemMessage(content=SYSTEM_PROMPT)]
            messages.extend(self.chat_histories.get(conversation_key, []))
            messages.append(HumanMessage(content=user_message))

            logger.info("Invoking agent...")
            response = self.agent.invoke({"messages": messages}, {"recursion_limit": 15})
            logger.info("Agent response received")

            ai_messages = [m for m in response["messages"] if isinstance(m, AIMessage)]
            if ai_messages:
                last_response = ai_messages[-1].content

                history = self.chat_histories.setdefault(conversation_key, [])
                history.extend((HumanMessage(content=user_message), AIMessage(content=last_response)))
                self.chat_histories[conversation_key] = history[-20:]

                logger.info(f"Response: {str(last_response)[:100]}...")
                return last_response

            logger.warning("No AI message in response")
            return (
                "Hmm, I wasn't able to generate a response for that. "
                "Could you try rephrasing your question? I'm here to help "
                "with anything about our lakehouse data! 😊"
            )

        except Exception as e:
            logger.error(f"Error in chat: {str(e)}")
            logger.error(traceback.format_exc())
            return self._friendly_error(e)

    @staticmethod
    def _friendly_error(e: Exception) -> str:
        """Convert technical exceptions into polite, user-facing messages."""
        error_str = str(e).lower()

        # API rate-limit errors (OpenAI-compatible providers)
        if any(kw in error_str for kw in ("rate limit", "rate_limit", "429", "too many requests", "quota")):
            return (
                "It looks like I'm getting a lot of requests right now and hit a temporary limit. 🕐 "
                "Please wait a moment and try again — I'll be ready shortly!"
            )

        # LangGraph recursion / iteration limit
        if any(kw in error_str for kw in ("recursion limit", "recursion_limit", "iteration limit")):
            return (
                "That's a really complex question! I tried multiple approaches but "
                "couldn't fully resolve it within my processing limits. 🤔\n\n"
                "Could you try breaking it into smaller questions? For example:\n"
                "• Ask about one metric at a time\n"
                "• Specify a particular table or time range\n\n"
                "I'd love to help you get the answer step by step!"
            )

        # Database / connection issues
        if any(kw in error_str for kw in ("connection", "timeout", "timed out", "trino", "database", "unreachable")):
            return (
                "I'm having a bit of trouble connecting to the data warehouse right now. 🔧 "
                "This is usually temporary — please try again in a few seconds. "
                "If it keeps happening, the data services might be restarting."
            )

        # Authentication / API key issues
        if any(kw in error_str for kw in ("auth", "api key", "api_key", "unauthorized", "401", "403")):
            return (
                "I'm experiencing a configuration issue on my end. 🔑 "
                "Please let the administrator know so they can check the setup. "
                "In the meantime, you can still browse the data API endpoints directly!"
            )

        # Fallback — never expose raw error text
        return (
            "I'm sorry, I ran into an unexpected issue while processing your request. 😔 "
            "Please try again in a moment. If the problem persists, try rephrasing "
            "your question or asking something simpler — I'm here to help!"
        )

    def reset(self, conversation_id: str = "default"):
        self.chat_histories.pop(self._conversation_key(conversation_id), None)

    def get_history(self, conversation_id: str = "default") -> List[Dict]:
        history = []
        for msg in self.chat_histories.get(self._conversation_key(conversation_id), []):
            if isinstance(msg, HumanMessage):
                history.append({"role": "user", "content": msg.content})
            elif isinstance(msg, AIMessage) and msg.content:
                history.append({"role": "assistant", "content": msg.content})
        return history


# Singleton
_chatbot_instance = None


def get_chatbot() -> DataChatbot:
    global _chatbot_instance
    if _chatbot_instance is None:
        _chatbot_instance = DataChatbot()
    return _chatbot_instance
