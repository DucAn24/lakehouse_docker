import json
import logging
import os
import re
import time
import traceback
from contextvars import ContextVar
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
from langgraph.prebuilt import create_react_agent

from config import get_available_sql_files
from core.charts import to_ui_result
from core.sql_query import execute_sql_with_filters, load_sql_file
from core.database import QueryValidationError, execute_query, execute_raw_query
from core.schema_context import schema_context

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


_SQL_BLOCK = re.compile(r"```sql.*?```", re.IGNORECASE | re.DOTALL)
MAX_QUERY_FAILURES = 3
MAX_ROWS_TO_MODEL = 100

# Per-request counter of failed queries, so a model stuck on a bad query stops instead of hitting the
# recursion limit. A mutable dict: tool threads share it even when they get a copy of the context.
_query_failures: ContextVar[Optional[Dict[str, int]]] = ContextVar("query_failures", default=None)


def _to_model(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, default=str, ensure_ascii=False)


def _failure(payload: Dict[str, Any]) -> Tuple[str, None]:
    """Error result for the model; after repeated failures it is told to give up."""
    counter = _query_failures.get()
    if counter is not None:
        counter["n"] += 1
        if counter["n"] >= MAX_QUERY_FAILURES:
            payload["stop"] = (
                "Too many failed queries. Do not run another query: tell the user politely that you "
                "couldn't answer this and suggest a simpler or rephrased question."
            )
    return _to_model(payload), None


def _success(
    sql: str, columns: List[str], data: List[Dict[str, Any]], elapsed_ms: int, chart_request=None, **extra
) -> Tuple[str, dict]:
    """Rows for the model (capped) and the full structured result for the UI (the tool artifact)."""
    ui = to_ui_result(sql, columns, data, elapsed_ms, chart_request)
    payload = {
        "query_executed": sql,
        "count": len(data),
        "columns": columns,
        "data": data[:MAX_ROWS_TO_MODEL],
        "chart_drawn": ui["chart"]["type"] if ui["chart"] else None,
        **extra,
    }
    if len(data) > MAX_ROWS_TO_MODEL:
        payload["note"] = (
            f"Only the first {MAX_ROWS_TO_MODEL} of {len(data)} rows are shown to you; "
            "the user sees the full result in the UI."
        )
    return _to_model(payload), ui


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


@tool(response_format="content_and_artifact")
def run_select_query(
    sql: str,
    limit: int = 50,
    chart_type: Optional[str] = None,
    x: Optional[str] = None,
    y: Optional[List[str]] = None,
    series: Optional[str] = None,
) -> Tuple[str, Optional[dict]]:
    """Execute a custom SELECT query against the Trino Data Lakehouse.

    Only SELECT statements are allowed. INSERT, UPDATE, DELETE, DROP, CREATE,
    ALTER, TRUNCATE, MERGE, REPLACE, GRANT, REVOKE are all blocked.
    A LIMIT clause will be added automatically if not present.
    The query is validated by Trino first: on a validation error you get Trino's message and the
    columns of the tables you used, so fix the query and call this tool again.
    Always use fully-qualified table names: delta.gold.<table_name>.
    The user sees the SQL, the rows and a chart in the UI automatically (a bar or line chart is
    picked from the data). Pass chart_type only when the user asks for a specific chart, or when
    another one clearly fits better; the columns must exist in your SELECT.

    Args:
        sql: A valid Trino SELECT query.
        limit: Max rows to return (default 50, max 1000).
        chart_type: Optional. "bar", "stacked_bar", "line", "pie", or "table" (no chart).
        x: Column for the x axis / pie labels (a category or a date).
        y: Numeric column(s) to plot, e.g. ["revenue"]. Several columns = several series.
        series: Optional column that splits one numeric y into series, e.g. x="month",
            series="device", y=["revenue"]. Needs exactly one y.
    """
    try:
        start = time.time()
        result = execute_raw_query(sql, limit=min(limit, 1000))
        return _success(
            result["query_executed"],
            result["columns"],
            result["data"],
            round((time.time() - start) * 1000),
            chart_request={"chart_type": chart_type, "x": x, "y": y, "series": series},
        )
    except QueryValidationError as e:
        logger.info("Query rejected by Trino (%s): %s", e.error_name, e)
        return _failure(
            {
                "error": str(e),
                "error_type": e.error_name,
                "hint": "Fix the query and try again. These are the real columns of the tables you used.",
                "tables": schema_context.tables_used_in(sql),
            }
        )
    except ValueError as e:
        # Safety guard triggered
        return _failure({"error": str(e), "blocked": True})
    except Exception as e:
        return _failure({"error": str(e)})


@tool(response_format="content_and_artifact")
def get_data(api_name: str, limit: int = 50) -> Tuple[str, Optional[dict]]:
    """Fetch data using a pre-built SQL template by name.

    Available templates:
      Olist:        cus_cnt, prd_cnt, revenue_by_region,
                    top_products_by_category, review_analysis.
      Clickstream:  funnel_analysis, session_analytics,
                    device_performance, click_revenue_by_country.
    The user sees the SQL, the rows and a chart in the UI automatically.

    Args:
        api_name: Template name (without .sql extension).
        limit: Maximum records to return (default 50).
    """
    available = get_available_sql_files()
    if api_name not in available:
        return _failure({"error": f"Template '{api_name}' not found. Available: {available}"})

    try:
        sql = load_sql_file(api_name)
        if "LIMIT" not in sql.upper():
            sql += f" LIMIT {limit}"
        start = time.time()
        data = execute_sql_with_filters(api_name, {}, limit)
    except Exception as e:
        return _failure({"error": getattr(e, "detail", str(e))})
    columns = list(data[0].keys()) if data else []
    return _success(sql, columns, data, round((time.time() - start) * 1000), api=api_name)


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

__SCHEMA__

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

5. The UI automatically shows the executed SQL, the full result table and a chart for every
   `run_select_query` / `get_data` call. Do NOT paste the SQL or a table of the rows in your answer;
   write a short summary and the insights instead (cite the key numbers).

   Charts: a bar/line chart is picked automatically. When the user asks for a chart type
   (pie, stacked, line, "plot", "chart", "trend by …"), pass `chart_type`, `x`, `y` (and `series`
   for a breakdown) to `run_select_query`, and select the columns that chart needs: one
   category/date column, one numeric column per series (or a third column for `series`).
   Use "table" if the user only wants the table. The result's `chart_drawn` says what was drawn;
   never draw ASCII charts.

   If `run_select_query` returns an error, read it (and the table columns it includes), fix the
   query and call it again. If the result says "stop", do not query any more.

6. Provide brief, meaningful insights after returning the data.

7. Language Preference: Always respond in the language the user asked in. If the user asks in Vietnamese ("năm có doanh thu cao nhất"), reply in Vietnamese! If the user asks in English, reply in English! Keep technical SQL code blocks in SQL.
"""


def build_system_prompt() -> str:
    return SYSTEM_PROMPT.replace("__SCHEMA__", schema_context.prompt_section())


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

    def chat(self, user_message: str, conversation_id: str = "default") -> Tuple[str, List[dict]]:
        """Answer a message; returns (text, structured results of the queries that were run)."""
        token = _query_failures.set({"n": 0})
        try:
            conversation_key = self._conversation_key(conversation_id)
            logger.info(f"Received message: {user_message[:100]}...")

            messages = [SystemMessage(content=build_system_prompt())]
            messages.extend(self.chat_histories.get(conversation_key, []))
            messages.append(HumanMessage(content=user_message))

            logger.info("Invoking agent...")
            response = self.agent.invoke({"messages": messages}, {"recursion_limit": 25})
            logger.info("Agent response received")

            # History holds plain text only, so every ToolMessage here belongs to this turn.
            queries = [
                m.artifact for m in response["messages"] if isinstance(m, ToolMessage) and isinstance(m.artifact, dict)
            ]

            ai_messages = [m for m in response["messages"] if isinstance(m, AIMessage)]
            if ai_messages:
                last_response = ai_messages[-1].content
                if queries:  # the UI renders the SQL from `queries`
                    last_response = _SQL_BLOCK.sub("", last_response).strip()

                history = self.chat_histories.setdefault(conversation_key, [])
                history.extend((HumanMessage(content=user_message), AIMessage(content=last_response)))
                self.chat_histories[conversation_key] = history[-20:]

                logger.info(f"Response: {str(last_response)[:100]}...")
                return last_response, queries

            logger.warning("No AI message in response")
            return (
                "Hmm, I wasn't able to generate a response for that. "
                "Could you try rephrasing your question? I'm here to help "
                "with anything about our lakehouse data! 😊"
            ), []

        except Exception as e:
            logger.error(f"Error in chat: {str(e)}")
            logger.error(traceback.format_exc())
            return self._friendly_error(e), []
        finally:
            _query_failures.reset(token)

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
