import os
import pandas as pd
from sqlalchemy import create_engine, text

from langchain_groq import ChatGroq
from langchain.tools import tool
from langchain.agents import initialize_agent, AgentType
# ── DB helper ────────────────────────────────────────────────────────────────

def get_engine():
    password = os.getenv("MYSQL_PASSWORD")

    if not password:
        raise ValueError("MYSQL_PASSWORD not found in environment variables")

    return create_engine(
        f"mysql+mysqlconnector://root:{password}@host.docker.internal:3306/stock_data"
    )


# ── Tools ────────────────────────────────────────────────────────────────────

@tool
def check_missing_values(symbol: str) -> str:
    """Check for null or missing values in latest stock record."""

    engine = get_engine()

    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                text(
                    """
                    SELECT *
                    FROM stock_prices
                    WHERE symbol = :symbol
                    ORDER BY date DESC
                    LIMIT 1
                    """
                ),
                conn,
                params={"symbol": symbol},
            )

        if df.empty:
            return f"No data found for {symbol}."

        nulls = df.isnull().sum()
        null_cols = nulls[nulls > 0]

        if null_cols.empty:
            return f"✅ No missing values for {symbol} on {df['date'].iloc[0]}."

        return (
            f"⚠️ Missing values detected for {symbol}: "
            f"{null_cols.to_dict()} on {df['date'].iloc[0]}"
        )

    finally:
        engine.dispose()


@tool
def check_price_anomaly(symbol: str) -> str:
    """Detect abnormal price movement (>3% change)."""

    engine = get_engine()

    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                text(
                    """
                    SELECT date, close_price
                    FROM stock_prices
                    WHERE symbol = :symbol
                    ORDER BY date DESC
                    LIMIT 8
                    """
                ),
                conn,
                params={"symbol": symbol},
            )

        if len(df) < 2:
            return "Not enough data to check price anomaly."

        df = df.sort_values("date")

        latest = df.iloc[-1]
        previous = df.iloc[-2]

        change_pct = (
            (latest["close_price"] - previous["close_price"])
            / previous["close_price"]
        ) * 100

        if abs(change_pct) > 3:
            direction = "spike" if change_pct > 0 else "drop"

            return (
                f"⚠️ Price anomaly detected for {symbol}: "
                f"{direction} of {change_pct:.2f}% on {latest['date']}"
            )

        return f"✅ No price anomaly for {symbol}. Change: {change_pct:.2f}%"

    finally:
        engine.dispose()


@tool
def check_volume_anomaly(symbol: str) -> str:
    """Detect abnormal volume (>2× weekly avg)."""

    engine = get_engine()

    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                text(
                    """
                    SELECT date, volume
                    FROM stock_prices
                    WHERE symbol = :symbol
                    ORDER BY date DESC
                    LIMIT 8
                    """
                ),
                conn,
                params={"symbol": symbol},
            )

        if len(df) < 2:
            return "Not enough data to check volume anomaly."

        df = df.sort_values("date")

        today = df.iloc[-1]
        avg_volume = df.iloc[:-1]["volume"].mean()

        ratio = today["volume"] / avg_volume

        if ratio >= 2:
            return (
                f"⚠️ Volume anomaly detected for {symbol} on {today['date']}: "
                f"{today['volume']:,} ({ratio:.2f}× weekly avg)"
            )

        return f"✅ Volume normal for {symbol}. Ratio: {ratio:.2f}×"

    finally:
        engine.dispose()


# ── Agent runner ──────────────────────────────────────────────────────────────

def run_stock_quality_agent(symbol: str = "MSFT") -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise ValueError("GROQ_API_KEY missing from environment variables")

    llm = ChatGroq(
        model="llama-3.1-8b-instant",
        temperature=0,
        api_key=api_key,
    )

    tools = [check_missing_values, check_price_anomaly, check_volume_anomaly]

    agent_executor = initialize_agent(
        tools=tools,
        llm=llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        handle_parsing_errors=True,
        max_iterations=6,
    )

    result = agent_executor.invoke({
        "input": f"""
            You are a stock ETL data-quality analyst.
            Perform these checks for symbol {symbol}:
            1) Missing values  2) Price anomaly  3) Volume anomaly
            Then provide a final summary of data quality.
        """
    })

    return result["output"]