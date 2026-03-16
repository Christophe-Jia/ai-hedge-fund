"""
统一配置注册表 — src/config.py

作用：
  1. 作为开发者查阅的配置清单（所有环境变量的唯一文档性索引）
  2. 作为 .env 文件生成的来源
  3. 供 Web App API key UI 查询"支持哪些 provider"

使用方式：
  from src.config import ALL_CONFIG, get_config, get_all_by_category

  # 读取单个配置
  api_key = get_config("OPENAI_API_KEY")

  # 列出所有 LLM 相关配置项
  llm_configs = get_all_by_category("llm")

注意：此文件不存储任何 key 值，不修改任何现有逻辑。
      实际 key 值仅存储在本地 .env 或 Web App SQLite 数据库中。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class ConfigEntry:
    """单个配置项的元数据描述。"""

    key: str          # 环境变量名
    category: str     # "llm" | "price_data" | "trading" | "social"
    description: str  # 用途说明
    required: bool    # 是否必填（False = 可选）
    default: str = "" # 默认值（仅作文档用途，不用于读取逻辑）


# ─────────────────────────────────────────────────────────────────────────────
# 所有配置项注册表（按类别分组）
# ─────────────────────────────────────────────────────────────────────────────

ALL_CONFIG: list[ConfigEntry] = [

    # ── LLM Providers ────────────────────────────────────────────────────────

    ConfigEntry(
        key="OPENAI_API_KEY",
        category="llm",
        description="OpenAI API Key（也用于 Moonshot/Kimi，配合 OPENAI_API_BASE）",
        required=False,
    ),
    ConfigEntry(
        key="OPENAI_API_BASE",
        category="llm",
        description=(
            "OpenAI 兼容 API base URL。"
            "Moonshot(Kimi): https://api.moonshot.cn/v1  "
            "留空则使用 OpenAI 官方端点。"
            "model_provider=OpenAI, model=moonshot-v1-8k / moonshot-v1-32k / moonshot-v1-128k"
        ),
        required=False,
        default="",
    ),
    ConfigEntry(
        key="ANTHROPIC_API_KEY",
        category="llm",
        description="Anthropic Claude API Key（claude-4-sonnet, claude-4.1-opus 等）",
        required=False,
    ),
    ConfigEntry(
        key="GROQ_API_KEY",
        category="llm",
        description="Groq API Key（deepseek-r1-distill-llama-70b, llama3 等，免费额度大）",
        required=False,
    ),
    ConfigEntry(
        key="DEEPSEEK_API_KEY",
        category="llm",
        description="DeepSeek API Key（deepseek-chat, deepseek-reasoner）",
        required=False,
    ),
    ConfigEntry(
        key="GOOGLE_API_KEY",
        category="llm",
        description="Google Gemini API Key（gemini-2.5-flash, gemini-2.5-pro）",
        required=False,
    ),
    ConfigEntry(
        key="XAI_API_KEY",
        category="llm",
        description="xAI Grok API Key（Grok 4 等）",
        required=False,
    ),
    ConfigEntry(
        key="GIGACHAT_API_KEY",
        category="llm",
        description="GigaChat API Key（Sberbank 俄语大模型）",
        required=False,
    ),
    ConfigEntry(
        key="OPENROUTER_API_KEY",
        category="llm",
        description="OpenRouter API Key（聚合多家模型，按用量计费）",
        required=False,
    ),
    ConfigEntry(
        key="AZURE_OPENAI_API_KEY",
        category="llm",
        description="Azure OpenAI API Key",
        required=False,
    ),
    ConfigEntry(
        key="AZURE_OPENAI_ENDPOINT",
        category="llm",
        description="Azure OpenAI Endpoint URL",
        required=False,
    ),
    ConfigEntry(
        key="AZURE_OPENAI_DEPLOYMENT_NAME",
        category="llm",
        description="Azure OpenAI Deployment Name（部署名称，例如 gpt-4o）",
        required=False,
    ),

    # ── Price & Fundamental Data ──────────────────────────────────────────────

    ConfigEntry(
        key="FINANCIAL_DATASETS_API_KEY",
        category="price_data",
        description=(
            "financialdatasets.ai API Key。"
            "免费 ticker（无需 key）：AAPL, GOOGL, MSFT, NVDA, TSLA。"
            "其他 ticker 需要付费 key。"
        ),
        required=False,
    ),

    # ── Crypto Trading (CCXT) ─────────────────────────────────────────────────

    ConfigEntry(
        key="CCXT_EXCHANGE",
        category="trading",
        description="交易所 ID，支持 binance / okx / bybit / kraken / coinbase 等",
        required=False,
        default="binance",
    ),
    ConfigEntry(
        key="CCXT_API_KEY",
        category="trading",
        description="CCXT 交易所 API Key（公开行情不需要，实盘下单必填）",
        required=False,
    ),
    ConfigEntry(
        key="CCXT_API_SECRET",
        category="trading",
        description="CCXT 交易所 API Secret（实盘下单必填）",
        required=False,
    ),
    ConfigEntry(
        key="CCXT_SANDBOX",
        category="trading",
        description="设为 true 使用交易所沙盒/测试网（强烈建议初始使用）",
        required=False,
        default="true",
    ),

    # ── US Stock Live Trading (Alpaca) ─────────────────────────────────────────

    ConfigEntry(
        key="ALPACA_API_KEY",
        category="trading",
        description="Alpaca API Key（paper 模式免费，无需真实资金）",
        required=False,
    ),
    ConfigEntry(
        key="ALPACA_API_SECRET",
        category="trading",
        description="Alpaca API Secret",
        required=False,
    ),
    ConfigEntry(
        key="ALPACA_BASE_URL",
        category="trading",
        description=(
            "Alpaca Base URL。"
            "Paper 模式：https://paper-api.alpaca.markets  "
            "Live 模式：https://api.alpaca.markets"
        ),
        required=False,
        default="https://paper-api.alpaca.markets",
    ),

    # ── Social Media Data ─────────────────────────────────────────────────────

    ConfigEntry(
        key="REDDIT_CLIENT_ID",
        category="social",
        description="Reddit App Client ID（在 https://www.reddit.com/prefs/apps 创建 script 类型 app）",
        required=False,
    ),
    ConfigEntry(
        key="REDDIT_CLIENT_SECRET",
        category="social",
        description="Reddit App Client Secret",
        required=False,
    ),
    ConfigEntry(
        key="REDDIT_USER_AGENT",
        category="social",
        description="Reddit User Agent 字符串，例如：ai-hedge-fund/1.0 by YourUsername",
        required=False,
        default="ai-hedge-fund/1.0",
    ),
    ConfigEntry(
        key="STOCKTWITS_ACCESS_TOKEN",
        category="social",
        description="StockTwits Access Token（可选，不填也能访问公开端点，填了提高速率限制）",
        required=False,
    ),
    ConfigEntry(
        key="TELEGRAM_BOT_TOKEN",
        category="social",
        description="Telegram Bot Token（通过 @BotFather 创建，用于监控加密货币频道/群组）",
        required=False,
    ),
    ConfigEntry(
        key="TELEGRAM_CHAT_IDS",
        category="social",
        description="要监控的 Telegram Chat ID，逗号分隔，群组为负数，例如 -1001234567890,-1009876543210",
        required=False,
    ),

    # ── Sentiment Analysis ────────────────────────────────────────────────────

    ConfigEntry(
        key="FINBERT_MODEL",
        category="social",
        description="FinBERT HuggingFace 模型名称（首次运行自动下载）",
        required=False,
        default="ProsusAI/finbert",
    ),
]


# ─────────────────────────────────────────────────────────────────────────────
# 便捷函数
# ─────────────────────────────────────────────────────────────────────────────

def get_config(key: str, default: str = "") -> str:
    """统一读取配置，先查环境变量，未设置则返回 default。

    用法：
        api_key = get_config("OPENAI_API_KEY")
        base_url = get_config("OPENAI_API_BASE", "")
    """
    return os.environ.get(key, default)


def get_all_by_category(category: str) -> list[ConfigEntry]:
    """按类别列出所有配置项（供 Web UI 渲染 provider 列表使用）。

    Args:
        category: "llm" | "price_data" | "trading" | "social"

    Returns:
        该类别下的所有 ConfigEntry 列表。
    """
    return [c for c in ALL_CONFIG if c.category == category]


def get_entry(key: str) -> ConfigEntry | None:
    """按环境变量名查找 ConfigEntry，找不到返回 None。"""
    for entry in ALL_CONFIG:
        if entry.key == key:
            return entry
    return None


def list_categories() -> list[str]:
    """返回所有不重复的类别名称，顺序同 ALL_CONFIG。"""
    seen: set[str] = set()
    result: list[str] = []
    for entry in ALL_CONFIG:
        if entry.category not in seen:
            seen.add(entry.category)
            result.append(entry.category)
    return result
