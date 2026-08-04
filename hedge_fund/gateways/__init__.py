"""
src.gateways – concrete exchange gateway implementations.

Available gateways:
  AlpacaGateway  – US equities via alpaca-py
  CcxtGateway    – Crypto exchanges via ccxt
  PaperGateway   – In-process paper trading (testing / simulation)
"""

from src.gateways.alpaca_gateway import AlpacaGateway
from src.gateways.ccxt_gateway import CcxtGateway
from src.gateways.paper_gateway import PaperGateway

__all__ = ["AlpacaGateway", "CcxtGateway", "PaperGateway"]
