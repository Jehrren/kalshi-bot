"""
Gemeinsame Datenmodelle für das Crypto-Handelssystem.

Ausgelagert aus rules.py um Circular-Imports zu vermeiden
(rules.py ↔ rules_15min.py würden sonst zirkulär importieren).
"""

from dataclasses import dataclass, field

SYSTEM = "crypto"


@dataclass
class CryptoSignal:
    ticker:      str
    rule_name:   str
    side:        str
    action:      str
    price_cents: int
    count:       int
    reason:      str
    system:      str = SYSTEM
    meta:        dict = field(default_factory=dict)
    track:       str = "crypto"
