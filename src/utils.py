from __future__ import annotations
import re
from datetime import datetime

PT_DATE_FMT = "%d/%m/%Y"

def data_hoje() -> str:
    return datetime.now().strftime(PT_DATE_FMT)

_DEF_PAIR_RE = re.compile(r"\s*([^:;]+)\s*:\s*([^;]+)\s*")


def extrair_valores(especificacao: str | None) -> str:
    """De "CHAVE: VALOR; CHAVE: VALOR" -> "VALOR VALOR"."""
    if not especificacao:
        return ""
    vals = []
    for m in _DEF_PAIR_RE.finditer(especificacao):
        vals.append(m.group(2).strip())
    return " ".join(vals)


def extrair_chaves(especificacao: str | None) -> str:
    """De "CHAVE: VALOR; CHAVE: VALOR" -> "CHAVE CHAVE"."""
    if not especificacao:
        return ""
    keys = []
    for m in _DEF_PAIR_RE.finditer(especificacao):
        keys.append(m.group(1).strip())
    return " ".join(keys)


def campos_obrigatorios_ok(d: dict[str, object], zeros_invalidos: bool = True) -> tuple[bool, list[str]]:
    vazios = []
    for k, v in d.items():
        if v is None or v == "":
            vazios.append(k)
        elif zeros_invalidos and (isinstance(v, (int, float)) and v == 0):
            vazios.append(k)
    return (len(vazios) == 0, vazios)