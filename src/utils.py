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

def campos_obrigatorios_ok(d: dict[str, object], zeros_invalidos: bool = True) -> tuple[bool, list[str]]:
    vazios = []
    for k, v in d.items():
        if v is None or v == "":
            vazios.append(k)
        elif zeros_invalidos and (isinstance(v, (int, float)) and v == 0):
            vazios.append(k)
    return (len(vazios) == 0, vazios)

def fmt_num(n):
    if n in (None, ""):
        return ""
    try:
        f = float(n)
        return str(int(f)) if f.is_integer() else str(f)
    except:
        return str(n)

def gerar_sinonimo(item, descricao, marca, qtd_med, un_med, emb_produto, qtd_emb_comercial, emb_comercial):
    # monta quantidade+unidade só se qtd_med > 0
    qtd_un = ""
    try:
        if qtd_med not in (None, "") and float(qtd_med) > 0:
            qtd_un = fmt_num(qtd_med) + (un_med or "")
    except:
        qtd_un = ""

    partes = [p for p in [item, descricao, marca, qtd_un] if p]
    sinonimo = " ".join(partes)

    if emb_produto:
        sinonimo = (sinonimo + " COMERCIALIZADO EM " + emb_produto).strip()

    # só adiciona " COM X UNIDADES" se qtd_emb_comercial existir e for != 1
    try:
        if qtd_emb_comercial not in (None, "") and int(qtd_emb_comercial) != 1 and emb_comercial:
            sinonimo = sinonimo + f" COM {emb_comercial} UNIDADES"
    except:
        pass

    return sinonimo.strip()

def gerar_palavra_chave(subfamilia, item, marca, emb_produto, qtd_med, un_med):
    qtd_un = ""
    try:
        if qtd_med not in (None, "") and float(qtd_med) > 0:
            qtd_un = fmt_num(qtd_med) + (un_med or "")
    except:
        qtd_un = ""

    partes = [p for p in [subfamilia, item, marca, emb_produto] if p]
    palavra_chave = ", ".join(partes)
    if qtd_un:
        palavra_chave = (palavra_chave + ", " + qtd_un) if palavra_chave else qtd_un

    return palavra_chave.strip()