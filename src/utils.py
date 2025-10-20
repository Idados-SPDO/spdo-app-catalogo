from __future__ import annotations
import re
from datetime import datetime
from io import BytesIO
from typing import Iterable, List
import io
import pandas as pd
import unicodedata
import streamlit as st

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

_DASH_RE = re.compile(r"\s*[-‐-‒–—−]+\s*")

def is_dash_placeholder(s: str | None) -> bool:
    s = (s or "").strip()
    return s == "" or bool(_DASH_RE.fullmatch(s))

def wipe_dashes(s: str | None) -> str:
    """Remove sequências de traços e normaliza espaços."""
    if not s:
        return ""
    out = _DASH_RE.sub(" ", s)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out

def safe_join_comma(parts: list[str]) -> str:
    parts = [p.strip() for p in parts if p and p.strip()]
    return ", ".join(parts)

def safe_qtd_un(qtd_med, un_med) -> str:
    try:
        if qtd_med not in (None, "") and float(qtd_med) > 0:
            um = "" if is_dash_placeholder(un_med) else (un_med or "")
            return (fmt_num(qtd_med) + um).strip()
    except Exception:
        pass
    return ""


def gerar_sinonimo(item, descricao, marca, qtd_med, un_med, emb_produto, qtd_emb_comercial, emb_comercial):
    item        = wipe_dashes(item)
    descricao   = wipe_dashes(descricao)
    marca       = "" if is_dash_placeholder(marca) else wipe_dashes(marca)
    emb_produto = "" if is_dash_placeholder(emb_produto) else wipe_dashes(emb_produto)
    emb_comercial = wipe_dashes(emb_comercial)

    qtd_un = safe_qtd_un(qtd_med, un_med)

    partes = [p for p in [item, descricao, marca, qtd_un] if p]
    sinonimo = " ".join(partes).strip()

    if emb_produto:
        sinonimo = (sinonimo + " COMERCIALIZADO EM " + emb_produto).strip()

    try:
        ### 
        if (qtd_emb_comercial not in (None, "", 1)) and emb_comercial:
            sinonimo = (sinonimo + f" COM {qtd_emb_comercial} {emb_comercial}").strip()
    except Exception:
        pass

    return wipe_dashes(sinonimo)

def gerar_palavra_chave(subfamilia, item, marca, emb_produto, qtd_med, un_med, familia=None):
    # regra 1: MARCA vazia ou apenas hífen -> omite
    marca_clean = "" if is_dash_placeholder(marca) else wipe_dashes(marca)

    # regra 2: SUBFAMILIA vazia ou hífen -> usa FAMILIA
    base_subfam = "" if is_dash_placeholder(subfamilia) else wipe_dashes(subfamilia)
    if not base_subfam:
        base_subfam = "" if is_dash_placeholder(familia) else wipe_dashes(familia)

    item_clean        = wipe_dashes(item)
    emb_produto_clean = "" if is_dash_placeholder(emb_produto) else wipe_dashes(emb_produto)
    qtd_un            = safe_qtd_un(qtd_med, un_med)

    partes = [base_subfam, item_clean, marca_clean, emb_produto_clean]
    out = safe_join_comma(partes)
    if qtd_un:
        out = (out + ", " + qtd_un) if out else qtd_un
    return out.strip()

def gerar_excel(df: pd.DataFrame, sheet_name: str = "Catálogo") -> bytes:
    output = BytesIO()
    # use "xlsxwriter" (recomendado). Alternativa: engine="openpyxl"
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        # Autoajuste simples de largura das colunas
        for i, col in enumerate(df.columns):
            max_len = max(10, min(60, int(df[col].astype(str).map(len).max())))
            ws.set_column(i, i, max_len)
    output.seek(0)
    return output.getvalue()


def _pick(row, *names):
    for n in names:
        if n in row and pd.notna(row[n]) and str(row[n]).strip() != "":
            return row[n]
    return None

def _to_float_safe(x):
    try:
        return float(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None

def _to_int_safe(x):
    try:
        # trata float vindo do Excel (ex.: 10.0)
        if isinstance(x, float) and x.is_integer():
            return int(x)
        return int(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None
    
COLS_TEMPLATE = [
    "REFERENCIA","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","ESPECIFICACAO",
    "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
]

def gerar_template_excel_catalogo() -> bytes:
    """
    Gera um Excel com a ordem exata de colunas usada no cadastro.
    OBS: REFERENCIA e INSUMO são opcionais; demais colunas são obrigatórias.
    ESPECIFICACAO deve ser preenchida no formato 'CHAVE: VALOR; CHAVE2: VALOR2'
    """
    df = pd.DataFrame(columns=COLS_TEMPLATE)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="CATALOGO")
        # opcional: dica em uma segunda aba
        dicas = pd.DataFrame({
            "CAMPO": COLS_TEMPLATE,
            "OBS": [
                "Opcional", "Obrigatório", "Obrigatório", "Obrigatório", "Obrigatório", "Obrigatório",
                "Obrigatório", "Obrigatório", "Opcional", "Obrigatório", "Obrigatório",
                "Obrigatório", "Obrigatório", "Obrigatório", "Obrigatório", "Obrigatório", "Obrigatório",
            ]
        })
        dicas.to_excel(writer, index=False, sheet_name="DICAS")
    return buf.getvalue()

BASE_ORDER_CATALOGO: list[str] = [
    "ID",
    "CODIGO_PRODUTO", "TIPO_CODIGO",
    "GRUPO", "CATEGORIA", "SEGMENTO", "FAMILIA", "SUBFAMILIA",
    "ITEM", "MARCA",
    "EMB_PRODUTO", "UN_MED", "QTD_MED", "EMB_COMERCIAL", "QTD_EMB_COMERCIAL",
    "PALAVRA_CHAVE", "SINONIMO", "DESCRICAO", "ESPECIFICACAO",
    "REFERENCIA",
    "DATA_CADASTRO", "DATA_ATUALIZACAO",
    "APROVADO_EM", "APROVADO_POR", "NOME_VALIDADOR",
]

# === ORDEM ESPECÍFICA DA ATUALIZAÇÃO (a que você descreveu) ===
BASE_ORDER_ATUALIZACAO: list[str] = [
    "ID", "DATA_CADASTRO", "DATA_ATUALIZACAO",
    "GRUPO", "CATEGORIA", "SEGMENTO", "FAMILIA", "SUBFAMILIA",
    "INSUMO", "ITEM",
    "CODIGO_PRODUTO", "TIPO_CODIGO",
    "EMB_PRODUTO", "UN_MED", "QTD_MED", "EMB_COMERCIAL", "QTD_EMB_COMERCIAL",
    "DESCRICAO", "ESPECIFICACAO",
    "PALAVRA_CHAVE", "SINONIMO",
    "REFERENCIA",
]

def apply_column_order(
    df: pd.DataFrame,
    base_order: Iterable[str],
    prepend: Iterable[str] | None = None,
    append: Iterable[str] | None = None,
) -> pd.DataFrame:
    """
    Reordena colunas seguindo:
    1) prepend (se existir)
    2) base_order (apenas as colunas presentes)
    3) quaisquer colunas restantes na ordem original
    4) append (empurradas para o final, se existirem)
    """
    cols_df: List[str] = list(df.columns)
    seen: set[str] = set()

    def pick(seq: Iterable[str] | None) -> list[str]:
        out: list[str] = []
        if not seq:
            return out
        for c in seq:
            if c in cols_df and c not in seen:
                out.append(c)
                seen.add(c)
        return out

    order: list[str] = []
    order += pick(prepend)
    order += pick(base_order)
    order += [c for c in cols_df if c not in seen]  # sobras em ordem original
    order += pick(append)

    return df[order]

# Açúcares sintáticos
def order_catalogo(df: pd.DataFrame, prepend: Iterable[str] | None = None, append: Iterable[str] | None = None) -> pd.DataFrame:
    return apply_column_order(df, BASE_ORDER_CATALOGO, prepend, append)

def order_atualizacao(df: pd.DataFrame, prepend: Iterable[str] | None = None, append: Iterable[str] | None = None) -> pd.DataFrame:
    return apply_column_order(df, BASE_ORDER_ATUALIZACAO, prepend, append)
