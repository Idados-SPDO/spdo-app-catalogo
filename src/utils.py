from __future__ import annotations
import re
from datetime import datetime
from io import BytesIO
import pandas as pd

import io
import pandas as pd

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
        if (qtd_emb_comercial not in (None, "", 1)) and emb_comercial:
            sinonimo = (sinonimo + f" COM {emb_comercial} UNIDADES").strip()
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
    

def gerar_template_excel_catalogo() -> bytes:
    """
    Gera um XLSX com:
      - Sheet 'Dados' com os mesmos cabeçalhos do exemplo do usuário
        (inclui um bloco ESPECIFICACAO com 6 colunas posicionais).
      - A PRIMEIRA LINHA DE DADOS define as chaves das especificações
        nas 6 colunas do bloco (COMPLETA, TIPO, TEMPERO, OSSO, CARACTERISTICA, LINHA).
      - A SEGUNDA LINHA DE DADOS é um exemplo preenchido.
      - Sheet 'Instruções' com orientações.
    Obs.: As 6 colunas do bloco ESPECIFICACAO têm nomes repetidos/“vazios” propositalmente,
    pois seu pipeline lê por POSIÇÃO (13..18) e captura a linha 0 de dados como header do bloco.
    """

    # Cabeçalhos exatamente na ordem do exemplo
    cols = [
        "ORIGINAL",
        "CODIGO DO INSUMO",
        "DATA_CADASTRO",
        "DATA_ATUALIZAÇÃO",
        "DICIONARIO",
        "GRUPO",
        "SUBGRUPO",
        "CATEGORIA",
        "FAMILIA",
        "SUBFAMILIA",
        "ITEM",
        "EAN",
        "ESPECIFICACAO", "", "", "", "", "",        # 6 colunas posicionais (13..18)
        "TIPO_PRODUTO",                              # incluído no template
        "MARCA",
        "EMB_PRODUTO",
        "UN_MED",
        "QTD_MED",
        "EMB_COMERCIAL",
        "QTD_EMB_COMERCIAL",
    ]

    # Linha 1 de dados: nomes das especificações no bloco 13..18
    r0 = {c: "" for c in cols}
    r0.update({
        cols[12]: "COMPLETA",    # 13a coluna
        cols[13]: "TIPO",
        cols[14]: "TEMPERO",
        cols[15]: "OSSO",
        cols[16]: "CARACTERISTICA",
        cols[17]: "LINHA",
    })

    # Linha 2 de dados: exemplo preenchido (similar ao que você enviou)
    r1 = {c: "" for c in cols}
    r1.update({
        "ORIGINAL": "7896581300188,Asa de Frango Bandeja Pif Paf 1kg,Frango",
        "CODIGO DO INSUMO": "",                  # se tiver um código interno
        "DATA_CADASTRO": "11/09/2025",
        "DATA_ATUALIZAÇÃO": "",
        "DICIONARIO": "",
        "GRUPO": "ALIMENTOS E BEBIDAS",
        "SUBGRUPO": "ALIMENTOS",
        "CATEGORIA": "CARNES E AVES",
        "FAMILIA": "AVES",
        "SUBFAMILIA": "FRANGO",
        "ITEM": "ASA DE FRANGO",
        "EAN": "7896581300188",                  # como texto para evitar notação científica
        cols[12]: "ASA DE FRANGO CONGELADO SEM TEMPERO COM OSSO",
        cols[13]: "CONGELADO",
        cols[14]: "SEM TEMPERO",
        cols[15]: "COM OSSO",
        cols[16]: "",
        cols[17]: "",
        "TIPO_PRODUTO": "CONGELADO",
        "MARCA": "PIF PAF",
        "EMB_PRODUTO": "BANDEJA",
        "UN_MED": "KG",
        "QTD_MED": 1,
        "EMB_COMERCIAL": "UNIDADE",
        "QTD_EMB_COMERCIAL": 1,
    }) # type: ignore

    # Outro exemplo (opcional)
    r2 = {c: "" for c in cols}
    r2.update({
        "ORIGINAL": "7898525451420,Asa de Frango Congelada Pacote LeVida 900g,Asa de Frango,Frango",
        "DATA_CADASTRO": "11/09/2025",
        "GRUPO": "ALIMENTOS E BEBIDAS",
        "SUBGRUPO": "ALIMENTOS",
        "CATEGORIA": "CARNES E AVES",
        "FAMILIA": "AVES",
        "SUBFAMILIA": "FRANGO",
        "ITEM": "ASA DE FRANGO",
        "EAN": "7898525451420",
        cols[12]: "ASA DE FRANGO CONGELADO SEM TEMPERO COM OSSO IQF",
        cols[13]: "CONGELADO",
        cols[14]: "SEM TEMPERO",
        cols[15]: "COM OSSO",
        cols[16]: "IQF",
        cols[17]: "",
        "TIPO_PRODUTO": "CONGELADO",
        "MARCA": "LEVIDA",
        "EMB_PRODUTO": "PACOTE",
        "UN_MED": "G",
        "QTD_MED": 900,
        "EMB_COMERCIAL": "UNIDADE",
        "QTD_EMB_COMERCIAL": 1,
    }) # type: ignore

    df = pd.DataFrame([r0, r1, r2], columns=cols)

    # Instruções
    instr = pd.DataFrame({
        "Instruções": [
            "1) Preencha as colunas normalmente (ORIGINAL, GRUPO, etc).",
            "2) O BLOCO ESPECIFICACAO são 6 colunas (posições 13..18).",
            "   A PRIMEIRA LINHA DE DADOS (logo após o cabeçalho) deve conter os NOMES das especificações nessas 6 colunas.",
            "   Ex.: COMPLETA | TIPO | TEMPERO | OSSO | CARACTERISTICA | LINHA",
            "3) A PARTIR DA LINHA SEGUINTE, preencha os VALORES para essas mesmas chaves.",
            "4) O pipeline vai ler a linha 1 de dados para descobrir os nomes das chaves e depois montar ESPECIFICACAO_TXT.",
            "5) Use '-' em SUBFAMILIA para cair no fallback de FAMILIA na PALAVRA_CHAVE; use '-' em MARCA para omiti-la.",
            "6) EAN e CODIGO DO INSUMO são tratados como texto; evite fórmulas/formatos que gerem notação científica.",
            "7) TIPO_PRODUTO é opcional, mas recomendado (aparece nos filtros da lista).",
        ]
    })

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Dados")
        instr.to_excel(writer, index=False, sheet_name="Instruções")

        # Auto-ajuste de largura
        ws_d = writer.sheets["Dados"]
        for i in range(len(df.columns)):
            col_series = df.iloc[:, i].astype(str)
            max_len = max(12, min(50, int(max(10, col_series.map(len).max()))))
            ws_d.set_column(i, i, max_len)

        ws_i = writer.sheets["Instruções"]
        ws_i.set_column(0, 0, 110)

    bio.seek(0)
    return bio.getvalue()