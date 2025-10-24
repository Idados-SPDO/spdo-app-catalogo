from __future__ import annotations
import re
import unicodedata
import pandas as pd
from typing import Any
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import os
from typing import Optional, Dict, Any
import json
from src.variables import FQN_USERS, FQN_APR, FQN_COR, FQN_MAIN, FQN_LOG_ATUAL, FQN_LOG_REPROV, FQN_LOG_VALID
# =========================
# Conexão
# =========================

def load_user_display_map(session) -> dict[str, str]:
    """
    Retorna {username: name}. Se falhar, retorna {}.
    """
    try:
        dfu = session.sql(f"SELECT USERNAME, NAME FROM {FQN_USERS}").to_pandas()
        dfu["USERNAME"] = dfu["USERNAME"].astype(str)
        dfu["NAME"] = dfu["NAME"].astype(str)
        return dict(zip(dfu["USERNAME"], dfu["NAME"]))
    except Exception:
        return {}

ALL = "— Todos —"

def _norm_txt(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.casefold().strip()

def _username_from_email_or_raw(u: str | None) -> str | None:
    if not u or (isinstance(u, float) and pd.isna(u)):
        return None
    u = str(u).strip()
    # se vier "nome@dominio", pegue só antes do "@"
    if "@" in u:
        u = u.split("@", 1)[0]
    return u

def _build_display_to_usernames(df: pd.DataFrame, user_map: dict | None) -> dict[str, set[str]]:
    """
    Retorna um dicionário: display_norm -> {usernames_norm}
    Aceita user_map em qualquer direção (username->display ou display->username).
    Também usa o que está em df['USUARIO_CADASTRO'] para complementar.
    """
    d2u: dict[str, set[str]] = {}

    def add(display: str | None, username: str | None):
        if not display or not username:
            return
        dn = _norm_txt(display)
        un = _norm_txt(username)
        if not dn or not un:
            return
        d2u.setdefault(dn, set()).add(un)

    # 1) A partir do user_map (aguenta os 2 sentidos)
    if isinstance(user_map, dict):
        for k, v in user_map.items():
            # caso 1: username -> display
            un1 = _username_from_email_or_raw(k)
            if isinstance(v, str):
                add(v, un1)
            # caso 2: display -> username
            un2 = _username_from_email_or_raw(v if isinstance(v, str) else None)
            if isinstance(k, str) and un2:
                add(k, un2)

    # 2) Complementa com o que existe no DF
    if "USUARIO_CADASTRO" in df.columns:
        for _, row in df[["USUARIO_CADASTRO"]].iterrows():
            raw_user = row.get("USUARIO_CADASTRO")
            un = _username_from_email_or_raw(raw_user)
            # se não houver display no map, use o próprio username como display
            if un:
                add(un, un)

    return d2u

def _to_display(v: str, user_map: dict | None) -> str:
    """
    Normaliza um valor (username ou display) para display name.
    Ex.: 'yago.m' -> 'Yago Moraes' se existir no user_map; caso contrário mantém.
    """
    if not isinstance(user_map, dict):
        return v
    return user_map.get(v, v)

def build_user_options(df: pd.DataFrame, user_map: dict | None) -> list[str]:
    """
    Retorna opções únicas de usuários **em display name** para o selectbox,
    sem duplicatas e já ordenadas alfabeticamente. Inclui a opção ALL.
    """
    if "USUARIO_CADASTRO" not in df.columns or df.empty:
        return [ALL]

    col = (
        df["USUARIO_CADASTRO"]
        .astype(str)
        .str.strip()
        .replace({"None": "", "nan": ""})
    )

    # Converte qualquer username para display name usando o mapa
    col_display = col.map(lambda v: _to_display(v, user_map))

    # Remove vazios, dedup e ordena
    uniques = sorted({x for x in col_display if x}, key=str.casefold)

    return [ALL, *uniques]

def apply_common_filters(
    df: pd.DataFrame,
    *,
    sel_user_name: str | None = None,
    f_insumo: str | None = None,
    f_codigo: str | None = None,
    f_palavra: str | None = None,
    user_map: dict | None = None,
) -> pd.Series:
    """
    Retorna uma máscara booleana aplicando:
      - filtro por usuário (sempre comparando com **display name**),
      - Insumo, Código do Produto e Palavra-chave (contains, case-insensitive).
    Se a coluna USUARIO_CADASTRO vier com username, converte para display via user_map.
    """
    if df.empty:
        return pd.Series(False, index=df.index)

    mask = pd.Series(True, index=df.index)

    # ---------- Filtro por usuário (sempre display name) ----------
    if "USUARIO_CADASTRO" in df.columns and sel_user_name and sel_user_name != ALL:
        col_user = (
            df["USUARIO_CADASTRO"]
            .astype(str)
            .str.strip()
            .replace({"None": "", "nan": ""})
        )
        # Normaliza a coluna para display name usando o mapa (corrige casos com username)
        col_user_display = col_user.map(lambda v: _to_display(v, user_map)).fillna("")
        mask &= col_user_display.str.casefold() == str(sel_user_name).casefold()

    # ---------- Filtro por Insumo ----------
    if f_insumo:
        if "INSUMO" in df.columns:
            mask &= df["INSUMO"].astype(str).str.contains(str(f_insumo), case=False, na=False)

    # ---------- Filtro por Código do Produto (exato ou contém) ----------
    if f_codigo:
        if "CODIGO_PRODUTO" in df.columns:
            # permite 'contém' porque pode haver zeros à esquerda, etc.
            mask &= df["CODIGO_PRODUTO"].astype(str).str.contains(str(f_codigo), case=False, na=False)

    # ---------- Filtro por Palavra-chave (em várias colunas: PALAVRA_CHAVE, SINONIMO, DESCRICAO) ----------
    if f_palavra:
        needles = str(f_palavra)
        cols_busca = [c for c in ["PALAVRA_CHAVE", "SINONIMO", "DESCRICAO", "ITEM", "ESPECIFICACAO"] if c in df.columns]
        if cols_busca:
            any_col = pd.Series(False, index=df.index)
            for c in cols_busca:
                any_col |= df[c].astype(str).str.contains(needles, case=False, na=False)
            mask &= any_col

    return mask

def _build_local_session() -> Session:
    cfg = st.secrets["snowflake"]
    return Session.builder.configs(cfg).create()


def get_session() -> Session:
    try:
        return get_active_session()
    except Exception:
        return _build_local_session()

# =========================
# DDL/CRUD
# =========================

def codigo_produto_exists_any(session: Session, codigo: str | None) -> tuple[bool, str]:
    """
    Verifica se o CODIGO_PRODUTO existe em alguma tabela “ativa”:
    - TB_CATALOGO_APROVADOS (aprovados)
    - TB_CATALOGO_INSUMOS  (cadastros pendentes/histórico)
    Retorna (existe, origem) onde origem ∈ {"APROVADOS", "PENDENTES", ""}.
    """
    if not codigo:
        return (False, "")
    codigo = str(codigo).strip()
    # 1) Aprovados primeiro (tem prioridade de bloqueio)
    q1 = f"""
      SELECT 1
      FROM {FQN_APR}
      WHERE CODIGO_PRODUTO = ?
      LIMIT 1
    """
    if not session.sql(q1, params=[codigo]).to_pandas().empty:
        return (True, "APROVADOS")

    # 2) Pendentes (insumos_h)
    q2 = f"""
      SELECT 1
      FROM {FQN_MAIN}
      WHERE CODIGO_PRODUTO = ?
      LIMIT 1
    """
    if not session.sql(q2, params=[codigo]).to_pandas().empty:
        return (True, "PENDENTES")

    return (False, "")

def insert_item(session: Session, item: dict[str, Any]) -> tuple[bool, str]:
    codigo = (item.get("CODIGO_PRODUTO") or "").strip()
    if not codigo:
        return False, "CODIGO_PRODUTO é obrigatório."
    exists, origem = codigo_produto_exists_any(session, codigo)
    if exists:
        return False, f"CODIGO_PRODUTO '{codigo}' já existe na base. Origem: '{origem}'"
    cols = [
        "REFERENCIA",
        "GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
        "TIPO_CODIGO","CODIGO_PRODUTO",
        "INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
        "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
        "SINONIMO","PALAVRA_CHAVE",
        "USUARIO_CADASTRO","USUARIO_ATUALIZACAO",
    ]

    placeholders = ", ".join([f":{i+1}" for i in range(len(cols))])
    sql = f"""
        INSERT INTO {FQN_MAIN}
        ({", ".join(cols)}, DATA_CADASTRO)
        VALUES ({placeholders}, CURRENT_TIMESTAMP())
    """
    try:
        params = [item.get(c) for c in cols]
        session.sql(sql, params).collect()
        return True, "Item salvo com sucesso."
    except Exception as e:
        msg = str(e)
        if "unique" in msg.lower():
            return False, "CODIGO_PRODUTO ou INSUMO já cadastrado."
        return False, f"Erro ao salvar item: {e}"


def listar_itens_df(session: Session) -> pd.DataFrame:
    try:
        t = session.table(FQN_MAIN)
        excluir = {"DATA_VALIDACAO", "USUARIO_VALIDADOR", "DATA_ATUALIZACAO", "USUARIO_ATUALIZACAO"}
        cols = [f.name for f in t.schema.fields if f.name not in excluir]
        return t.select(cols).sort("DATA_CADASTRO", ascending=False).to_pandas()
    except Exception:
        return pd.DataFrame()
    

def _make_salt(n: int = 16) -> str:
    return os.urandom(n).hex()

def _hash_password(password: str, salt: str) -> str:
    import hashlib
    m = hashlib.sha256()
    m.update((salt + password).encode("utf-8"))
    return m.hexdigest()

def users_list_usernames(session: Session) -> list[str]:
    try:
        df = session.sql(f"SELECT USERNAME FROM {FQN_USERS} ORDER BY USERNAME").to_pandas()
        return [str(u) for u in df["USERNAME"].tolist()]
    except Exception:
        return []

def users_get(session: Session, username: str) -> Optional[Dict[str, Any]]:
    sql = f"""
      SELECT USERNAME, NAME, ROLE, PASSWORD_HASH, SALT
      FROM {FQN_USERS}
      WHERE USERNAME = ?
    """
    try:
        df = session.sql(sql, params=[username]).to_pandas()
        if df.empty:
            return None
        r = df.iloc[0]
        return {
            "username": r["USERNAME"],
            "name": r.get("NAME"),
            "role": r.get("ROLE"),
            "password_hash": r.get("PASSWORD_HASH"),
            "salt": r.get("SALT"),
        }
    except Exception:
        return None

def users_create_or_update(session: Session, username: str, name: str, role: str, password: str) -> None:
    salt = _make_salt()
    phash = _hash_password(password, salt)
    sql = f"""
      MERGE INTO {FQN_USERS} t
      USING (SELECT ? AS USERNAME, ? AS NAME, ? AS ROLE, ? AS PASSWORD_HASH, ? AS SALT) s
      ON t.USERNAME = s.USERNAME
      WHEN MATCHED THEN UPDATE SET
        NAME=s.NAME, ROLE=s.ROLE, PASSWORD_HASH=s.PASSWORD_HASH, SALT=s.SALT
      WHEN NOT MATCHED THEN INSERT (USERNAME, NAME, ROLE, PASSWORD_HASH, SALT)
      VALUES (s.USERNAME, s.NAME, s.ROLE, s.PASSWORD_HASH, s.SALT)
    """
    session.sql(sql, params=[username, name, role, phash, salt]).collect()

def users_update_password(session: Session, username: str, new_password: str) -> None:
    salt = _make_salt()
    phash = _hash_password(new_password, salt)
    sql = f"UPDATE {FQN_USERS} SET PASSWORD_HASH = ?, SALT = ? WHERE USERNAME = ?"
    session.sql(sql, params=[phash, salt, username]).collect()

def users_check_password(session: Session, username: str, password: str) -> bool:
    u = users_get(session, username)
    if not u or not u.get("salt") or not u.get("password_hash"):
        return False
    return _hash_password(password, u["salt"]) == u["password_hash"]


def _sql_json(value) -> str:
    if value is None:
        return "NULL"
    return "PARSE_JSON('" + json.dumps(value).replace("'", "''") + "')"

def _sql_array(iterable) -> str:
    if not iterable:
        return "NULL"
    items = ", ".join("'" + str(x).replace("'", "''") + "'" for x in iterable)
    return f"ARRAY_CONSTRUCT({items})"

def fetch_row_snapshot(session, table_fqn: str, item_id: int):
    try:
        df = session.sql(f"SELECT OBJECT_CONSTRUCT(*) AS O FROM {table_fqn} WHERE ID = ?", params=[item_id]).to_pandas()
        if df.empty:
            return None
        return df.iloc[0]["O"]
    except Exception:
        return None

def log_validacao(session, *, item_id, codigo_produto, origem, destino, obs, user):
    sql = f"""
      INSERT INTO {FQN_LOG_VALID}
      (ITEM_ID, CODIGO_PRODUTO, ORIGEM_TABELA, DESTINO_TABELA, OBSERVACAO, APROVADO_POR_USER, APROVADO_POR_NOME)
      SELECT ?, ?, ?, ?, ?, ?, ?
    """
    params = [
        item_id, codigo_produto, origem, destino, obs,
        (user or {}).get("username"), (user or {}).get("name"),
    ]
    session.sql(sql, params=params).collect()

def log_reprovacao(session, *, item_id, codigo_produto, origem, destino, motivo, user):
    sql = f"""
      INSERT INTO {FQN_LOG_REPROV}
      (ITEM_ID, CODIGO_PRODUTO, ORIGEM_TABELA, DESTINO_TABELA, MOTIVO, REPROVADO_POR_USER, REPROVADO_POR_NOME)
      SELECT ?, ?, ?, ?, ?, ?, ?
    """
    params = [
        item_id, codigo_produto, origem, destino, motivo,
        (user or {}).get("username"), (user or {}).get("name"),
    ]
    session.sql(sql, params=params).collect()

def log_atualizacao(session, *, item_id, codigo_produto, colunas_alteradas, before_obj, after_obj, user):
    sql = f"""
      INSERT INTO {FQN_LOG_ATUAL}
      (ITEM_ID, CODIGO_PRODUTO, COLUNAS_ALTERADAS, BEFORE_SNAPSHOT, AFTER_SNAPSHOT, ATUALIZADO_POR_USER, ATUALIZADO_POR_NOME)
      SELECT ?, ?, {_sql_array(colunas_alteradas)}, {_sql_json(before_obj)}, {_sql_json(after_obj)}, ?, ?
    """
    params = [
        item_id, codigo_produto,
        (user or {}).get("username"), (user or {}).get("name"),
    ]
    session.sql(sql, params=params).collect()


# --- add acima (próximo das outras funções) ---
def fetch_existing_codigos_dual(session: Session, codigos: list[str]) -> tuple[set[str], set[str]]:
    """
    Retorna 2 conjuntos:
      - exist_pend: códigos que existem em TB_CATALOGO_INSUMOS
      - exist_aprv: códigos que existem em TB_CATALOGO_APROVADOS
    """
    if not codigos:
        return set(), set()

    codigos = [c for c in {str(x).strip() for x in codigos} if c]
    CHUNK = 1000

    exist_pend: set[str] = set()
    exist_aprv: set[str] = set()

    for i in range(0, len(codigos), CHUNK):
        chunk = codigos[i:i+CHUNK]
        placeholders = ", ".join(["?"] * len(chunk))

        q_pend = f"""
          SELECT DISTINCT CODIGO_PRODUTO
          FROM {FQN_MAIN}
          WHERE CODIGO_PRODUTO IN ({placeholders})
        """
        df1 = session.sql(q_pend, params=chunk).to_pandas()
        if not df1.empty:
            exist_pend |= {str(x).strip() for x in df1["CODIGO_PRODUTO"].astype(str).tolist()}

        q_aprv = f"""
          SELECT DISTINCT CODIGO_PRODUTO
          FROM {FQN_APR}
          WHERE CODIGO_PRODUTO IN ({placeholders})
        """
        df2 = session.sql(q_aprv, params=chunk).to_pandas()
        if not df2.empty:
            exist_aprv |= {str(x).strip() for x in df2["CODIGO_PRODUTO"].astype(str).tolist()}

    return exist_pend, exist_aprv
