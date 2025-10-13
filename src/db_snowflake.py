from __future__ import annotations
import pandas as pd
from typing import Any
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import os
from typing import Optional, Dict, Any
import json
# =========================
# Conexão
# =========================
FQN_USERS = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_USERS"

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

def build_user_options(df: pd.DataFrame, user_map: dict[str,str]) -> list[str]:
    """
    Constrói lista de nomes para o dropdown. Se o df tem usernames,
    converte pra nomes quando possível.
    """
    raw = (
        df.get("USUARIO_CADASTRO", pd.Series(dtype=str))
          .dropna()
          .astype(str)
          .unique()
          .tolist()
    )
    names = sorted({ user_map.get(u, u) for u in raw })  # type: ignore
    return ["— Todos —"] + names

def apply_common_filters(df: pd.DataFrame, *, sel_user_name: str, f_insumo: str, f_codigo: str, f_palavra: str, user_map: dict[str,str]) -> pd.Series:
    """
    Retorna uma máscara booleana aplicando os 4 filtros.
    - sel_user_name: nome escolhido no dropdown (ou '— Todos —')
    """
    mask = pd.Series(True, index=df.index)

    # 1) Usuário (nome → usernames correspondentes)
    if sel_user_name and sel_user_name != "— Todos —":
        # caminhar user_map inverso: nomes podem repetir, então aceita múltiplos usernames
        usernames = [u for u, n in user_map.items() if n == sel_user_name]
        if not usernames:
            # se não achou no map, pode ser que o df já tenha o próprio nome
            mask &= df.get("USUARIO_CADASTRO", pd.Series("", index=df.index)).astype(str).eq(sel_user_name)
        else:
            mask &= df.get("USUARIO_CADASTRO", pd.Series("", index=df.index)).astype(str).isin(usernames)

    # 2) Insumo (contém, case-insensitive) — pode trocar pra exato se preferir
    if f_insumo:
        mask &= df.get("INSUMO", pd.Series("", index=df.index)).astype(str).str.contains(f_insumo, case=False, regex=False)

    # 3) Código do produto (exato)
    if f_codigo:
        mask &= df.get("CODIGO_PRODUTO", pd.Series("", index=df.index)).astype(str).str.strip().eq(f_codigo.strip())

    # 4) Palavra-chave (contém)
    if f_palavra:
        mask &= df.get("PALAVRA_CHAVE", pd.Series("", index=df.index)).astype(str).str.contains(f_palavra, case=False, regex=False)

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

def insert_item(session: Session, item: dict[str, Any]) -> tuple[bool, str]:
    # Colunas que serão ligadas por parâmetro (sem as datas!)
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
        INSERT INTO BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_INSUMOS
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
        t = session.table("BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_INSUMOS")
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
      INSERT INTO BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_LOG_VALIDACAO
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
      INSERT INTO BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_LOG_REPROVACAO
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
      INSERT INTO BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_LOG_ATUALIZACAO
      (ITEM_ID, CODIGO_PRODUTO, COLUNAS_ALTERADAS, BEFORE_SNAPSHOT, AFTER_SNAPSHOT, ATUALIZADO_POR_USER, ATUALIZADO_POR_NOME)
      SELECT ?, ?, {_sql_array(colunas_alteradas)}, {_sql_json(before_obj)}, {_sql_json(after_obj)}, ?, ?
    """
    params = [
        item_id, codigo_produto,
        (user or {}).get("username"), (user or {}).get("name"),
    ]
    session.sql(sql, params=params).collect()
