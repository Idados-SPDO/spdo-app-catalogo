from __future__ import annotations
import pandas as pd
from typing import Any
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import os, hashlib
from typing import Optional, Dict, Any
# =========================
# Conexão
# =========================
FQN_USERS = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_USERS"

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
    cols = [
        "REFERENCIA","DATA_CADASTRO","DATA_ATUALIZACAO",
        "GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
        "CODIGO_PRODUTO","TIPO_PRODUTO",  # <— aqui
        "INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
        "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL","SINONIMO","PALAVRA_CHAVE"
    ]

    placeholders = ", ".join([f":{i+1}" for i in range(len(cols))])
    sql = f"INSERT INTO TB_CATALOGO_INSUMOS ({', '.join(cols)}) VALUES ({placeholders})"
    try:
        session.sql(sql, list(item.get(c) for c in cols)).collect()
        return True, "Item salvo com sucesso."
    except Exception as e:
        msg = str(e)
        if "unique" in msg.lower():
            return False, "CODIGO_PRODUTO ou INSUMO já cadastrado."
        return False, f"Erro ao salvar item: {e}"


def listar_itens_df(session: Session) -> pd.DataFrame:
    try:
        return session.table("BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_INSUMOS").sort("DATA_CADASTRO", ascending=False).to_pandas()
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