from __future__ import annotations
import pandas as pd
from typing import Any
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

# =========================
# Conexão
# =========================

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

DDL = """
CREATE TABLE IF NOT EXISTS TB_CATALOGO_INSUMOS (
  ID                 NUMBER AUTOINCREMENT START 1 INCREMENT 1,
  REFERENCIA         VARCHAR,
  DATA_CADASTRO      VARCHAR,
  DATA_ATUALIZACAO   VARCHAR,
  GRUPO              VARCHAR,
  CATEGORIA          VARCHAR,
  SEGMENTO           VARCHAR,
  FAMILIA            VARCHAR,
  SUBFAMILIA         VARCHAR,
  EAN_PRODUTO        VARCHAR UNIQUE,
  INSUMO             VARCHAR UNIQUE,
  ITEM               VARCHAR,
  DESCRICAO          VARCHAR,
  ESPECIFICACAO      VARCHAR,
  MARCA              VARCHAR,
  EMB_PRODUTO        VARCHAR,
  UN_MED             VARCHAR,
  QTD_MED            FLOAT,
  EMB_COMERCIAL      VARCHAR,
  QTD_EMB_COMERCIAL  NUMBER,
  CONSTRAINT PK_CATALOGO PRIMARY KEY (ID)
);
"""


def ensure_table(session: Session) -> None:
    session.sql(DDL).collect()


def insert_item(session: Session, item: dict[str, Any]) -> tuple[bool, str]:
    cols = [
        "REFERENCIA","DATA_CADASTRO","DATA_ATUALIZACAO",
        "GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
        "EAN_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
        "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL"
    ]
    placeholders = ", ".join([f":{i+1}" for i in range(len(cols))])
    sql = f"INSERT INTO TB_CATALOGO_INSUMOS ({', '.join(cols)}) VALUES ({placeholders})"
    try:
        session.sql(sql, list(item.get(c) for c in cols)).collect()
        return True, "Item salvo com sucesso."
    except Exception as e:
        msg = str(e)
        if "unique" in msg.lower():
            return False, "EAN_PRODUTO ou INSUMO já cadastrado."
        return False, f"Erro ao salvar item: {e}"


def listar_itens_df(session: Session) -> pd.DataFrame:
    try:
        return session.table("TB_CATALOGO_INSUMOS").sort("DATA_CADASTRO", ascending=False).to_pandas()
    except Exception:
        return pd.DataFrame()