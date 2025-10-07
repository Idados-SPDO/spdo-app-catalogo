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