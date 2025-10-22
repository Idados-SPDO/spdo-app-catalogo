# pages/1_Catalogo.py
import streamlit as st
import pandas as pd
from src.db_snowflake import apply_common_filters, build_user_options, get_session, load_user_display_map
from src.utils import order_catalogo
from src.auth import init_auth, is_authenticated

FQN_APROV = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_APROVADOS"

# ===== Helpers =====
ORDER_CATALOGO = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","REFERENCIA",
    "DATA_CADASTRO","USUARIO_CADASTRO",
    "DATA_APROVACAO","USUARIO_APROVACAO",        # novos
    "DATA_VALIDACAO","USUARIO_VALIDADOR",
    "DATA_ATUALIZACAO","USUARIO_ATUALIZACAO",
]

def reorder(df: pd.DataFrame, wanted: list[str]) -> pd.DataFrame:
    keep = [c for c in wanted if c in df.columns]
    rest = [c for c in df.columns if c not in keep]
    return df[keep + rest]

def coerce_datetimes(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=False)
            try:
                # Se vier timezone-aware, remove o tz para exibir como local
                if getattr(df[c].dt, "tz", None) is not None:
                    df[c] = df[c].dt.tz_localize(None)
            except Exception:
                pass
    return df

def build_datetime_column_config(df: pd.DataFrame, cols: list[str]) -> dict:
    cfg = {}
    for c in cols:
        if c in df.columns and pd.api.types.is_datetime64_any_dtype(df[c]):
            cfg[c] = st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm", disabled=True)
    return cfg

# ===== Auth & page =====
init_auth()
if not is_authenticated():
    st.error("Faça login para continuar.")
    st.stop()

st.set_page_config(page_title="Catálogo • Lista", layout="wide")
st.title("📚 Catálogo de Insumos")

# ===== Dados =====
session = get_session()
try:
    df = session.table(FQN_APROV).to_pandas()
    df = order_catalogo(df)
except Exception as e:
    st.error(f"Falha ao carregar aprovados: {e}")
    st.stop()

if df.empty:
    st.info("Nenhum item aprovado ainda.")
    st.stop()

# Ordenação exigida
df = reorder(df, ORDER_CATALOGO)
DT_COLS = ["DATA_CADASTRO", "DATA_APROVACAO", "DATA_VALIDACAO", "DATA_ATUALIZACAO"]
df = coerce_datetimes(df, DT_COLS)
dt_cfg = build_datetime_column_config(df, DT_COLS)
user_map = load_user_display_map(session)

st.subheader("Filtros")
c1, c2, c3, c4 = st.columns(4)
with c1:
    sel_user = st.selectbox("Usuário (cadastro)", build_user_options(df, user_map), index=0, key="cat_sel_user")
with c2:
    f_insumo = st.text_input("Insumo (contém)", key="cat_f_insumo")
with c3:
    f_codigo = st.text_input("Código do Produto (exato)", key="cat_f_codigo")
with c4:
    f_palavra = st.text_input("Palavra-chave (contém)", key="cat_f_palavra")

mask = apply_common_filters(
    df,
    sel_user_name=sel_user,
    f_insumo=f_insumo,
    f_codigo=f_codigo,
    f_palavra=f_palavra,
    user_map=user_map,
)

df_filtrado = df[mask].copy()

# ===== Tabela =====
st.caption(f"Itens no catalogo: **{len(df_filtrado)}**")

st.dataframe(df_filtrado, width="stretch", hide_index=True,  column_config=dt_cfg)
