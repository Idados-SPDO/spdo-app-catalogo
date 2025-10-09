# pages/1_Catalogo.py
import streamlit as st
import pandas as pd
from src.db_snowflake import get_session
from src.utils import gerar_excel, order_catalogo
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

# ===== Filtros =====
ALL = "— Todos —"
def unique_opts(s: pd.Series):
    return [ALL] + sorted([str(x) for x in s.dropna().unique()])

c1, c2, c3 = st.columns(3)
with c1:
    f_codigo = st.text_input("Código do Produto (exato)", key="cat_f_codigo")
with c2:
    tipos_opts = unique_opts(df.get("TIPO_CODIGO", pd.Series(dtype=str)))
    sel_tipo = st.selectbox("Tipo do Código", tipos_opts, index=0, key="cat_sel_tipo")
with c3:
    f_palavra = st.text_input("Palavras-chave (contém)", key="cat_f_palavra")

mask = pd.Series(True, index=df.index)
if f_codigo:
    mask &= df.get("CODIGO_PRODUTO", pd.Series("", index=df.index)).astype(str).str.strip().eq(f_codigo.strip())
if sel_tipo != ALL:
    mask &= df.get("TIPO_CODIGO", pd.Series("", index=df.index)).astype(str).eq(sel_tipo)
if f_palavra:
    mask &= df.get("PALAVRA_CHAVE", pd.Series("", index=df.index)).astype(str).str.contains(f_palavra, case=False, regex=False)

df_filtrado = df[mask]

# ===== Tabela =====
st.dataframe(df_filtrado, use_container_width=True, hide_index=True)
