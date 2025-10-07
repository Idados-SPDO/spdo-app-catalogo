# catalogo.py
import streamlit as st
import pandas as pd
from src.db_snowflake import get_session, listar_itens_df
from src.utils import gerar_excel

from src.auth import init_auth, is_authenticated

init_auth()
if not is_authenticated():
    st.error("Faça login para continuar.")
    st.stop()

    
st.set_page_config(page_title="Catálogo • Lista", layout="wide")
st.title("📚 Catálogo de Insumos")

session = get_session()
df = listar_itens_df(session)

if df.empty:
    st.info("Nenhum item cadastrado ainda.")
    st.stop()

# ---- Reordena colunas: TIPO_PRODUTO logo após CODIGO_PRODUTO ----
def move_after(cols: list[str], col: str, after: str) -> list[str]:
    if col in cols and after in cols:
        cols.remove(col)
        idx = cols.index(after) + 1
        cols.insert(idx, col)
    return cols

cols = list(df.columns)
cols = move_after(cols, "TIPO_PRODUTO", "CODIGO_PRODUTO")
df = df.reindex(columns=cols, fill_value=None)

# ---------------------------
# Filtros (somente 3 campos)
# ---------------------------
ALL = "— Todos —"

def unique_opts(series: pd.Series):
    return [ALL] + sorted([str(x) for x in series.dropna().unique()])

c1, c2, c3 = st.columns(3)
with c1:
    f_codigo = st.text_input("Código do Produto (exato)")
with c2:
    tipos_opts = unique_opts(df.get("TIPO_PRODUTO", pd.Series(dtype=str)))
    sel_tipo = st.selectbox("Tipo do Produto", tipos_opts, index=0)
with c3:
    f_palavra = st.text_input("Palavras-chave (contém)")

# ---------------------------
# Aplicação dos filtros
# ---------------------------
mask = pd.Series(True, index=df.index)

if f_codigo:
    mask &= df.get("CODIGO_PRODUTO", pd.Series("", index=df.index)).astype(str).str.strip() == f_codigo.strip()

if sel_tipo != ALL:
    mask &= df.get("TIPO_PRODUTO", pd.Series("", index=df.index)).astype(str) == sel_tipo

if f_palavra:
    mask &= df.get("PALAVRA_CHAVE", pd.Series("", index=df.index)).astype(str).str.contains(f_palavra, case=False, regex=False)

df_filtrado = df[mask]

# ---------------------------
# Tabela + download
# ---------------------------
st.dataframe(df_filtrado, use_container_width=True, hide_index=True)

st.download_button(
    "⬇️ Baixar CSV",
    data=df_filtrado.to_csv(index=False).encode("utf-8"),
    file_name="catalogo_insumos.csv",
    mime="text/csv",
)

try:
    xlsx_bytes = gerar_excel(df_filtrado)
    if not isinstance(xlsx_bytes, (bytes, bytearray)):
        raise TypeError(f"'gerar_excel' deve retornar bytes, veio {type(xlsx_bytes)}")
except Exception as e:
    st.error(f"Falha ao gerar o XLSX: {e}")
    st.stop()

st.download_button(
    "⬇️ Baixar XLSX",
    data=xlsx_bytes,
    file_name="catalogo_insumos.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
