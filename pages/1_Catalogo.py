import streamlit as st
import pandas as pd
from src.db_snowflake import get_session
from src.utils import gerar_excel
from src.auth import init_auth, is_authenticated

FQN_APROV = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_APROVADOS"

init_auth()
if not is_authenticated():
    st.error("Faça login para continuar.")
    st.stop()

st.set_page_config(page_title="Catálogo • Lista", layout="wide")
st.title("📚 Catálogo de Insumos (Aprovados)")

session = get_session()
try:
    # carrega somente os aprovados
    df = session.table(FQN_APROV).to_pandas()
except Exception as e:
    st.error(f"Falha ao carregar aprovados: {e}")
    st.stop()

if df.empty:
    st.info("Nenhum item aprovado ainda.")
    st.stop()

# mover TIPO_PRODUTO após CODIGO_PRODUTO
def move_after(cols: list[str], col: str, after: str) -> list[str]:
    if col in cols and after in cols:
        cols.remove(col)
        cols.insert(cols.index(after) + 1, col)
    return cols

cols = move_after(list(df.columns), "TIPO_PRODUTO", "CODIGO_PRODUTO")
df = df.reindex(columns=cols, fill_value=None)

# --- filtros
ALL = "— Todos —"
def unique_opts(s: pd.Series):
    return [ALL] + sorted([str(x) for x in s.dropna().unique()])

c1, c2, c3 = st.columns(3)
with c1:
    f_codigo = st.text_input("Código do Produto (exato)", key="cat_f_codigo")
with c2:
    tipos_opts = unique_opts(df.get("TIPO_PRODUTO", pd.Series(dtype=str)))
    sel_tipo = st.selectbox("Tipo do Produto", tipos_opts, index=0, key="cat_sel_tipo")
with c3:
    f_palavra = st.text_input("Palavras-chave (contém)", key="cat_f_palavra")

mask = pd.Series(True, index=df.index)
if f_codigo:
    mask &= df.get("CODIGO_PRODUTO", pd.Series("", index=df.index)).astype(str).str.strip().eq(f_codigo.strip())
if sel_tipo != ALL:
    mask &= df.get("TIPO_PRODUTO", pd.Series("", index=df.index)).astype(str).eq(sel_tipo)
if f_palavra:
    mask &= df.get("PALAVRA_CHAVE", pd.Series("", index=df.index)).astype(str).str.contains(f_palavra, case=False, regex=False)

df_filtrado = df[mask]

st.dataframe(df_filtrado, use_container_width=True, hide_index=True)

st.download_button(
    "⬇️ Baixar CSV",
    data=df_filtrado.to_csv(index=False).encode("utf-8"),
    file_name="catalogo_insumos.csv",
    mime="text/csv",
)

try:
    xlsx_bytes = gerar_excel(df_filtrado)
except Exception as e:
    st.error(f"Falha ao gerar o XLSX: {e}")
else:
    st.download_button(
        "⬇️ Baixar XLSX",
        data=xlsx_bytes,
        file_name="catalogo_insumos.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
