import streamlit as st
import pandas as pd
from src.db_snowflake import get_session, listar_itens_df

st.set_page_config(page_title="Catálogo • Lista", layout="wide")
st.title("📚 Catálogo de Insumos")

session = get_session()
df = listar_itens_df(session)

if df.empty:
    st.info("Nenhum item cadastrado ainda.")
    st.stop()

# ---------------------------
# Filtros
# ---------------------------
with st.expander("🔎 Filtros"):
    # Opção padrão para "sem filtro"
    ALL = "— Todos —"

    # Opções únicas (ordenadas) para dropdowns
    def unique_opts(series: pd.Series):
        return [ALL] + sorted([str(x) for x in series.dropna().unique()])

    grupos_opts     = unique_opts(df.get("GRUPO", pd.Series(dtype=str)))
    categorias_opts = unique_opts(df.get("CATEGORIA", pd.Series(dtype=str)))
    itens_opts      = unique_opts(df.get("ITEM", pd.Series(dtype=str)))

    c1, c2, c3 = st.columns(3)
    with c1:
        sel_grupo = st.selectbox("Grupo", grupos_opts, index=0)
    with c2:
        sel_categoria = st.selectbox("Categoria", categorias_opts, index=0)
    with c3:
        sel_item = st.selectbox("Item", itens_opts, index=0)

    c4, c5, c6 = st.columns(3)
    with c4:
        f_ean = st.text_input("EAN_PRODUTO (exato)")
    with c5:
        f_desc = st.text_input("Descrição contém")
    with c6:
        f_espec = st.text_input("Especificação contém")

    # Botão para limpar filtros de texto rapidamente
    if st.button("🧹 Limpar filtros"):
        st.rerun()

# ---------------------------
# Aplicação dos filtros
# ---------------------------
mask = pd.Series(True, index=df.index)

if sel_grupo != ALL:
    mask &= (df["GRUPO"].astype(str) == sel_grupo)

if sel_categoria != ALL:
    mask &= (df["CATEGORIA"].astype(str) == sel_categoria)

if sel_item != ALL:
    mask &= (df["ITEM"].astype(str) == sel_item)

if f_ean:
    mask &= (df.get("EAN_PRODUTO", pd.Series("", index=df.index)).astype(str).str.strip() == f_ean.strip())

if f_desc:
    mask &= df.get("DESCRICAO", pd.Series("", index=df.index)).astype(str).str.contains(f_desc, case=False, regex=False)

if f_espec:
    mask &= df.get("ESPECIFICACAO", pd.Series("", index=df.index)).astype(str).str.contains(f_espec, case=False, regex=False)

df = df[mask]

# ---------------------------
# Tabela + download
# ---------------------------
st.dataframe(df, use_container_width=True, hide_index=True)

st.download_button(
    "⬇️ Baixar CSV",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name="catalogo_insumos.csv",
    mime="text/csv",
)
