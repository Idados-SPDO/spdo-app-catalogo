import streamlit as st
import pandas as pd
from datetime import datetime
from src.db_snowflake import get_session, listar_itens_df

st.set_page_config(page_title="Catálogo • Atualização", layout="wide")
st.title("🛠️ Atualização de Insumos")

# ---------------------------
# Carrega dados
# ---------------------------
session = get_session()
df = listar_itens_df(session)

if df.empty:
    st.info("Nenhum item cadastrado ainda.")
    st.stop()

# ---------------------------
# Filtros (iguais ao catálogo)
# ---------------------------
with st.expander("🔎 Filtros"):
    ALL = "— Todos —"

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

# Aplica filtros
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

df_view = df[mask].copy()

if df_view.empty:
    st.info("Nenhum item encontrado com os filtros aplicados.")
    st.stop()

# ---------------------------
# Editor in-line
# ---------------------------
# ordena colunas (opcional)
preferred = [
    "ID","DATA_CADASTRO","DATA_ATUALIZACAO",
    "GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "INSUMO","ITEM","MARCA","EAN_PRODUTO",
    "EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
    "DESCRICAO","ESPECIFICACAO","REFERENCIA"
]
cols = [c for c in preferred if c in df_view.columns] + [c for c in df_view.columns if c not in preferred]
df_view = df_view[cols]

# guardamos original para diff
df_before = df_view.copy()

# configura colunas não-editáveis
disabled_cols = [c for c in ["ID","DATA_CADASTRO","DATA_ATUALIZACAO"] if c in df_view.columns]

edited = st.data_editor(
    df_view,
    num_rows="dynamic",
    use_container_width=True,
    hide_index=True,
    disabled=disabled_cols,
    column_config={
        "QTD_MED": st.column_config.NumberColumn(format="%.2f"),
        "QTD_EMB_COMERCIAL": st.column_config.NumberColumn(format="%d"),
    },
    key="editor_atualizacao"
)

# ---------------------------
# Botão salvar alterações
# ---------------------------
st.markdown("---")
if st.button("💾 Salvar alterações"):
    # identifica linhas alteradas comparando com df_before (mesmo índice do editor é posicional)
    # como st.data_editor retorna novo df com índice 0..n-1, vamos fazer merge por uma coluna chave
    # preferimos usar ID (PK). Se não houver ID, pode trocar para EAN_PRODUTO (UNIQUE).
    key_col = "ID" if "ID" in edited.columns else ("EAN_PRODUTO" if "EAN_PRODUTO" in edited.columns else None)

    if key_col is None:
        st.error("Não encontrei coluna chave (ID ou EAN_PRODUTO) para atualização.")
        st.stop()

    # cria mapa original por chave
    original_by_key = df_before.set_index(key_col)
    edited_by_key = edited.set_index(key_col)

    # alinha chaves presentes em ambos (ignore novas linhas adicionadas no editor)
    common_keys = original_by_key.index.intersection(edited_by_key.index)

    changes = []
    for k in common_keys:
        before_row = original_by_key.loc[k]
        after_row = edited_by_key.loc[k]

        # detecta diferenças coluna a coluna
        diff_cols = []
        for col in edited.columns:
            if col in disabled_cols or col == key_col:
                continue
            b = before_row.get(col)
            a = after_row.get(col)
            # considera NaN == NaN
            if (pd.isna(b) and pd.isna(a)) or (b == a):
                continue
            diff_cols.append(col)

        if diff_cols:
            changes.append((k, diff_cols))

    if not changes:
        st.info("Nenhuma alteração detectada.")
        st.stop()

    # executa UPDATE por linha alterada
    updated = 0
    errors = []

    def sql_escape(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "NULL"
        if isinstance(val, (int, float)):
            return str(val)
        # string/date -> escapa aspas simples
        return "'" + str(val).replace("'", "''") + "'"

    today = datetime.now().strftime("%Y-%m-%d")

    for key_val, cols_changed in changes:
        set_parts = []
        for c in cols_changed:
            set_parts.append(f'{c} = {sql_escape(edited_by_key.loc[key_val, c])}')
        # sempre atualiza DATA_ATUALIZACAO
        if "DATA_ATUALIZACAO" in df.columns:
            set_parts.append(f"DATA_ATUALIZACAO = '{today}'")

        set_clause = ", ".join(set_parts)

        # monta WHERE
        where_clause = f"{key_col} = {sql_escape(key_val)}"

        # nome da tabela — se você tiver um helper para isso, substitua aqui
        table_name = "TB_CATALOGO_INSUMOS"

        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

        try:
            session.sql(sql).collect()
            updated += 1
        except Exception as e:
            errors.append((key_val, str(e)))

    if errors:
        st.warning(f"Concluído com observações: {updated} linha(s) atualizada(s), {len(errors)} erro(s).")
        with st.expander("Ver erros"):
            for k, err in errors:
                st.write(f"{key_col}={k}: {err}")
    else:
        st.success(f"✅ {updated} linha(s) atualizada(s) com sucesso.")
