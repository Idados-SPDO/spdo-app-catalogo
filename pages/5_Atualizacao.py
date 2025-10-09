# pages/3_Atualizacao.py (ajustes principais)
import streamlit as st
import pandas as pd
from datetime import datetime
from src.db_snowflake import get_session
from src.utils import data_hoje  # você pode não usar mais se trocar por CURRENT_TIMESTAMP()
from src.auth import init_auth, is_authenticated, current_user

FQN_APROV = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_APROVADOS"  # <- fonte/destino

init_auth()
if not is_authenticated():
    st.error("Faça login para continuar.")
    st.stop()

st.set_page_config(page_title="Catálogo • Atualização", layout="wide")
st.title("🛠️ Atualização de Insumos")

user = current_user()
session = get_session()

# -------- Carrega apenas aprovados --------
try:
    df = session.table(FQN_APROV).to_pandas()
except Exception as e:
    st.error(f"Falha ao carregar aprovados: {e}")
    st.stop()

if df.empty:
    st.info("Nenhum item aprovado para atualizar.")
    st.stop()

# -------- Filtros --------
ALL = "— Todos —"
def unique_opts(series: pd.Series):
    return [ALL] + sorted([str(x) for x in series.dropna().unique()])

c1, c2, c3, c4 = st.columns(4)
with c1:
    f_codigo = st.text_input("Código do Produto (exato)")
with c2:
    tipo_opts = unique_opts(df.get("TIPO_CODIGO", pd.Series(dtype=str)))
    sel_tipo = st.selectbox("Tipo do Produto", tipo_opts, index=0)
with c3:
    insumo_opts = unique_opts(df.get("INSUMO", pd.Series(dtype=str)))
    sel_insumo = st.selectbox("Insumo", insumo_opts, index=0)
with c4:
    f_palavra = st.text_input("Palavra-chave (contém)")

mask = pd.Series(True, index=df.index)
if f_codigo:
    mask &= df.get("CODIGO_PRODUTO", pd.Series("", index=df.index)).astype(str).str.strip().eq(f_codigo.strip())
if sel_tipo != ALL:
    mask &= df.get("TIPO_CODIGO", pd.Series("", index=df.index)).astype(str).eq(sel_tipo)
if sel_insumo != ALL:
    mask &= df.get("INSUMO", pd.Series("", index=df.index)).astype(str).eq(sel_insumo)
if f_palavra:
    mask &= df.get("PALAVRA_CHAVE", pd.Series("", index=df.index)).astype(str).str.contains(f_palavra, case=False, regex=False)

df_view = df[mask].copy()

# -------- Ordem exigida --------
ORDER_ATUALIZACAO = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","REFERENCIA",
    "DATA_CADASTRO","USUARIO_CADASTRO",
    "DATA_APROVACAO","USUARIO_APROVACAO",        # novos
    "DATA_VALIDACAO","USUARIO_VALIDADOR",
    "DATA_ATUALIZACAO","USUARIO_ATUALIZACAO",
]
def reorder(df_in: pd.DataFrame, wanted: list[str]) -> pd.DataFrame:
    keep = [c for c in wanted if c in df_in.columns]
    rest = [c for c in df_in.columns if c not in keep]
    return df_in[keep + rest]
df_view = reorder(df_view, ORDER_ATUALIZACAO)

if df_view.empty:
    st.info("Nenhum item encontrado com os filtros aplicados.")
    st.stop()

# -------- Editor --------
df_before = df_view.copy()

lock_cols = [
    "ID","DATA_CADASTRO","USUARIO_CADASTRO","DATA_VALIDACAO","USUARIO_VALIDADOR",
    "DATA_ATUALIZACAO","USUARIO_ATUALIZACAO"
]
disabled_cols = [c for c in lock_cols if c in df_view.columns]

edited = st.data_editor(
    df_view,
    num_rows="fixed",
    use_container_width=True,
    hide_index=True,
    disabled=disabled_cols,
    column_config={
        "QTD_MED": st.column_config.NumberColumn(format="%.2f"),
        "QTD_EMB_COMERCIAL": st.column_config.NumberColumn(format="%d"),
    },
    key="editor_atualizacao"
)

# -------- Salvar --------
st.markdown("---")
if st.button("💾 Salvar alterações"):
    key_col = "ID" if "ID" in edited.columns else ("CODIGO_PRODUTO" if "CODIGO_PRODUTO" in edited.columns else None)
    if key_col is None:
        st.error("Não encontrei coluna chave (ID ou CODIGO_PRODUTO) para atualização.")
        st.stop()

    original_by_key = df_before.set_index(key_col)
    edited_by_key   = edited.set_index(key_col)
    common_keys = original_by_key.index.intersection(edited_by_key.index)

    changes = []
    for k in common_keys:
        before_row = original_by_key.loc[k]
        after_row  = edited_by_key.loc[k]
        diff_cols = []
        for col in edited.columns:
            if col in disabled_cols or col == key_col:
                continue
            b = before_row.get(col)
            a = after_row.get(col)
            if (pd.isna(b) and pd.isna(a)) or (b == a):
                continue
            diff_cols.append(col)
        if diff_cols:
            changes.append((k, diff_cols))

    if not changes:
        st.info("Nenhuma alteração detectada.")
        st.stop()

    updated, errors = 0, []

    def sql_escape(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "NULL"
        if isinstance(val, (int, float)):
            return str(val)
        return "'" + str(val).replace("'", "''") + "'"

    # Atualiza direto na tabela de APROVADOS
    table_name = FQN_APROV
    usuario_atual = user["username"] if isinstance(user, dict) and "username" in user else None

    for key_val, cols_changed in changes:
        set_parts = []
        for c in cols_changed:
            set_parts.append(f'{c} = {sql_escape(edited_by_key.loc[key_val, c])}')

        # timestamps/usuário pelo banco (mais robusto)
        if "DATA_ATUALIZACAO" in df.columns:
            set_parts.append("DATA_ATUALIZACAO = CURRENT_TIMESTAMP()")
        if "USUARIO_ATUALIZACAO" in df.columns and usuario_atual:
            set_parts.append(f"USUARIO_ATUALIZACAO = {sql_escape(usuario_atual)}")

        set_clause = ", ".join(set_parts)
        where_clause = f"{key_col} = {sql_escape(key_val)}"
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
