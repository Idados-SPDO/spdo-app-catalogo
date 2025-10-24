import streamlit as st
import pandas as pd
from src.db_snowflake import apply_common_filters, build_user_options, get_session, load_user_display_map, log_atualizacao, fetch_row_snapshot
from src.auth import init_auth, is_authenticated, current_user
from src.utils import extrair_valores, gerar_sinonimo, gerar_palavra_chave
from src.variables import FQN_APR


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
    df = session.table(FQN_APR).to_pandas()
except Exception as e:
    st.error(f"Falha ao carregar aprovados: {e}")
    st.stop()

if df.empty:
    st.info("Nenhum item aprovado para atualizar.")
    st.stop()

# -------- Filtros --------
user_map = load_user_display_map(session)

st.subheader("Filtros")
c1, c2, c3, c4 = st.columns(4)
with c1:
    sel_user = st.selectbox("Usuário", build_user_options(df, user_map), index=0, key="atu_sel_user")
with c2:
    f_insumo = st.text_input("Insumo", key="atu_f_insumo")
with c3:
    f_codigo = st.text_input("Código do Produto", key="atu_f_codigo")
with c4:
    f_palavra = st.text_input("Palavra-chave", key="atu_f_palavra")

mask = apply_common_filters(
    df,
    sel_user_name=sel_user,
    f_insumo=f_insumo,
    f_codigo=f_codigo,
    f_palavra=f_palavra,
    user_map=user_map,
)

df_view = df[mask].copy()
# -------- Ordem exigida --------
ORDER_ATUALIZACAO = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","QTD_EMB_PRODUTO", "EMB_PRODUTO", "QTD_MED", "UN_MED", "QTD_EMB_COMERCIAL", "EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","REFERENCIA",
    "DATA_CADASTRO","USUARIO_CADASTRO",
    "DATA_APROVACAO","USUARIO_APROVACAO",  
    "DATA_ATUALIZACAO","USUARIO_ATUALIZACAO",
]
def reorder(df_in: pd.DataFrame, wanted: list[str]) -> pd.DataFrame:
    keep = [c for c in wanted if c in df_in.columns]
    rest = [c for c in df_in.columns if c not in keep]
    return df_in[keep + rest]
df_view = reorder(df_view, ORDER_ATUALIZACAO)



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

DT_COLS = ["DATA_CADASTRO", "DATA_APROVACAO"]
df_view = coerce_datetimes(df_view, DT_COLS)
dt_cfg  = build_datetime_column_config(df_view, DT_COLS)

if df_view.empty:
    st.info("Nenhum item encontrado com os filtros aplicados.")
    st.stop()

# -------- Editor --------
df_before = df_view.copy()

lock_cols = [
    "ID","DATA_CADASTRO","USUARIO_CADASTRO"
]
disabled_cols = [c for c in lock_cols if c in df_view.columns]
st.caption(f"Itens para validar no banco: **{len(df_view)}**")

edited = st.data_editor(
    df_view,
    num_rows="fixed",
    width="stretch",
    hide_index=True,
    disabled=disabled_cols,
    column_config={
        "QTD_MED": st.column_config.NumberColumn(format="%.2f"),
        "QTD_EMB_COMERCIAL": st.column_config.NumberColumn(format="%d"),
        **dt_cfg,
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
    table_name = FQN_APR
    usuario_atual = user["name"] if isinstance(user, dict) and "name" in user else None

    for key_val, cols_changed in changes:
        before = fetch_row_snapshot(session, FQN_APR, int(key_val)) if str(key_val).isdigit() else None
        
        deps_desc     = {"ESPECIFICACAO"}
        deps_sinonimo = {"ITEM","ESPECIFICACAO","MARCA","QTD_MED","UN_MED","EMB_PRODUTO","QTD_EMB_COMERCIAL","EMB_COMERCIAL","DESCRICAO"}
        deps_palavra  = {"SUBFAMILIA","ITEM","MARCA","EMB_PRODUTO","QTD_MED","UN_MED","FAMILIA"}

        changed = set(cols_changed)
        set_parts = []

        row_after = edited_by_key.loc[key_val]

        for c in cols_changed:
            set_parts.append(f'{c} = {sql_escape(edited_by_key.loc[key_val, c])}')
        if changed & deps_desc:
            novo_desc = extrair_valores(row_after.get("ESPECIFICACAO", ""))
            set_parts.append(f"DESCRICAO = {sql_escape(novo_desc)}")
            changed.add("DESCRICAO")
        else:
            # mantém o que está vindo do editor (se existir) ou recalcula por garantia
            novo_desc = row_after.get("DESCRICAO")
            if novo_desc is None and "ESPECIFICACAO" in edited.columns:
                novo_desc = extrair_valores(row_after.get("ESPECIFICACAO", ""))

        # 2.3: se qualquer dependência de SINONIMO mudou, recalcula
        if changed & deps_sinonimo:
            sinonimo_novo = gerar_sinonimo(
                row_after.get("ITEM"),
                novo_desc or row_after.get("DESCRICAO") or "",
                row_after.get("MARCA"),
                row_after.get("QTD_MED"),
                row_after.get("UN_MED"),
                row_after.get("EMB_PRODUTO"),
                row_after.get("QTD_EMB_COMERCIAL"),
                row_after.get("EMB_COMERCIAL"),
            )
            set_parts.append(f"SINONIMO = {sql_escape(sinonimo_novo)}")
            changed.add("SINONIMO")

        # 2.4: se qualquer dependência de PALAVRA_CHAVE mudou, recalcula
        if changed & deps_palavra:
            palavra_nova = gerar_palavra_chave(
                row_after.get("SUBFAMILIA"),
                row_after.get("ITEM"),
                row_after.get("MARCA"),
                row_after.get("EMB_PRODUTO"),
                row_after.get("QTD_MED"),
                row_after.get("UN_MED"),
                row_after.get("FAMILIA"),
            )
            set_parts.append(f"PALAVRA_CHAVE = {sql_escape(palavra_nova)}")
            changed.add("PALAVRA_CHAVE")
            
        # timestamps/usuário pelo banco (mais robusto)
        if "DATA_ATUALIZACAO" in edited.columns:
            set_parts.append("DATA_ATUALIZACAO = CURRENT_TIMESTAMP()")
        if "USUARIO_ATUALIZACAO" in edited.columns and usuario_atual:
            set_parts.append(f"USUARIO_ATUALIZACAO = {sql_escape(usuario_atual)}")

        set_clause = ", ".join(set_parts)
        where_clause = f"{key_col} = {sql_escape(key_val)}"
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

        try:
            session.sql(sql).collect()
            updated += 1
        except Exception as e:
            errors.append((key_val, str(e)))

        after = fetch_row_snapshot(session, FQN_APR, int(key_val)) if str(key_val).isdigit() else None
        try:
            log_atualizacao(
                session,
                item_id=int(key_val) if str(key_val).isdigit() else None,
                codigo_produto=str(edited_by_key.loc[key_val, "CODIGO_PRODUTO"]) if "CODIGO_PRODUTO" in edited_by_key.columns else None,
                colunas_alteradas=cols_changed,
                before_obj=before,
                after_obj=after,
                user=user,
            )
        except Exception:
            pass

    if errors:
        st.warning(f"Concluído com observações: {updated} linha(s) atualizada(s), {len(errors)} erro(s).")
        with st.expander("Ver erros"):
            for k, err in errors:
                st.write(f"{key_col}={k}: {err}")
    else:
        st.success(f"✅ {updated} linha(s) atualizada(s) com sucesso.")
