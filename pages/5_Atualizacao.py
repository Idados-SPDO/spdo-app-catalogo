import streamlit as st
import pandas as pd
from src.db_snowflake import apply_common_filters, build_user_options, get_session, load_user_display_map, log_atualizacao, fetch_row_snapshot
from src.auth import require_roles, current_user
from src.utils import extrair_valores, gerar_sinonimo, gerar_palavra_chave
from src.variables import FQN_APR


require_roles("ADMIN")

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
ALL_LABEL = "Todos"
NULL_LABEL = "(vazio)"
def norm_str_series(s: pd.Series, *, drop_dot_zero: bool = False) -> pd.Series:
    """
    Normaliza valores para filtros:
    - converte para string
    - remove .0 no final (útil p/ EAN vindo como float)
    - strip
    - vazio -> NA
    """
    s = s.astype("string")
    if drop_dot_zero:
        s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.strip()
    s = s.replace(["", "nan", "NaN", "None"], pd.NA)
    return s

def dropdown_options(s_norm: pd.Series, *, all_label: str = ALL_LABEL, null_label: str = NULL_LABEL) -> list[str]:
    opts = [all_label]
    if s_norm.isna().any():
        opts.append(null_label)

    uniq = pd.Series(pd.unique(s_norm.dropna())).astype("string")
    uniq = uniq[uniq.str.len() > 0].sort_values()
    opts.extend(uniq.tolist())
    return opts

def apply_dropdown_to_mask(
    mask: pd.Series,
    s_norm: pd.Series,
    selected: str,
    *,
    all_label: str = ALL_LABEL,
    null_label: str = NULL_LABEL
) -> pd.Series:
    if selected == all_label:
        return mask
    if selected == null_label:
        return mask & s_norm.isna()
    return mask & (s_norm == selected)


FILTER_KEYS_UPD = [
    "upd_f_id",
    "upd_sel_user",
    "upd_sel_insumo_dd",
    "upd_sel_codigo_dd",
    "upd_f_palavra",
    "upd_sel_grupo_dd",
    "upd_sel_categoria_dd",
    "upd_sel_segmento_dd",
    "upd_sel_familia_dd",
    "upd_sel_subfamilia_dd",
    "editor_atualizacao",  # opcional, mas recomendado para “zerar” o data_editor
]

def reset_filters_upd():
    for k in FILTER_KEYS_UPD:
        st.session_state.pop(k, None)
    st.rerun()
    
def _series_and_opts(df_in: pd.DataFrame, col: str, *, drop_dot_zero: bool = False):
    if col in df_in.columns:
        s = norm_str_series(df_in[col], drop_dot_zero=drop_dot_zero)
    else:
        s = pd.Series(pd.NA, index=df_in.index, dtype="string")
    return s, dropdown_options(s)

def _apply_selected(df_in: pd.DataFrame, s_norm: pd.Series, selected: str) -> pd.DataFrame:
    if selected == ALL_LABEL:
        return df_in
    if selected == NULL_LABEL:
        return df_in[s_norm.isna()]
    return df_in[s_norm == selected]

def _selectbox_with_reset(label: str, options: list[str], key: str) -> str:
    cur = st.session_state.get(key, ALL_LABEL)
    if cur not in options:
        st.session_state[key] = ALL_LABEL
        cur = ALL_LABEL
    return st.selectbox(label, options, index=options.index(cur), key=key)

user_map = load_user_display_map(session)

st.subheader("Filtros")
if st.button("🧹 Limpar filtros", key="upd_btn_clear"):
    reset_filters_upd()
   
# Séries normalizadas (para opções e comparação) — DF completo
s_insumo = norm_str_series(df["INSUMO"]) if "INSUMO" in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")
s_codigo = norm_str_series(df["CODIGO_PRODUTO"], drop_dot_zero=True) if "CODIGO_PRODUTO" in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")

opt_insumo = dropdown_options(s_insumo)
opt_codigo = dropdown_options(s_codigo)

# =========================
# Linha 1 (4 filtros)
# ID | Usuário | Insumo | Código
# =========================
r1 = st.columns(4)
with r1[0]:
    sel_id = st.text_input("ID", key="upd_f_id")
with r1[1]:
    sel_user = st.selectbox(
        "Usuário (cadastro)",
        build_user_options(df, user_map),
        index=0,
        key="upd_sel_user",
    )
with r1[2]:
    sel_insumo = st.selectbox(
        "Insumo",
        opt_insumo,
        index=0,
        key="upd_sel_insumo_dd",
    )
with r1[3]:
    sel_codigo = st.selectbox(
        "Código do Produto (exato)",
        opt_codigo,
        index=0,
        key="upd_sel_codigo_dd",
    )

# =========================
# Linha 2 (4 filtros)
# Palavra-chave | Grupo | Categoria | Segmento
# =========================
r2 = st.columns(4)
with r2[0]:
    f_palavra = st.text_input("Palavra-chave (contém)", key="upd_f_palavra")

# 1) mask base (usuário + palavra)
mask = apply_common_filters(
    df,
    sel_user_name=sel_user,
    f_insumo="",
    f_codigo="",
    f_palavra=f_palavra,
    user_map=user_map,
)

# 2) aplica Insumo/Código exatos no mask (alinhado ao DF completo)
mask = apply_dropdown_to_mask(mask, s_insumo, sel_insumo)
mask = apply_dropdown_to_mask(mask, s_codigo, sel_codigo)

# 3) aplica filtro de ID (exato)
sel_id_norm = (sel_id or "").strip()
if sel_id_norm:
    if "ID" in df.columns and sel_id_norm.isdigit():
        mask = mask & (df["ID"].astype("Int64") == int(sel_id_norm))
    else:
        st.warning("ID inválido. Use um número inteiro.")
        mask = mask & False

# 4) escopo inicial para cascata
df_scope = df[mask].copy()

with r2[1]:
    s_grupo_sc, opt_grupo_sc = _series_and_opts(df_scope, "GRUPO")
    sel_grupo = _selectbox_with_reset("Grupo", opt_grupo_sc, key="upd_sel_grupo_dd")
    df_scope = _apply_selected(df_scope, s_grupo_sc, sel_grupo)

with r2[2]:
    s_cat_sc, opt_cat_sc = _series_and_opts(df_scope, "CATEGORIA")
    sel_categoria = _selectbox_with_reset("Categoria", opt_cat_sc, key="upd_sel_categoria_dd")
    df_scope = _apply_selected(df_scope, s_cat_sc, sel_categoria)

with r2[3]:
    s_seg_sc, opt_seg_sc = _series_and_opts(df_scope, "SEGMENTO")
    sel_segmento = _selectbox_with_reset("Segmento", opt_seg_sc, key="upd_sel_segmento_dd")
    df_scope = _apply_selected(df_scope, s_seg_sc, sel_segmento)

# =========================
# Linha 3 (máx 4 filtros)
# Família | Subfamília | (vazio) | (vazio)
# =========================
r3 = st.columns(4)
with r3[0]:
    s_fam_sc, opt_fam_sc = _series_and_opts(df_scope, "FAMILIA")
    sel_familia = _selectbox_with_reset("Família", opt_fam_sc, key="upd_sel_familia_dd")
    df_scope = _apply_selected(df_scope, s_fam_sc, sel_familia)

with r3[1]:
    s_sub_sc, opt_sub_sc = _series_and_opts(df_scope, "SUBFAMILIA")
    sel_subfamilia = _selectbox_with_reset("Subfamília", opt_sub_sc, key="upd_sel_subfamilia_dd")
    df_scope = _apply_selected(df_scope, s_sub_sc, sel_subfamilia)

with r3[2]:
    st.empty()
with r3[3]:
    st.empty()

# Resultado final já filtrado pela cascata
df_view = df_scope

# Aplica filtros dropdown (exatos)
mask = apply_dropdown_to_mask(mask, s_insumo, sel_insumo)
mask = apply_dropdown_to_mask(mask, s_codigo, sel_codigo)

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

if st.button("Recarregar tabela"):
    st.rerun()
     
edited = st.data_editor(
    df_view,
    num_rows="fixed",
    width="stretch",
    hide_index=True,
    disabled=disabled_cols,
    column_config={
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
