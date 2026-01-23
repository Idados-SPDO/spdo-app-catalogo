import streamlit as st
import pandas as pd
from io import BytesIO
import hashlib
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
from src.db_snowflake import apply_common_filters, build_user_options, get_session, load_user_display_map
from src.utils import order_catalogo
from src.auth import require_roles, current_user
from src.variables import FQN_APR


# ===== Helpers =====
ORDER_CATALOGO = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","QTD_EMB_PRODUTO", "EMB_PRODUTO", "QTD_MED", "UN_MED", "QTD_EMB_COMERCIAL", "EMB_COMERCIAL",
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

# ===== Auth & page =====
require_roles("USER", "OPERACIONAL", "ADMIN")
user = current_user()
role = (user.get("role") or "USER").upper().strip()
is_user_role = role == "USER"

st.set_page_config(page_title="Catálogo • Lista", layout="wide")
st.title("📚 Catálogo de Insumos")

USER_COLS_SPEC = [
    ("Código do Insumo",   "INSUMO"),
    ("ID FGV",             "ID"),
    ("Grupo", "GRUPO"),
    ("Categoria", "CATEGORIA"),
    ("Segmento", "SEGMENTO"),
    ("Familia", "FAMILIA"),
    ("Subfamília", "SUBFAMILIA"),
    ("EAN",                "CODIGO_PRODUTO"),
    ("Categoria",          "CATEGORIA"),
    ("Descrição",          "SINONIMO"),
    ("Marca",              "MARCA"),
    ("Fabricante",         "MARCA"),        # duplicado propositalmente
    ("Quantidade",         "QTD_MED"),
    ("Unidade de Medida",  "UN_MED"),
    ("Embalagem",          "EMB_PRODUTO"),
]

def build_user_view(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    out["Prioridade"] = pd.Series([pd.NA] * len(df), index=df.index, dtype="Int64")

    for new_name, snow_name in USER_COLS_SPEC:
        if snow_name in df.columns:
            out[new_name] = df[snow_name]
        else:
            out[new_name] = pd.NA

    if "EAN" in out.columns:
        s = out["EAN"]
        out["EAN"] = (
            s.astype("string")
             .str.replace(r"\.0$", "", regex=True)
             .str.strip()
        )

    return out


KEY_SELECTED = "cat_selected_keys"
KEY_EDITOR = "cat_table_editor"
KEY_SELECT_ALL = "cat_select_all_visible"
KEY_VISIBLE_KEYS = "cat_visible_row_keys"

FILTER_KEYS = [
    "cat_sel_user",
    "cat_sel_insumo_dd",
    "cat_sel_codigo_dd",
    "cat_f_palavra",
    "cat_sel_grupo_dd",
    "cat_sel_categoria_dd",
    "cat_sel_segmento_dd",
    "cat_sel_familia_dd",
    "cat_sel_subfamilia_dd",
]

def reset_catalogo_page_state():
    for k in FILTER_KEYS + [KEY_SELECTED, KEY_EDITOR, KEY_SELECT_ALL, KEY_VISIBLE_KEYS]:
        st.session_state.pop(k, None)
    st.rerun()

def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "catalogo") -> bytes:
    df_x = df.copy()

    # Formata datetimes para Excel (evita problemas e deixa legível)
    for c in df_x.columns:
        if pd.api.types.is_datetime64_any_dtype(df_x[c]):
            df_x[c] = df_x[c].dt.strftime("%d/%m/%Y %H:%M")

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df_x.to_excel(writer, index=False, sheet_name=sheet_name)
    return bio.getvalue()

# ===== Dados =====
session = get_session()
try:
    df = session.table(FQN_APR).to_pandas()
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

c_f1, c_f2 = st.columns([1, 5])
with c_f1:
    if st.button("Limpar filtros", key="cat_btn_limpar_filtros"):
        reset_catalogo_page_state()

# Séries normalizadas (para opções e comparação)
s_insumo = norm_str_series(df["INSUMO"]) if "INSUMO" in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")
s_codigo = norm_str_series(df["CODIGO_PRODUTO"], drop_dot_zero=True) if "CODIGO_PRODUTO" in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")

s_grupo = norm_str_series(df["GRUPO"]) if "GRUPO" in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")
s_categoria = norm_str_series(df["CATEGORIA"]) if "CATEGORIA" in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")
s_segmento = norm_str_series(df["SEGMENTO"]) if "SEGMENTO" in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")
s_familia = norm_str_series(df["FAMILIA"]) if "FAMILIA" in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")
s_subfamilia = norm_str_series(df["SUBFAMILIA"]) if "SUBFAMILIA" in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")

# Opções
opt_insumo = dropdown_options(s_insumo)
opt_codigo = dropdown_options(s_codigo)

opt_grupo = dropdown_options(s_grupo)
opt_categoria = dropdown_options(s_categoria)
opt_segmento = dropdown_options(s_segmento)
opt_familia = dropdown_options(s_familia)
opt_subfamilia = dropdown_options(s_subfamilia)

# Linha 1 (4 colunas)
c1, c2, c3, c4 = st.columns(4)
with c1:
    sel_user = st.selectbox(
        "Usuário (cadastro)",
        build_user_options(df, user_map),
        index=0,
        key="cat_sel_user"
    )
with c2:
    sel_insumo = st.selectbox(
        "Insumo",
        opt_insumo,
        index=0,
        key="cat_sel_insumo_dd"
    )
    pass
with c3:
    sel_codigo = st.selectbox(
        "Código do Produto (exato)",
        opt_codigo,
        index=0,
        key="cat_sel_codigo_dd"
    )
    pass
with c4:
    f_palavra = st.text_input("Palavra-chave (contém)", key="cat_f_palavra")

mask = apply_common_filters(
    df,
    sel_user_name=sel_user,
    f_insumo="",
    f_codigo="",
    f_palavra=f_palavra,
    user_map=user_map,
)
df_scope = df[mask].copy()

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

# Linha 2 (4 colunas)
d1, d2, d3, d4 = st.columns(4)

with d1:
    s_grupo, opt_grupo = _series_and_opts(df_scope, "GRUPO")
    sel_grupo = _selectbox_with_reset("Grupo", opt_grupo, key="cat_sel_grupo_dd")
    df_scope = _apply_selected(df_scope, s_grupo, sel_grupo)

with d2:
    s_categoria, opt_categoria = _series_and_opts(df_scope, "CATEGORIA")
    sel_categoria = _selectbox_with_reset("Categoria", opt_categoria, key="cat_sel_categoria_dd")
    df_scope = _apply_selected(df_scope, s_categoria, sel_categoria)

with d3:
    s_segmento, opt_segmento = _series_and_opts(df_scope, "SEGMENTO")
    sel_segmento = _selectbox_with_reset("Segmento", opt_segmento, key="cat_sel_segmento_dd")
    df_scope = _apply_selected(df_scope, s_segmento, sel_segmento)

with d4:
    s_familia, opt_familia = _series_and_opts(df_scope, "FAMILIA")
    sel_familia = _selectbox_with_reset("Família", opt_familia, key="cat_sel_familia_dd")
    df_scope = _apply_selected(df_scope, s_familia, sel_familia)

# Linha 3 (Subfamília)
e1, e2, e3, e4 = st.columns(4)

with e1:
    s_subfamilia, opt_subfamilia = _series_and_opts(df_scope, "SUBFAMILIA")
    sel_subfamilia = _selectbox_with_reset("Subfamília", opt_subfamilia, key="cat_sel_subfamilia_dd")
    df_scope = _apply_selected(df_scope, s_subfamilia, sel_subfamilia)

# Aplica filtros dropdown (exatos)
mask = apply_dropdown_to_mask(mask, s_insumo, sel_insumo)
mask = apply_dropdown_to_mask(mask, s_codigo, sel_codigo)

mask = apply_dropdown_to_mask(mask, s_grupo, sel_grupo)
mask = apply_dropdown_to_mask(mask, s_categoria, sel_categoria)
mask = apply_dropdown_to_mask(mask, s_segmento, sel_segmento)
mask = apply_dropdown_to_mask(mask, s_familia, sel_familia)
mask = apply_dropdown_to_mask(mask, s_subfamilia, sel_subfamilia)

df_filtrado = df[mask].copy()

# ===== Tabela =====
# ===== Tabela =====
st.caption(f"Itens no catalogo: **{len(df_filtrado)}**")

# Inicializa seleção em memória
if KEY_SELECTED not in st.session_state:
    st.session_state[KEY_SELECTED] = set()
elif not isinstance(st.session_state[KEY_SELECTED], set):
    st.session_state[KEY_SELECTED] = set(st.session_state[KEY_SELECTED] or [])

selected_set = set(st.session_state[KEY_SELECTED])

# Chave única por linha
if "ID" in df_filtrado.columns:
    base_id = df_filtrado["ID"].astype("string").fillna("")
else:
    base_id = df_filtrado.index.astype("string")

row_key = (base_id + "|" + df_filtrado.index.astype(str)).astype("string")
current_keys = set(row_key.tolist())

# Guarda as chaves visíveis para o callback do toggle
st.session_state[KEY_VISIBLE_KEYS] = row_key.tolist()

def _toggle_all_visible():
    keys = set(st.session_state.get(KEY_VISIBLE_KEYS, []))
    sel = st.session_state.get(KEY_SELECTED, set())
    if not isinstance(sel, set):
        sel = set(sel or [])
    if st.session_state.get(KEY_SELECT_ALL, False):
        st.session_state[KEY_SELECTED] = sel | keys
    else:
        st.session_state[KEY_SELECTED] = sel - keys

# Barra acima da tabela (placeholder para o toggle)
b1, b2, b3 = st.columns([1.3, 2.5, 6])
with b1:
    if st.button("Recarregar tabela", key="cat_btn_reload"):
        st.rerun()
with b2:
    toggle_ph = st.empty()

# Monta DF exibido (USER vs demais)
if is_user_role:
    df_view = build_user_view(df_filtrado)
    col_cfg = {
        "Selecionada": st.column_config.CheckboxColumn("Selecionada", help="Marque para incluir no download.")
    }
else:
    df_view = df_filtrado.copy()
    col_cfg = {
        "Selecionada": st.column_config.CheckboxColumn("Selecionada", help="Marque para incluir no download.")
    }
    col_cfg.update(dt_cfg)

# Sempre cria a coluna Selecionada no índice 0
df_editor = df_view.copy()
df_editor.insert(
    0,
    "Selecionada",
    row_key.isin(pd.Series(list(selected_set), dtype="string")).to_numpy()
)

# Index = row_key (para mapear seleção)
df_editor.index = row_key

# Deixa tudo read-only, exceto Selecionada
disabled_cols = [c for c in df_editor.columns if c != "Selecionada"]

df_edited = st.data_editor(
    df_editor,
    use_container_width=True,
    hide_index=True,
    column_config=col_cfg,
    disabled=disabled_cols,
    num_rows="fixed",
    key=KEY_EDITOR,
)

# Atualiza seleção com base no que foi marcado manualmente na tabela
selected_now = set(df_edited.index[df_edited["Selecionada"]].tolist())

# Preserva seleções que não estão visíveis + aplica o estado atual visível
st.session_state[KEY_SELECTED] = (selected_set - current_keys) | selected_now
selected_set = set(st.session_state[KEY_SELECTED])

# Agora que a seleção foi atualizada, sincroniza o toggle (todos visíveis selecionados?)
all_visible_selected = (len(current_keys) > 0) and current_keys.issubset(selected_set)
st.session_state[KEY_SELECT_ALL] = all_visible_selected

# Renderiza o toggle no placeholder (fica acima da tabela)
with toggle_ph:
    st.toggle(
        "Selecionar todos (visíveis)",
        key=KEY_SELECT_ALL,
        on_change=_toggle_all_visible,
    )

# Selecionados (apenas do recorte atual)
selected_in_view = selected_set & current_keys
mask_sel = row_key.isin(list(selected_in_view))
df_selected_base = df_filtrado[mask_sel].copy()

st.caption(f"Selecionados (nesta tabela): **{len(df_selected_base)}**")

# DF para download segue perfil
if is_user_role:
    df_download = build_user_view(df_selected_base).reset_index(drop=True)
else:
    df_download = df_selected_base.reset_index(drop=True)

xlsx_bytes = df_to_xlsx_bytes(df_download, sheet_name="selecionados")

st.download_button(
    "Baixar itens selecionados",
    data=xlsx_bytes,
    file_name="catalogo_selecionados.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    disabled=df_selected_base.empty,
    key="cat_btn_download_selected",
)


