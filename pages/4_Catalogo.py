from io import BytesIO

from src.db_snowflake import (
    apply_common_filters,
    build_user_options,
    get_session,
    load_user_display_map,
)
from src.utils import order_catalogo
from src.auth import require_roles, current_user
from src.variables import FQN_APR


# =========================
# Config / Auth
# =========================
st.set_page_config(page_title="Catálogo • Lista", layout="wide")

require_roles("USER", "OPERACIONAL", "ADMIN")
user = current_user()
role = (user.get("role") or "USER").upper().strip()
is_user_role = role == "USER"

st.title("📚 Catálogo de Insumos")


# =========================
# Helpers
# =========================
ORDER_CATALOGO = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","QTD_EMB_PRODUTO", "EMB_PRODUTO", "QTD_MED", "UN_MED", "QTD_EMB_COMERCIAL", "EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","REFERENCIA",
    "DATA_CADASTRO","USUARIO_CADASTRO",
    "DATA_APROVACAO","USUARIO_APROVACAO",
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










USER_COLS_SPEC = [
    ("Código do Insumo",   "INSUMO"),
    ("ID FGV",             "ID"),
    ("EAN",                "CODIGO_PRODUTO"),
    ("Categoria",          "CATEGORIA"),
    ("Grupo de Insumo",    "FAMILIA"),
    ("Descrição",          "SINONIMO"),
    ("Marca",              "MARCA"),
    ("Fabricante",         "MARCA"),  # duplicado propositalmente
    ("Quantidade",         "QTD_MED"),
    ("Unidade de Medida",  "UN_MED"),
    ("Embalagem",          "EMB_PRODUTO"),
]

def build_user_view(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    out["Prioridade"] = pd.Series([pd.NA] * len(df), index=df.index, dtype="Int64")

    for new_name, snow_name in USER_COLS_SPEC:
        out[new_name] = df[snow_name] if snow_name in df.columns else pd.NA




    if "EAN" in out.columns:
        s = out["EAN"]
        out["EAN"] = (
            s.astype("string")
             .str.replace(r"\.0$", "", regex=True)
             .str.strip()
        )

    return out

def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "catalogo") -> bytes:
    df_x = df.copy()


    for c in df_x.columns:
        if pd.api.types.is_datetime64_any_dtype(df_x[c]):
            df_x[c] = df_x[c].dt.strftime("%d/%m/%Y %H:%M")

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df_x.to_excel(writer, index=False, sheet_name=sheet_name)
    return bio.getvalue()


# =========================
# Dados (tudo junto: com e sem INSUMO)
# =========================
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


df = reorder(df, ORDER_CATALOGO)

DT_COLS = ["DATA_CADASTRO", "DATA_APROVACAO", "DATA_VALIDACAO", "DATA_ATUALIZACAO"]
df = coerce_datetimes(df, DT_COLS)
dt_cfg = build_datetime_column_config(df, DT_COLS)

user_map = load_user_display_map(session)


# =========================
# Filtros (únicos)
# =========================
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


# =========================
# Tabela (única)
# =========================
st.caption(f"Itens no catálogo: **{len(df_filtrado)}**")

if is_user_role:
    df_display = build_user_view(df_filtrado).reset_index(drop=True)
    st.dataframe(df_display, hide_index=True, use_container_width=True)
else:
    df_display = df_filtrado.copy()
    st.dataframe(df_display, hide_index=True, use_container_width=True, column_config=dt_cfg)


# =========================
# Download XLSX
# =========================
xlsx_bytes = df_to_xlsx_bytes(df_display, sheet_name="catalogo_filtrado")
st.download_button(
    label="⬇️ Baixar XLSX",
    data=xlsx_bytes,
    file_name=f"catalogo_filtrado_{role.lower()}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True,
)
