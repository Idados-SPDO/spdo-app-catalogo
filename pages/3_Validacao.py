import streamlit as st
import pandas as pd

from src.db_snowflake import (
    apply_common_filters,
    build_user_options,
    get_session,
    listar_itens_df,
    load_user_display_map,
    log_validacao,
    log_reprovacao,
)
from src.auth import init_auth, current_user, require_roles
from src.utils import extrair_valores, gerar_sinonimo
from src.variables import FQN_MAIN, FQN_COR, FQN_APR


# ==============================
# Constantes / Config
# ==============================
ORDER_VALIDACAO = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","QTD_EMB_PRODUTO", "EMB_PRODUTO", "QTD_MED", "UN_MED", "QTD_EMB_COMERCIAL", "EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","DATA_CADASTRO","USUARIO_CADASTRO","REFERENCIA",
]

# (mantido caso você ainda use em outras partes / futuras evoluções)
ORDER_CORRECOES = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","QTD_EMB_PRODUTO", "EMB_PRODUTO", "QTD_MED", "UN_MED", "QTD_EMB_COMERCIAL", "EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","REFERENCIA",
    "DATA_CADASTRO","USUARIO_CADASTRO",
    "DATA_REPROVACAO","USUARIO_REPROVACAO",
    "MOTIVO",
]

EDITABLE_COR_COLS = [
    "GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA","TIPO_CODIGO","CODIGO_PRODUTO",
    "INSUMO","ITEM","DESCRICAO","ESPECIFICACAO","MARCA","QTD_EMB_PRODUTO",
    "EMB_PRODUTO", "QTD_MED", "UN_MED", "QTD_EMB_COMERCIAL", "EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","REFERENCIA"
]


# ==============================
# Helpers
# ==============================
def sql_str(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "NULL"
    s = str(v).replace("'", "''")
    return f"'{s}'"

def reorder(df: pd.DataFrame, wanted: list[str], prepend: list[str] | None = None) -> pd.DataFrame:
    prepend = prepend or []
    keep = [c for c in wanted if c in df.columns]
    rest = [c for c in df.columns if c not in (set(prepend) | set(keep))]
    return df[[*(c for c in prepend if c in df.columns), *keep, *rest]]

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
        if c not in df.columns:
            continue
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            cfg[c] = st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm", disabled=True)
        else:
            cfg[c] = st.column_config.TextColumn(disabled=True)
    return cfg

def _sql_escape(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + str(val).replace("'", "''") + "'"

def _build_desc(row_after) -> str:
    """
    Recria DESCRICAO a partir de ESPECIFICACAO, seguindo 5_Atualizacao.
    - Se DESCRICAO está vazio/nulo, tenta extrair de ESPECIFICACAO
    """
    desc = row_after.get("DESCRICAO")
    if (desc is None or str(desc).strip() == "") and "ESPECIFICACAO" in row_after:
        return extrair_valores(row_after.get("ESPECIFICACAO", "") or "")
    return desc if desc is not None else ""

def _build_sinonimo_like_update(row_after) -> str:
    """
    Recalcula SINONIMO com a MESMA assinatura usada na 5_Atualizacao.
    """
    desc = _build_desc(row_after)
    return gerar_sinonimo(
        row_after.get("ITEM"),
        desc or row_after.get("DESCRICAO") or "",
        row_after.get("MARCA"),
        row_after.get("QTD_MED"),
        row_after.get("UN_MED"),
        row_after.get("EMB_PRODUTO"),
        row_after.get("QTD_EMB_COMERCIAL"),
        row_after.get("EMB_COMERCIAL"),
    )

def _recalc_sinonimo_df_inplace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recalcula DESCRICAO (se vazia) e SINONIMO para TODAS as linhas visíveis no DataFrame.
    Retorna o próprio df (mutado) para encadear.
    """
    if "SINONIMO" not in df.columns:
        df["SINONIMO"] = ""
    if "DESCRICAO" not in df.columns:
        df["DESCRICAO"] = ""

    def _calc(row):
        row_after = row.to_dict()
        novo_desc = _build_desc(row_after)
        row_after["DESCRICAO"] = novo_desc
        return novo_desc, _build_sinonimo_like_update(row_after)

    out = df.apply(lambda r: pd.Series(_calc(r), index=["__DESC_NEW__", "__SIN_NEW__"]), axis=1)
    mask_apply_desc = df["DESCRICAO"].astype(str).str.strip().eq("") | df["DESCRICAO"].isna()
    df.loc[mask_apply_desc, "DESCRICAO"] = out["__DESC_NEW__"]
    df["SINONIMO"] = out["__SIN_NEW__"]
    return df

def _persist_sinonimo_batch(session, table_fqn: str, df_ids: pd.DataFrame, id_col: str = "ID"):
    """
    Atualiza no banco em lote:
      - Atualiza DESCRICAO (apenas quando calculada nova)
      - Atualiza SINONIMO (sempre com o valor recalculado)
    Usa CASE ... WHEN ... THEN ... para eficiência.
    """
    if df_ids.empty or id_col not in df_ids or "SINONIMO" not in df_ids:
        return

    work = df_ids[[id_col, "SINONIMO"]].copy()
    has_desc = "DESCRICAO" in df_ids.columns
    if has_desc:
        work["__DESC_APPLY__"] = df_ids["DESCRICAO"]

    pairs = []
    for _, r in work.iterrows():
        _id = int(r[id_col])
        _sin = "" if pd.isna(r["SINONIMO"]) else str(r["SINONIMO"])
        _desc = None
        if has_desc:
            _desc = None if pd.isna(r["__DESC_APPLY__"]) else str(r["__DESC_APPLY__"])
        pairs.append((_id, _sin, _desc))

    if not pairs:
        return

    ids_csv = ", ".join(str(i) for (i, _, __) in pairs)
    when_sin = " ".join([f"WHEN {i} THEN {_sql_escape(s)}" for (i, s, __) in pairs])

    sets = [f"SINONIMO = CASE {id_col} {when_sin} END"]
    if has_desc:
        when_desc = " ".join([
            f"WHEN {i} THEN {_sql_escape(d) if d is not None else 'DESCRICAO'}"
            for (i, _, d) in pairs
        ])
        sets.append(f"DESCRICAO = CASE {id_col} {when_desc} END")

    sql = f"""
        UPDATE {table_fqn}
        SET {", ".join(sets)}
        WHERE {id_col} IN ({ids_csv})
    """
    session.sql(sql).collect()

def _persist_insumo_batch(session, table_fqn: str, df_before: pd.DataFrame, df_after: pd.DataFrame):
    """
    Atualiza INSUMO em lote (somente IDs que mudaram e INSUMO não vazio).
    Atualiza também DATA_ATUALIZACAO / USUARIO_ATUALIZACAO se existirem na tabela.
    """
    if df_after.empty or "ID" not in df_after.columns or "INSUMO" not in df_after.columns:
        return 0

    b = df_before[["ID", "INSUMO"]].copy()
    a = df_after[["ID", "INSUMO"]].copy()
    b["INSUMO"] = b["INSUMO"].astype("string").fillna("").str.strip()
    a["INSUMO"] = a["INSUMO"].astype("string").fillna("").str.strip()

    merged = a.merge(b, on="ID", suffixes=("", "_OLD"))

    changed = merged.loc[
        (merged["INSUMO"] != merged["INSUMO_OLD"]) & (merged["INSUMO"] != ""),
        ["ID", "INSUMO"]
    ].copy()

    if changed.empty:
        return 0

    cols_tbl = {c.name.upper() for c in session.table(table_fqn).schema}

    ids = [int(x) for x in changed["ID"].tolist()]
    ids_csv = ", ".join(str(i) for i in ids)

    when_ins = " ".join([
        f"WHEN {int(r.ID)} THEN {_sql_escape(r.INSUMO)}"
        for r in changed.itertuples(index=False)
    ])

    sets = [f"INSUMO = CASE ID {when_ins} END"]

    if "DATA_ATUALIZACAO" in cols_tbl:
        sets.append("DATA_ATUALIZACAO = CURRENT_TIMESTAMP()")
    if "USUARIO_ATUALIZACAO" in cols_tbl:
        u = current_user()
        sets.append(f"USUARIO_ATUALIZACAO = {sql_str(u.get('name') or u.get('username'))}")

    sql = f"""
        UPDATE {table_fqn}
        SET {", ".join(sets)}
        WHERE ID IN ({ids_csv})
    """
    session.sql(sql).collect()
    return len(ids)


# ==============================
# Ações de Banco
# ==============================
def apply_decision(session, df_items: pd.DataFrame, user: dict, ids: list[int], decisao: str, obs: str | None):
    """
    APROVADO  -> move de FQN_MAIN -> FQN_APR, audita e deleta da principal
    REJEITADO -> move de FQN_MAIN -> FQN_COR, audita e deleta da principal
    """
    if not ids:
        return

    session.sql("ALTER SESSION SET TIMEZONE = 'America/Sao_Paulo'").collect()
    ids_csv = ", ".join(str(i) for i in ids)

    get_cols = lambda table_fqn: [c.name for c in session.table(table_fqn).schema]
    cols_main = get_cols(FQN_MAIN)

    sql_move_insert = None
    sql_move_meta = None
    toast_icon = "✅"
    destino_legenda = "Aprovados"

    if decisao == "APROVADO":
        target = FQN_APR
        cols_target = [c for c in get_cols(target) if c in cols_main]
        col_list = ", ".join(cols_target)

        sql_move_insert = f"""
            INSERT INTO {target} ({col_list})
            SELECT {col_list}
            FROM {FQN_MAIN}
            WHERE ID IN ({ids_csv})
        """

        cols_apr_all = get_cols(target)
        meta_sets = []
        if "USUARIO_APROVACAO" in cols_apr_all:
            meta_sets.append(f"USUARIO_APROVACAO = {sql_str(user.get('name'))}")
        if "DATA_APROVACAO" in cols_apr_all:
            meta_sets.append("DATA_APROVACAO = CURRENT_TIMESTAMP()")

        if meta_sets:
            sql_move_meta = f"""
                UPDATE {target}
                SET {', '.join(meta_sets)}
                WHERE ID IN ({ids_csv})
            """

        toast_icon = "✅"
        destino_legenda = "Aprovados"

        for _id in ids:
            try:
                cod = None
                row = df_items.loc[df_items["ID"] == _id]
                if not row.empty and "CODIGO_PRODUTO" in row.columns:
                    cod = None if pd.isna(row["CODIGO_PRODUTO"].iloc[0]) else str(row["CODIGO_PRODUTO"].iloc[0])
                log_validacao(
                    session,
                    item_id=_id,
                    codigo_produto=cod,
                    origem=FQN_MAIN,
                    destino=FQN_APR,
                    obs=obs,
                    user=user,
                )
            except Exception:
                pass

    else:
        target = FQN_COR
        cols_target = [c for c in get_cols(target) if c in cols_main]
        col_list = ", ".join(cols_target)

        sql_move_insert = f"""
            INSERT INTO {target} ({col_list})
            SELECT {col_list}
            FROM {FQN_MAIN}
            WHERE ID IN ({ids_csv})
        """
        sql_move_meta = f"""
            UPDATE {target}
            SET USUARIO_REPROVACAO = {sql_str(user.get('name'))},
                DATA_REPROVACAO    = CURRENT_TIMESTAMP(),
                MOTIVO             = {sql_str(obs)}
            WHERE ID IN ({ids_csv})
        """
        toast_icon = "❌"
        destino_legenda = "Correção"

        for _id in ids:
            try:
                cod = None
                row = df_items.loc[df_items["ID"] == _id]
                if not row.empty and "CODIGO_PRODUTO" in row.columns:
                    cod = None if pd.isna(row["CODIGO_PRODUTO"].iloc[0]) else str(row["CODIGO_PRODUTO"].iloc[0])
                log_reprovacao(
                    session,
                    item_id=_id,
                    codigo_produto=cod,
                    origem=FQN_MAIN,
                    destino=FQN_COR,
                    motivo=obs,
                    user=user,
                )
            except Exception:
                pass

    sql_delete_main = f"DELETE FROM {FQN_MAIN} WHERE ID IN ({ids_csv})"

    try:
        session.sql("BEGIN").collect()
        session.sql(sql_move_insert).collect()
        if sql_move_meta:
            session.sql(sql_move_meta).collect()
        session.sql(sql_delete_main).collect()
        session.sql("COMMIT").collect()
        st.toast(f"{len(ids)} item(ns) movidos para {destino_legenda}.", icon=toast_icon)
    except Exception as e:
        session.sql("ROLLBACK").collect()
        st.error(f"Falha ao mover itens: {e}")


# ==============================
# Página (SEM TABS) - Validação
# ==============================
st.set_page_config(page_title="Catálogo • Validação", layout="wide")
init_auth()
require_roles("ADMIN")

st.title("✅ Validação de Itens")

user = current_user()
session = get_session()
session.sql("ALTER SESSION SET TIMEZONE = 'America/Sao_Paulo'").collect()

df_all = listar_itens_df(session)

if df_all.empty:
    st.info("Nenhum item cadastrado ainda.")
    st.stop()

user_map = load_user_display_map(session)

# ------------------------------
# Filtros
# ------------------------------
st.subheader("Filtros")
c1, c2, c3, c4 = st.columns(4)
with c1:
    sel_user = st.selectbox(
        "Usuário",
        build_user_options(df_all, user_map),
        index=0,
        key="val_sel_user",
    )
with c2:
    f_insumo = st.text_input("Insumo", key="val_f_insumo")
with c3:
    f_codigo = st.text_input("Código do Produto", key="val_f_codigo")
with c4:
    f_palavra = st.text_input("Palavra-chave", key="val_f_palavra")

mask = apply_common_filters(
    df_all,
    sel_user_name=sel_user,
    f_insumo=f_insumo,
    f_codigo=f_codigo,
    f_palavra=f_palavra,
    user_map=user_map,
)

df_view_before = df_all[mask].copy()

if df_view_before.empty:
    st.info("Nenhum item com os filtros aplicados.")
    st.stop()

st.caption(f"Itens no banco (filtro aplicado): **{len(df_view_before)}**")

# ------------------------------
# View editável (Validar + INSUMO)
# ------------------------------
df_view = df_view_before.copy()

if "Validar" not in df_view.columns:
    df_view.insert(0, "Validar", False)

left_sel, _ = st.columns([1, 3])
with left_sel:
    select_all_val = st.checkbox("Selecionar todos", key="val_select_all")
if select_all_val:
    df_view["Validar"] = True

df_view = _recalc_sinonimo_df_inplace(df_view)

# (opcional) persistir sinonimo sempre que renderiza
try:
    _persist_sinonimo_batch(session, FQN_MAIN, df_view[["ID", "DESCRICAO", "SINONIMO"]])
except Exception as e:
    st.warning(f"Não foi possível atualizar SINONIMO/descrição (pendentes): {e}")

DT_COLS_VAL = ["DATA_CADASTRO"]
df_view = coerce_datetimes(df_view, DT_COLS_VAL)
dt_cfg_val = build_datetime_column_config(df_view, DT_COLS_VAL)

df_view = reorder(df_view, ORDER_VALIDACAO, prepend=["Validar"])

col_cfg_all = {}
for c in df_view.columns:
    if c == "Validar":
        col_cfg_all[c] = st.column_config.CheckboxColumn(label="Validar", help="Marque para incluir na ação.")
    elif c == "INSUMO":
        col_cfg_all[c] = st.column_config.TextColumn(label="INSUMO", help="Preencha/ajuste o INSUMO antes de aprovar.")
    elif c in dt_cfg_val:
        col_cfg_all[c] = dt_cfg_val[c]
    else:
        col_cfg_all[c] = st.column_config.Column(disabled=True)

edited = st.data_editor(
    df_view,
    num_rows="fixed",
    hide_index=True,
    use_container_width=True,
    key="editor_validacao_unica",
    column_config=col_cfg_all,
    column_order=list(df_view.columns),
)

st.caption("Itens selecionados sem INSUMO preenchido não entram em Aprovar/Rejeitar.")
# ------------------------------
# Seleção para Aprovar/Rejeitar (SEM exigir INSUMO)
# ------------------------------
sel_mask = edited["Validar"] == True
ids_sel_ok = edited.loc[sel_mask, "ID"].tolist()

colA, colB = st.columns([1, 1])
with colA:
    if "open_aprova" not in st.session_state:
        st.session_state.open_aprova = False
    if st.button("✅ Aprovar selecionados", disabled=(len(ids_sel_ok) == 0)):
        st.session_state.open_aprova = True

with colB:
    if "open_reprova" not in st.session_state:
        st.session_state.open_reprova = False
    if st.button("❌ Rejeitar selecionados", disabled=(len(ids_sel_ok) == 0)):
        st.session_state.open_reprova = True


@st.dialog("Confirmar aprovação")
def dlg_aprova(ids: list[int]):
    st.write(f"Você vai **APROVAR** {len(ids)} item(ns).")
    obs = st.text_area("Observação (opcional)", key="dlg_obs_aprova")

    try:
        sel_edited = edited[edited["ID"].isin(ids)].copy()
        sel_edited = _recalc_sinonimo_df_inplace(sel_edited)
        _persist_sinonimo_batch(session, FQN_MAIN, sel_edited[["ID", "DESCRICAO", "SINONIMO"]])
        _persist_insumo_batch(session, FQN_MAIN, df_all[df_all["ID"].isin(ids)].copy(), sel_edited)
    except Exception as e:
        st.warning(f"Falha ao sincronizar campos antes da aprovação: {e}")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Confirmar ✅", type="primary"):
            apply_decision(session, df_all, user, ids, "APROVADO", obs)
            st.rerun()
    with c2:
        st.button("Cancelar", key="cancelA")

@st.dialog("Confirmar rejeição")
def dlg_reprova(ids: list[int]):
    st.write(f"Você vai **REJEITAR** {len(ids)} item(ns).")
    obs = st.text_area("Motivo/observação (opcional)", key="dlg_obs_reprova")

    try:
        sel_edited = edited[edited["ID"].isin(ids)].copy()
        sel_edited = _recalc_sinonimo_df_inplace(sel_edited)
        _persist_sinonimo_batch(session, FQN_MAIN, sel_edited[["ID", "DESCRICAO", "SINONIMO"]])
        _persist_insumo_batch(session, FQN_MAIN, df_all[df_all["ID"].isin(ids)].copy(), sel_edited)
    except Exception as e:
        st.warning(f"Falha ao sincronizar campos antes da rejeição: {e}")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Confirmar ❌", type="primary"):
            apply_decision(session, df_all, user, ids, "REJEITADO", obs)
            st.rerun()
    with c2:
        st.button("Cancelar", key="cancelR")

if st.session_state.get("open_aprova"):
    st.session_state.open_aprova = False
    dlg_aprova(ids_sel_ok)

if st.session_state.get("open_reprova"):
    st.session_state.open_reprova = False
    dlg_reprova(ids_sel_ok)

