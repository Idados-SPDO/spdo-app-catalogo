import streamlit as st
import pandas as pd
from src.db_snowflake import apply_common_filters, build_user_options, get_session, listar_itens_df, load_user_display_map, log_validacao, log_reprovacao
from src.auth import init_auth, is_authenticated, current_user

# ==============================
# Constantes / Config
# ==============================
FQN_MAIN  = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_INSUMOS_H"
FQN_COR   = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_CORRECOES_H"
FQN_APR   = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_APROVADOS_H"

ORDER_VALIDACAO = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","DATA_CADASTRO","USUARIO_CADASTRO","REFERENCIA",
]

ORDER_CORRECOES = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","REFERENCIA",
    "DATA_CADASTRO","USUARIO_CADASTRO",
    "DATA_REPROVACAO","USUARIO_REPROVACAO",
    "MOTIVO",
]

EDITABLE_COR_COLS = [
    "GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA","TIPO_CODIGO","CODIGO_PRODUTO",
    "INSUMO","ITEM","DESCRICAO","ESPECIFICACAO","MARCA","EMB_PRODUTO","UN_MED","QTD_MED",
    "EMB_COMERCIAL","QTD_EMB_COMERCIAL","SINONIMO","PALAVRA_CHAVE","REFERENCIA"
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

# ==============================
# Ações de Banco
# ==============================
def apply_decision(session, df_items, user, ids, decisao: str, obs: str | None):
    """
    APROVADO  -> move de FQN_MAIN -> FQN_APR, audita e deleta da principal
    REJEITADO -> move de FQN_MAIN -> FQN_COR, audita e deleta da principal
    """
    if not ids:
        return

    session.sql("ALTER SESSION SET TIMEZONE = 'America/Sao_Paulo'").collect()
    ids_csv = ", ".join(str(i) for i in ids)


    # 2) Colunas comuns (principal -> destino)
    get_cols = lambda table_fqn: [c.name for c in session.table(table_fqn).schema]
    cols_main = get_cols(FQN_MAIN)

    # inicializa para evitar NameError
    sql_move_insert = None
    sql_move_meta   = None
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
            meta_sets.append(f"USUARIO_APROVACAO = {sql_str(user['name'])}")
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
                    origem=FQN_MAIN,          # ou FQN_COR se a aprovação estiver vindo de correções (depende do teu fluxo)
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
            SET USUARIO_REPROVACAO = {sql_str(user['name'])},
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

    # 3) Delete da principal
    sql_delete_main = f"DELETE FROM {FQN_MAIN} WHERE ID IN ({ids_csv})"

    # 4) Transação atômica
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

def approve_correcoes(session, edited_df: pd.DataFrame, ids: list[int], user: dict):
    """
    Atualiza os campos editados em FQN_COR, move para FQN_APR, audita e remove da FQN_COR.
    """
    if not ids:
        return

    session.sql("ALTER SESSION SET TIMEZONE = 'America/Sao_Paulo'").collect()

    # mapa id -> linha editada (somente ids selecionados)
    edited_map = {int(r["ID"]): r for _, r in edited_df.iterrows() if int(r["ID"]) in ids}

    # Colunas comuns para mover
    cols_cor   = [c.name for c in session.table(FQN_COR).schema]
    cols_aprov = [c.name for c in session.table(FQN_APR).schema]
    common_cols = [c for c in cols_cor if c in cols_aprov]
    col_list = ", ".join(common_cols)

    # Auditoria
    values_audit = []
    for _id in ids:
        row = edited_map[_id]
        cod = None if pd.isna(row.get("CODIGO_PRODUTO")) else str(row.get("CODIGO_PRODUTO"))
        values_audit.append(
            f"({_id}, {sql_str(cod)}, 'APROVADO', {sql_str('Aprovado após correção')}, {sql_str(user['username'])}, {sql_str(user['name'])})"
        )

    try:
        session.sql("BEGIN").collect()

        # 1) UPDATE no CORRECOES com os campos editáveis
        for _id in ids:
            row = edited_map[_id]
            sets = []
            for c in EDITABLE_COR_COLS:
                if c in row:
                    sets.append(f'{c} = {sql_str(row[c])}')
            if sets:
                sql_upd = f"UPDATE {FQN_COR} SET {', '.join(sets)}, DATA_ATUALIZACAO = CURRENT_TIMESTAMP() WHERE ID = {_id}"
                session.sql(sql_upd).collect()

        # 2) Move para APROVADOS
        sql_insert = f"""
            INSERT INTO {FQN_APR} ({col_list})
            SELECT {col_list}
            FROM {FQN_COR}
            WHERE ID IN ({", ".join(str(i) for i in ids)})
        """
        session.sql(sql_insert).collect()

        # 3) Metadados de aprovação (se existirem as colunas)
        cols_apr_all = [c.name for c in session.table(FQN_APR).schema]
        meta_sets = []
        if "USUARIO_APROVACAO" in cols_apr_all:
            meta_sets.append(f"USUARIO_APROVACAO = {sql_str(user['name'])}")
        if "DATA_APROVACAO" in cols_apr_all:
            meta_sets.append("DATA_APROVACAO = CURRENT_TIMESTAMP()")

        if meta_sets:
            session.sql(f"""
                UPDATE {FQN_APR}
                SET {', '.join(meta_sets)}
                WHERE ID IN ({", ".join(str(i) for i in ids)})
            """).collect()


        # 5) Remove do CORRECOES
        session.sql(f"DELETE FROM {FQN_COR} WHERE ID IN ({', '.join(str(i) for i in ids)})").collect()

        session.sql("COMMIT").collect()
        st.toast(f"{len(ids)} item(ns) aprovados e movidos para o Catálogo.", icon="✅")
    except Exception as e:
        session.sql("ROLLBACK").collect()
        st.error(f"Falha ao aprovar correções: {e}")

# ==============================
# Página
# ==============================
# Auth
init_auth()
if not is_authenticated():
    st.error("Faça login para continuar.")
    st.stop()

st.set_page_config(page_title="Catálogo • Validação", layout="wide")
st.title("✅ Validação de Itens")

user = current_user()
session = get_session()
session.sql("ALTER SESSION SET TIMEZONE = 'America/Sao_Paulo'").collect()

# ---------- Aba: Pendente de Validação ----------

df = listar_itens_df(session)

if df.empty:
        st.info("Nenhum item cadastrado ainda.")
else:
        user_map = load_user_display_map(session)

        st.subheader("Filtros")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            sel_user = st.selectbox(
            "Usuário",
            build_user_options(df, user_map),  # <- retorna só labels legíveis
            index=0,
            key="val_sel_user"
        )
        with c2:
            f_insumo = st.text_input("Insumo", key="val_f_insumo")
        with c3:
            f_codigo = st.text_input("Código do Produto", key="val_f_codigo")
        with c4:
            f_palavra = st.text_input("Palavra-chave", key="val_f_palavra")

        mask = apply_common_filters(
            df,
            sel_user_name=sel_user,   # <- passa o *nome de exibição* selecionado
            f_insumo=f_insumo,
            f_codigo=f_codigo,
            f_palavra=f_palavra,
            user_map=user_map,
        )

        df_view = df[mask].copy()
        if df_view.empty:
            st.info("Nenhum item com os filtros aplicados.")
            ids_sel = []
        else:
            # coluna de ação
            st.caption(f"Itens para validação no banco: **{len(df_view)}**")
            if "Validar" not in df_view.columns:
                df_view.insert(0, "Validar", False)
            left_sel, right_sel = st.columns([1, 3])
            with left_sel:
                select_all_val = st.checkbox("Selecionar todos", key="val_select_all")
            if select_all_val:
                df_view["Validar"] = True

            # datas formatadas
            DT_COLS_VAL = ["DATA_CADASTRO"]
            df_view = coerce_datetimes(df_view, DT_COLS_VAL)
            dt_cfg_val = build_datetime_column_config(df_view, DT_COLS_VAL)

            # reordena (leva "Validar" pro início)
            df_view = reorder(df_view, ORDER_VALIDACAO, prepend=["Validar"])

            # ==== travar tudo exceto "Validar" ====
            col_cfg_all = {}
            for c in df_view.columns:
                if c == "Validar":
                    col_cfg_all[c] = st.column_config.CheckboxColumn(label="Validar", help="Marque para incluir na ação.")
                elif c in dt_cfg_val:
                    # datas já com formato e travadas
                    col_cfg_all[c] = dt_cfg_val[c]
                else:
                    # genérico travado
                    col_cfg_all[c] = st.column_config.Column(disabled=True)

            edited = st.data_editor(
                df_view,
                num_rows="fixed",
                hide_index=True,
                width="stretch",
                key="editor_validacao",
                column_config=col_cfg_all,
                column_order=list(df_view.columns) 
            ) # type: ignore

            sel_mask = edited["Validar"] == True
            ids_sel = edited.loc[sel_mask, "ID"].tolist()


        st.markdown("---")
        colA, colB = st.columns([1, 1])
        with colA:
            if "open_aprova" not in st.session_state:
                st.session_state.open_aprova = False
            btn_aprova = st.button("✅ Aprovar selecionados", disabled=(len(ids_sel) == 0))
            if btn_aprova:
                st.session_state.open_aprova = True
        with colB:
            if "open_reprova" not in st.session_state:
                st.session_state.open_reprova = False
            btn_reprova = st.button("❌ Rejeitar selecionados", disabled=(len(ids_sel) == 0))
            if btn_reprova:
                st.session_state.open_reprova = True

        @st.dialog("Confirmar aprovação")
        def dlg_aprova(ids):
            st.write(f"Você vai **APROVAR** {len(ids)} item(ns).")
            obs = st.text_area("Observação (opcional)", key="dlg_obs_aprova")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirmar ✅", type="primary"):
                    apply_decision(session, df, user, ids, "APROVADO", obs)
                    st.rerun()
            with c2:
                st.button("Cancelar", key="cancelA")

        @st.dialog("Confirmar rejeição")
        def dlg_reprova(ids):
            st.write(f"Você vai **REJEITAR** {len(ids)} item(ns).")
            obs = st.text_area("Motivo/observação (opcional)", key="dlg_obs_reprova")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirmar ❌", type="primary"):
                    apply_decision(session, df, user, ids, "REJEITADO", obs)
                    st.rerun()
            with c2:
                st.button("Cancelar", key="cancelR")

        if st.session_state.get("open_aprova"):
            st.session_state.open_aprova = False
            dlg_aprova(ids_sel)

        if st.session_state.get("open_reprova"):
            st.session_state.open_reprova = False
            dlg_reprova(ids_sel)
