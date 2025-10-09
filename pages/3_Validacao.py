import streamlit as st
import pandas as pd
from src.db_snowflake import get_session, listar_itens_df
from src.auth import init_auth, is_authenticated, current_user

# ==============================
# Constantes / Config
# ==============================
FQN_MAIN  = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_INSUMOS"
FQN_AUDIT = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_VALIDACOES"
FQN_COR   = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_CORRECOES"
FQN_APR   = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_APROVADOS"

ORDER_VALIDACAO = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","DATA_CADASTRO","USUARIO_CADASTRO","DATA_VALIDACAO",
    "USUARIO_VALIDADOR","DATA_ATUALIZACAO","USUARIO_ATUALIZACAO","REFERENCIA",
]

ORDER_CORRECOES = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","REFERENCIA",
    "DATA_CADASTRO","USUARIO_CADASTRO",
    "DATA_REPROVACAO","USUARIO_REPROVACAO",      # novos
    "MOTIVO",
    "DATA_VALIDACAO","USUARIO_VALIDADOR",
    "DATA_ATUALIZACAO","USUARIO_ATUALIZACAO",
    "STATUS_VALIDACAO",
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

    # 1) Auditoria (INSERT em lote)
    values = []
    for _id in ids:
        row = df_items.loc[df_items["ID"] == _id].head(1)
        cod = None if row.empty else (None if pd.isna(row["CODIGO_PRODUTO"].iloc[0]) else str(row["CODIGO_PRODUTO"].iloc[0]))
        values.append(f"({_id}, {sql_str(cod)}, {sql_str(decisao)}, {sql_str(obs)}, {sql_str(user['username'])}, {sql_str(user['name'])})")
    sql_ins_audit = f"""
        INSERT INTO {FQN_AUDIT}
          (ID_ITEM, CODIGO_PRODUTO, DECISAO, OBS, VALIDADO_POR, NOME_VALIDADOR)
        VALUES {", ".join(values)}
    """

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
            meta_sets.append(f"USUARIO_APROVACAO = {sql_str(user['username'])}")
        if "DATA_APROVACAO" in cols_apr_all:
            meta_sets.append("DATA_APROVACAO = CURRENT_TIMESTAMP()")
        # (opcional: manter também o conceito de validador)
        if "USUARIO_VALIDADOR" in cols_apr_all:
            meta_sets.append(f"USUARIO_VALIDADOR = {sql_str(user['username'])}")
        if "DATA_VALIDACAO" in cols_apr_all:
            meta_sets.append("DATA_VALIDACAO = CURRENT_TIMESTAMP()")

        if meta_sets:
            sql_move_meta = f"""
                UPDATE {target}
                SET {', '.join(meta_sets)}
                WHERE ID IN ({ids_csv})
            """

        toast_icon = "✅"
        destino_legenda = "Aprovados"

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
            SET USUARIO_REPROVACAO = {sql_str(user['username'])},
                DATA_REPROVACAO    = CURRENT_TIMESTAMP(),
                MOTIVO             = {sql_str(obs)},
                STATUS_VALIDACAO   = 'PENDENTE'
            WHERE ID IN ({ids_csv})
        """
        toast_icon = "❌"
        destino_legenda = "Correção"

    # 3) Delete da principal
    sql_delete_main = f"DELETE FROM {FQN_MAIN} WHERE ID IN ({ids_csv})"

    # 4) Transação atômica
    try:
        session.sql("BEGIN").collect()
        session.sql(sql_ins_audit).collect()
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
            meta_sets.append(f"USUARIO_APROVACAO = {sql_str(user['username'])}")
        if "DATA_APROVACAO" in cols_apr_all:
            meta_sets.append("DATA_APROVACAO = CURRENT_TIMESTAMP()")
        if "USUARIO_VALIDADOR" in cols_apr_all:
            meta_sets.append(f"USUARIO_VALIDADOR = {sql_str(user['username'])}")
        if "DATA_VALIDACAO" in cols_apr_all:
            meta_sets.append("DATA_VALIDACAO = CURRENT_TIMESTAMP()")

        if meta_sets:
            session.sql(f"""
                UPDATE {FQN_APR}
                SET {', '.join(meta_sets)}
                WHERE ID IN ({", ".join(str(i) for i in ids)})
            """).collect()

        # 4) Auditoria
        sql_audit = f"""
            INSERT INTO {FQN_AUDIT}
              (ID_ITEM, CODIGO_PRODUTO, DECISAO, OBS, VALIDADO_POR, NOME_VALIDADOR)
            VALUES {", ".join(values_audit)}
        """
        session.sql(sql_audit).collect()

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

tab_valida, tab_corr = st.tabs(["🟡 Pendente de Validação", "❌ Não Aprovados"])

# ---------- Aba: Pendente de Validação ----------
with tab_valida:
    df = listar_itens_df(session)

    if df.empty:
        st.info("Nenhum item cadastrado ainda.")
    else:
        ALL = "— Todos —"
        unique_opts = lambda s: [ALL] + sorted([str(x) for x in s.dropna().unique()])

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            f_codigo = st.text_input("Código do Produto (exato)", key="val_f_codigo")
        with c2:
            sel_tipo = st.selectbox("Tipo do Produto",
                                    unique_opts(df.get("TIPO_CODIGO", pd.Series(dtype=str))),
                                    index=0, key="val_sel_tipo")
        with c3:
            sel_insumo = st.selectbox("Insumo",
                                      unique_opts(df.get("INSUMO", pd.Series(dtype=str))),
                                      index=0, key="val_sel_insumo")
        with c4:
            f_palavra = st.text_input("Palavra-chave (contém)", key="val_f_palavra")

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
        if df_view.empty:
            st.info("Nenhum item com os filtros aplicados.")
            ids_sel = []
        else:
            if "Selecionar" not in df_view.columns:
                df_view.insert(0, "Selecionar", False)

            DT_COLS_VAL = ["DATA_CADASTRO", "DATA_ATUALIZACAO", "DATA_VALIDACAO"]
            df_view = coerce_datetimes(df_view, DT_COLS_VAL)
            dt_cfg_val = build_datetime_column_config(df_view, DT_COLS_VAL)
            df_view = reorder(df_view, ORDER_VALIDACAO, prepend=["Selecionar"])

            edited = st.data_editor(
                df_view,
                num_rows="fixed",
                hide_index=True,
                use_container_width=True,
                key="editor_validacao",
                column_config=dt_cfg_val,
            )

            sel_mask = edited["Selecionar"] == True
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

# ---------- Aba: Não Aprovados ----------
with tab_corr:
    st.subheader("Itens para correção")

    try:
        cnt = session.sql(f"SELECT COUNT(*) AS N FROM {FQN_COR}").collect()[0]["N"]
        st.caption(f"Total reprovados no banco: **{cnt}**")

        df_cor = session.table(FQN_COR).to_pandas()
        if "REPROVADO_EM" in df_cor.columns:
            df_cor = df_cor.sort_values("REPROVADO_EM", ascending=False)
    except Exception as e:
        st.error(f"Erro ao carregar correções: {e}")
        df_cor = pd.DataFrame()

    if df_cor.empty:
        st.info("Nenhum item reprovado.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            f_cod2 = st.text_input("Código do Produto (exato)", key="cor_f_cod")
        with c2:
            f_mot  = st.text_input("Motivo (contém)", key="cor_f_mot")

        mask_cor = pd.Series(True, index=df_cor.index)
        if f_cod2:
            mask_cor &= df_cor.get("CODIGO_PRODUTO", pd.Series("", index=df_cor.index)).astype(str).eq(f_cod2.strip())
        if f_mot:
            mask_cor &= df_cor.get("MOTIVO", pd.Series("", index=df_cor.index)).astype(str).str.contains(f_mot, case=False, regex=False)

        df_cor_view = df_cor[mask_cor].copy()
        if df_cor_view.empty:
            st.info("Nenhum item com os filtros aplicados.")
        else:
            if "Selecionar" not in df_cor_view.columns:
                df_cor_view.insert(0, "Selecionar", False)

            DT_COLS_COR = ["REPROVADO_EM","DATA_CADASTRO","DATA_ATUALIZACAO","DATA_VALIDACAO"]
            df_cor_view = coerce_datetimes(df_cor_view, DT_COLS_COR)
            dt_cfg_cor  = build_datetime_column_config(df_cor_view, DT_COLS_COR)
            df_cor_view = reorder(df_cor_view, ORDER_CORRECOES, prepend=["Selecionar"])

            edited_cor = st.data_editor(
                df_cor_view, num_rows="fixed", hide_index=True,
                use_container_width=True, key="editor_correcao_page",
                column_config=dt_cfg_cor,
            )

            sel_mask = edited_cor["Selecionar"] if "Selecionar" in edited_cor.columns else pd.Series(False, index=edited_cor.index)
            sel_ids  = edited_cor.loc[sel_mask == True, "ID"].tolist() if "ID" in edited_cor.columns else []

            st.markdown("---")
            left, right = st.columns([2,1])
            with left:
                st.caption("Atualize os valores necessários diretamente na tabela acima, selecione as linhas e aprove.")
            with right:
                if st.button("✅ Aprovar selecionados", disabled=(len(sel_ids) == 0), key="cor_btn_aprovar"):
                    approve_correcoes(session, edited_cor, sel_ids, user)
                    st.rerun()
