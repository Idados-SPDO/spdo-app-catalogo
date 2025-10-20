import streamlit as st
import pandas as pd
from src.db_snowflake import apply_common_filters, build_user_options, get_session, load_user_display_map
from src.auth import init_auth, is_authenticated, current_user

st.title("Não Aprovados")

init_auth()
if not is_authenticated():
    st.error("Faça login para continuar.")
    st.stop()
user = current_user()
session = get_session()

FQN_MAIN  = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_INSUMOS_H"
FQN_COR   = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_CORRECOES_H"
FQN_APR   = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_APROVADOS_H"

ORDER_CORRECOES = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","REFERENCIA",
    "DATA_CADASTRO","USUARIO_CADASTRO",
    "DATA_REPROVACAO","USUARIO_REPROVACAO",      # novos
    "MOTIVO",
    "DATA_ATUALIZACAO","USUARIO_ATUALIZACAO",
]

EDITABLE_COR_COLS = [
    "GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA","TIPO_CODIGO","CODIGO_PRODUTO",
    "INSUMO","ITEM","DESCRICAO","ESPECIFICACAO","MARCA","EMB_PRODUTO","UN_MED","QTD_MED",
    "EMB_COMERCIAL","QTD_EMB_COMERCIAL","SINONIMO","PALAVRA_CHAVE","REFERENCIA"
]
st.subheader("Itens para correção")


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


def sql_str(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): 
        return "NULL"
    s = str(v).replace("'", "''")
    return f"'{s}'"

def resend_to_validacao(session, edited_df: pd.DataFrame, ids: list[int], user: dict):
    """
    Atualiza campos editáveis em FQN_COR, move de FQN_COR -> FQN_MAIN (fila de validação),
    zera campos de validação em MAIN, audita e remove de FQN_COR.
    """
    if not ids:
        return

    session.sql("ALTER SESSION SET TIMEZONE = 'America/Sao_Paulo'").collect()

    # mapa id -> linha editada (somente ids selecionados)
    edited_map = {int(r["ID"]): r for _, r in edited_df.iterrows() if int(r["ID"]) in ids}

    cols_cor   = [c.name for c in session.table(FQN_COR).schema]
    cols_main  = [c.name for c in session.table(FQN_MAIN).schema]
    common_cols = [c for c in cols_cor if c in cols_main]
    col_list = ", ".join(common_cols)

    # Auditoria: marcar ação de reenvio para validação
    values_audit = []
    for _id in ids:
        row = edited_map[_id]
        cod = None if pd.isna(row.get("CODIGO_PRODUTO")) else str(row.get("CODIGO_PRODUTO"))
        values_audit.append(
            f"({_id}, {sql_str(cod)}, 'REENVIADO_VALIDACAO', {sql_str('Reenviado para validação após correção')}, {sql_str(user['username'])}, {sql_str(user['name'])})"
        )

    ids_csv = ", ".join(str(i) for i in ids)

    try:
        session.sql("BEGIN").collect()

        # 1) UPDATE nos campos editáveis em COR + carimbar DATA_ATUALIZACAO
        for _id in ids:
            row = edited_map[_id]
            sets = []
            for c in EDITABLE_COR_COLS:
                if c in row:
                    sets.append(f"{c} = {sql_str(row[c])}")
            if sets:
                sql_upd = f"""
                    UPDATE {FQN_COR}
                    SET {', '.join(sets)}, DATA_ATUALIZACAO = CURRENT_TIMESTAMP()
                    WHERE ID = {_id}
                """
                session.sql(sql_upd).collect()

        # 2) Inserir de COR -> MAIN (fila de validação)
        sql_insert = f"""
            INSERT INTO {FQN_MAIN} ({col_list})
            SELECT {col_list}
            FROM {FQN_COR}
            WHERE ID IN ({ids_csv})
        """
        session.sql(sql_insert).collect()

        # 3) Zerar campos de validação e atualizar metadados em MAIN
        # (se as colunas existirem)
        cols_main_all = set(cols_main)
        meta_sets = []

        if "DATA_ATUALIZACAO" in cols_main_all:
            meta_sets.append("DATA_ATUALIZACAO = NULL")
        if "USUARIO_ATUALIZACAO" in cols_main_all:
            meta_sets.append("USUARIO_ATUALIZACAO = NULL")
        
        if meta_sets:
            # usamos os mesmos IDs recém inseridos; como a PK é ID e foi copiada, funciona 1:1
            session.sql(f"""
                UPDATE {FQN_MAIN}
                SET {', '.join(meta_sets)}
                WHERE ID IN ({ids_csv})
            """).collect()


        # 5) Remover do COR
        session.sql(f"DELETE FROM {FQN_COR} WHERE ID IN ({ids_csv})").collect()

        session.sql("COMMIT").collect()
        st.toast(f"{len(ids)} item(ns) reenviado(s) para Validação.", icon="📤")
    except Exception as e:
        session.sql("ROLLBACK").collect()
        st.error(f"Falha ao reenviar para validação: {e}")

try:
        cnt = session.sql(f"SELECT COUNT(*) AS N FROM {FQN_COR}").collect()[0]["N"]
        st.caption(f"Total reprovados no banco: **{cnt}**")
        
        df_cor = session.sql(f"""
            SELECT * EXCLUDE (DATA_ATUALIZACAO, USUARIO_ATUALIZACAO)
            FROM {FQN_COR}
        """).to_pandas()

        if "REPROVADO_EM" in df_cor.columns:
            df_cor = df_cor.sort_values("REPROVADO_EM", ascending=False)
except Exception as e:
        st.error(f"Erro ao carregar correções: {e}")
        df_cor = pd.DataFrame()

if df_cor.empty:
        st.info("Nenhum item reprovado.")
else:
        user_map = load_user_display_map(session)

        st.subheader("Filtros")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            sel_user = st.selectbox("Usuário", build_user_options(df_cor, user_map), index=0, key="cor_sel_user")
        with c2:
            f_insumo = st.text_input("Insumo", key="cor_f_insumo")
        with c3:
            f_codigo = st.text_input("Código do Produto", key="cor_f_codigo")
        with c4:
            f_palavra = st.text_input("Palavra-chave", key="cor_f_palavra")

        mask_cor = apply_common_filters(
            df_cor,
            sel_user_name=sel_user,
            f_insumo=f_insumo,
            f_codigo=f_codigo,
            f_palavra=f_palavra,
            user_map=user_map,
        )

        df_cor_view = df_cor[mask_cor].copy()



        if df_cor_view.empty:
            st.info("Nenhum item com os filtros aplicados.")
        else:
            if "Selecionar" not in df_cor_view.columns:
                df_cor_view.insert(0, "Selecionar", False)
            
            left_sel_cor, right_sel_cor = st.columns([1, 3])
            with left_sel_cor:
                select_all_cor = st.checkbox("Selecionar todos", key="cor_select_all")
            if select_all_cor:
                df_cor_view["Selecionar"] = True

            DT_COLS_COR = ["DATA_REPROVACAO", "DATA_CADASTRO"]
            df_cor_view = coerce_datetimes(df_cor_view, DT_COLS_COR)
            dt_cfg_cor  = build_datetime_column_config(df_cor_view, DT_COLS_COR)
            df_cor_view = reorder(df_cor_view, ORDER_CORRECOES, prepend=["Selecionar"])
            LOCK_COR = {"ID","DATA_CADASTRO","USUARIO_CADASTRO","DATA_REPROVACAO","USUARIO_REPROVACAO","MOTIVO"}

            col_cfg_cor = {}
            for c in df_cor_view.columns:
                if c == "Selecionar":
                    col_cfg_cor[c] = st.column_config.CheckboxColumn(label="Selecionar")
                elif c in dt_cfg_cor:
                    col_cfg_cor[c] = dt_cfg_cor[c]  # já vem com disabled=True do helper
                elif c in LOCK_COR:
                    col_cfg_cor[c] = st.column_config.Column(disabled=True)
                else:
                    pass
            edited_cor = st.data_editor(
                df_cor_view,
                num_rows="fixed",
                hide_index=True,
                width="stretch",
                key="editor_correcao_page",
                column_config=col_cfg_cor,
            )

            sel_mask = edited_cor["Selecionar"] if "Selecionar" in edited_cor.columns else pd.Series(False, index=edited_cor.index)
            sel_ids  = edited_cor.loc[sel_mask == True, "ID"].tolist() if "ID" in edited_cor.columns else []

            st.markdown("---")
            left, right = st.columns([2,1])
            with left:
                st.caption("Atualize os valores necessários diretamente na tabela acima, selecione as linhas e aprove.")
            with right:
                if st.button("✅ Aprovar selecionados", disabled=(len(sel_ids) == 0), key="cor_btn_aprovar"):
                    resend_to_validacao(session, edited_cor, sel_ids, user)
                    st.rerun()
