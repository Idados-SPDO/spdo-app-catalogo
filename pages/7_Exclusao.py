import streamlit as st
import pandas as pd
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window
from datetime import datetime, timezone

from src.auth import require_roles, current_user
from src.db_snowflake import get_session
from src.variables import FQN_APR, FQN_RMV, FQN_LOG_RMV

require_roles("ADMIN")

st.set_page_config(page_title="Catálogo • Remoção", layout="wide")
st.title("🗑️ Remoção de Insumos")

session = get_session()
FQN_CATALOGO = FQN_APR


def _esc(s: str) -> str:
    return (s or "").replace("'", "''")


def load_df(f_insumo: str, f_id: str, f_ean: str) -> pd.DataFrame:
    t = session.table(FQN_CATALOGO)
    cat_cols = {c.upper() for c in t.schema.names}

    if f_insumo.strip() and "INSUMO" in cat_cols:
        t = t.filter(F.col("INSUMO").ilike(f"%{f_insumo.strip()}%"))
    if f_ean.strip() and "CODIGO_PRODUTO" in cat_cols:
        t = t.filter(F.col("CODIGO_PRODUTO").ilike(f"%{f_ean.strip()}%"))
    if f_id.strip() and "ID" in cat_cols:
        try:
            t = t.filter(F.col("ID") == int(f_id.strip()))
        except Exception:
            t = t.filter(F.col("ID").cast("string") == f_id.strip())

    show_cols = [c for c in ["ID", "CODIGO_PRODUTO", "INSUMO", "CATEGORIA", "FAMILIA", "MARCA"] if c in cat_cols]
    if not show_cols:
        st.error("Tabela não possui colunas esperadas para seleção (ex.: ID, INSUMO, CODIGO_PRODUTO).")
        st.stop()

    return t.select([F.col(c) for c in show_cols]).to_pandas()


# -----------------------------
# Filtros (mantém botão Buscar)
# -----------------------------

tab1, tab2 = st.tabs(["Remoção", "Lista de Removidos"])

with tab1:
    st.subheader("Filtros")
    with st.form("exc_filters"):
        c1, c2, c3 = st.columns(3)
        with c1:
            f_insumo = st.text_input("Insumo:")
        with c2:
            f_id = st.text_input("ID:")
        with c3:
            f_ean = st.text_input("EAN/Código do Produto:")

        submitted = st.form_submit_button("Buscar")

    # Atualiza DF somente quando clicar em Buscar OU quando ainda não existe cache
    if submitted or "rmv_df" not in st.session_state:
        st.session_state["rmv_last_filters"] = {"INSUMO": f_insumo, "ID": f_id, "EAN": f_ean}
        try:
            st.session_state["rmv_df"] = load_df(f_insumo, f_id, f_ean)
        except Exception as e:
            st.error(f"Falha ao carregar dados do catálogo: {e}")
            st.stop()
    else:
        st.caption("Exibindo o último resultado carregado. Clique em **Buscar** para atualizar.")

    df = st.session_state["rmv_df"].copy()

    if df.empty:
        st.warning("Nenhum registro encontrado com esses filtros.")
        st.stop()

    st.caption(f"Registros carregados: **{len(df)}**")

    # -----------------------------
    # Tabela com seleção
    # -----------------------------
    df_ui = df.copy()
    df_ui.insert(0, "REMOVER", False)

    edited = st.data_editor(
        df_ui,
        hide_index=True,
        use_container_width=True,
        column_config={
            "REMOVER": st.column_config.CheckboxColumn("Remover?", default=False),
        },
        key="rmv_editor",
    )

    if "ID" not in edited.columns:
        st.error("A coluna ID é obrigatória para remoção.")
        st.stop()

    to_remove = edited.loc[edited["REMOVER"] == True, "ID"].dropna().tolist()

    # -----------------------------
    # Aplicar remoção (mover + log)
    # -----------------------------
    st.divider()
    st.subheader("Aplicar remoção")

    motivo = st.text_area("Motivo (opcional)", placeholder="Ex.: item duplicado / inconsistência / etc.")
    confirm = st.text_input("Digite REMOVER para habilitar", placeholder="REMOVER")

    cA, cB = st.columns([2, 1])
    with cA:
        st.warning("A ação remove do catálogo e move para a tabela de removidos. Use com cautela.", icon="⚠️")
    with cB:
        can_run = (confirm.strip().upper() == "REMOVER") and (len(to_remove) > 0)

        if st.button("Remover selecionados", use_container_width=True, disabled=not can_run):
            ids = []
            for x in to_remove:
                try:
                    ids.append(int(x))
                except Exception:
                    pass

            if not ids:
                st.error("Nenhum ID válido selecionado para remoção.")
                st.stop()

            ids_in = ",".join(str(i) for i in ids)

            try:
                u = current_user()
                usuario = u.get("username", "admin")
                now_ntz = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

                # Expressões seguras caso alguma coluna não exista no catálogo
                cat_cols = {c.upper() for c in session.table(FQN_CATALOGO).schema.names}
                codigo_expr = "CODIGO_PRODUTO" if "CODIGO_PRODUTO" in cat_cols else "CAST(NULL AS STRING)"
                insumo_expr = "INSUMO" if "INSUMO" in cat_cols else "CAST(NULL AS STRING)"

                session.sql("BEGIN TRANSACTION").collect()

                # 1) Move o registro inteiro para removidos
                session.sql(f"""
                    INSERT INTO {FQN_RMV}
                    SELECT *
                    FROM {FQN_CATALOGO}
                    WHERE ID IN ({ids_in})
                """).collect()

                # 2) Log (um único INSERT...SELECT)
                session.sql(f"""
                    INSERT INTO {FQN_LOG_RMV}
                        (ID, CODIGO_PRODUTO, INSUMO, MOTIVO, DATA_REMOCAO, USUARIO_REMOCAO)
                    SELECT
                        ID,
                        {codigo_expr},
                        {insumo_expr},
                        '{_esc(motivo)}',
                        '{now_ntz}',
                        '{_esc(usuario)}'
                    FROM {FQN_CATALOGO}
                    WHERE ID IN ({ids_in})
                """).collect()

                # 3) Remove do catálogo
                session.sql(f"DELETE FROM {FQN_CATALOGO} WHERE ID IN ({ids_in})").collect()

                session.sql("COMMIT").collect()

                # limpa cache para recarregar da fonte
                st.session_state.pop("rmv_df", None)

                st.success(f"Remoção concluída. Itens movidos: {len(ids)}")
                st.rerun()

            except Exception as e:
                try:
                    session.sql("ROLLBACK").collect()
                except Exception:
                    pass
                st.error(f"Falha ao remover/mover: {e}")
                st.stop()

with tab2:
    st.subheader("Lista de Removidos:")

    try:
        rmv = session.table(FQN_RMV)
        log = session.table(FQN_LOG_RMV)
    except Exception as e:
        st.error(f"Falha ao carregar tabelas de removidos/log: {e}")
        st.stop()

    rmv_cols = {c.upper() for c in rmv.schema.names}
    log_cols = {c.upper() for c in log.schema.names}

    if "CODIGO_PRODUTO" not in rmv_cols or "CODIGO_PRODUTO" not in log_cols:
        st.error("Para o merge, ambas as tabelas precisam ter a coluna CODIGO_PRODUTO.")
        st.stop()

    # Se houver mais de um log por CODIGO_PRODUTO, pega o mais recente (DATA_REMOCAO desc)
    w = Window.partition_by(log["CODIGO_PRODUTO"]).order_by(log["DATA_REMOCAO"].desc_nulls_last())

    log_latest = (
        log.select(
            log["CODIGO_PRODUTO"].as_("CODIGO_PRODUTO"),
            log["MOTIVO"].as_("MOTIVO_RMV"),
            log["DATA_REMOCAO"].as_("DATA_REMOCAO_RMV"),
            log["USUARIO_REMOCAO"].as_("USUARIO_REMOCAO_RMV"),
            F.row_number().over(w).as_("RN"),
        )
        .filter(F.col("RN") == 1)
        .drop("RN")
    )

    joined = rmv.join(
        log_latest,
        rmv["CODIGO_PRODUTO"] == log_latest["CODIGO_PRODUTO"],
        how="left",
    )

    # Todas as colunas do removido + as 3 do log
    rmv_select_cols = [rmv[c] for c in rmv.columns]

    final_df = joined.select(
        *rmv_select_cols,
        F.col("MOTIVO_RMV"),
        F.col("DATA_REMOCAO_RMV"),
        F.col("USUARIO_REMOCAO_RMV"),
    )

    st.dataframe(final_df, use_container_width=True, hide_index=True)
