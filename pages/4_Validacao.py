# pages/3_Validacao.py
import streamlit as st
import pandas as pd
from datetime import datetime, date
from src.db_snowflake import get_session, listar_itens_df
from src.auth import init_auth, is_authenticated, current_user

from src.auth import init_auth, is_authenticated

init_auth()
if not is_authenticated():
    st.error("Faça login para continuar.")
    st.stop()

FQN_MAIN   = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_INSUMOS"
FQN_AUDIT  = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_VALIDACOES"

st.set_page_config(page_title="Catálogo • Validação", layout="wide")
init_auth()

st.title("✅ Validação de Itens")

# exige login
if not is_authenticated():
    st.error("Faça login para acessar a validação.")
    st.stop()

user = current_user()
session = get_session()

session.sql("ALTER SESSION SET TIMEZONE = 'America/Sao_Paulo'").collect()


tab_valida, tab_aprov, tab_corr, tab_hist = st.tabs(
    ["✔️ Validação", "✅ Aprovados", "📝 Correção", "🧾 Histórico"]
)

# ==============================
# Aba 1: Validação (não altera a principal)
# ==============================
with tab_valida:
    df = listar_itens_df(session)
    if df.empty:
        st.info("Nenhum item cadastrado ainda.")
        st.stop()

    # Filtros (sem status/última decisão)
    ALL = "— Todos —"
    def unique_opts(series: pd.Series):
        return [ALL] + sorted([str(x) for x in series.dropna().unique()])

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        f_codigo = st.text_input("Código do Produto (exato)", key="val_f_codigo")
    with c2:
        tipo_opts = unique_opts(df.get("TIPO_PRODUTO", pd.Series(dtype=str)))
        sel_tipo = st.selectbox("Tipo do Produto", tipo_opts, index=0, key="val_sel_tipo")

    with c3:
        insumo_opts = unique_opts(df.get("INSUMO", pd.Series(dtype=str)))
        sel_insumo = st.selectbox("Insumo", insumo_opts, index=0, key="val_sel_insumo")

    with c4:
        f_palavra = st.text_input("Palavra-chave (contém)", key="val_f_palavra")

    mask = pd.Series(True, index=df.index)
    if f_codigo:
        mask &= (df.get("CODIGO_PRODUTO", pd.Series("", index=df.index)).astype(str).str.strip() == f_codigo.strip())
    if sel_tipo != ALL:
        mask &= (df.get("TIPO_PRODUTO", pd.Series("", index=df.index)).astype(str) == sel_tipo)
    if sel_insumo != ALL:
        mask &= (df.get("INSUMO", pd.Series("", index=df.index)).astype(str) == sel_insumo)
    if f_palavra:
        mask &= df.get("PALAVRA_CHAVE", pd.Series("", index=df.index)).astype(str).str.contains(f_palavra, case=False, regex=False)

    df_view = df[mask].copy()
    if df_view.empty:
        st.info("Nenhum item com os filtros aplicados.")
        st.stop()

    # coluna de seleção (sem exibir colunas de status)
    df_view.insert(0, "Selecionar", False)

    preferred = [
        "Selecionar","ID","CODIGO_PRODUTO","TIPO_PRODUTO","INSUMO","ITEM","MARCA",
        "PALAVRA_CHAVE","SINONIMO","GRUPO","CATEGORIA","FAMILIA","SUBFAMILIA"
    ]
    cols = [c for c in preferred if c in df_view.columns] + [c for c in df_view.columns if c not in preferred]
    df_view = df_view[cols]

    edited = st.data_editor(
        df_view,
        num_rows="fixed",
        hide_index=True,
        use_container_width=True,
        key="editor_validacao"
    )

    # itens selecionados
    sel_mask = edited["Selecionar"] == True
    sel_rows = edited[sel_mask]
    ids_sel = sel_rows.get("ID", pd.Series(dtype=int)).tolist()

    st.markdown("---")
    colA, colB = st.columns([1,1])
    with colA:
        if "open_aprova" not in st.session_state: st.session_state.open_aprova = False
        btn_aprova = st.button("✅ Aprovar selecionados", disabled=not ids_sel)
        if btn_aprova:
            st.session_state.open_aprova = True
    with colB:
        if "open_reprova" not in st.session_state: st.session_state.open_reprova = False
        btn_reprova = st.button("❌ Rejeitar selecionados", disabled=not ids_sel)
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

# ==============================
# Aba 2: Histórico (somente leitura a partir da auditoria)
# ==============================
with tab_hist:
    st.subheader("Histórico de validações")
    # filtros de histórico
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        f_dec = st.selectbox("Decisão", ["— Todos —", "APROVADO", "REJEITADO"], index=0, key="hist_f_dec")
    with col2:
        # usuários distintos na auditoria
        try:
            users_df = session.sql(f"SELECT DISTINCT VALIDADO_POR, NOME_VALIDADOR FROM {FQN_AUDIT}").to_pandas()
        except Exception:
            users_df = pd.DataFrame(columns=["VALIDADO_POR","NOME_VALIDADOR"])
        user_opts = ["— Todos —"] + sorted(
            {f"{r['NOME_VALIDADOR']} ({r['VALIDADO_POR']})" if pd.notna(r['NOME_VALIDADOR']) else r['VALIDADO_POR']
             for _, r in users_df.iterrows() if pd.notna(r.get("VALIDADO_POR"))}
        )
        f_user = st.selectbox("Validador", user_opts, index=0, key="hist_f_user")
    with col3:
        f_ini = st.date_input("Data início", value=date.today().replace(day=1), key="hist_f_ini")
    with col4:
        f_fim = st.date_input("Data fim", value=date.today(), key="hist_f_fim")

    # monta SQL do histórico com joins leves para campos visuais
    
    sql_hist = f"""
SELECT
  a.ID_EVENTO,
  a.VALIDADO_EM,                                  -- NTZ
  a.VALIDADO_EM::TIMESTAMP_LTZ AS VALIDADO_EM_BR, -- usa TIMEZONE da sessão (São Paulo)
  a.DECISAO,
  a.OBS,
  a.VALIDADO_POR,
  a.NOME_VALIDADOR,
  a.ID_ITEM,
  a.CODIGO_PRODUTO,
  t.INSUMO,
  t.ITEM,
  t.MARCA,
  t.TIPO_PRODUTO,
  t.PALAVRA_CHAVE
FROM {FQN_AUDIT} a
LEFT JOIN {FQN_MAIN} t
  ON a.ID_ITEM = t.ID
WHERE a.VALIDADO_EM >= TO_TIMESTAMP_NTZ('{f_ini} 00:00:00')
  AND a.VALIDADO_EM <  TO_TIMESTAMP_NTZ('{f_fim} 23:59:59')
"""

    # filtros dinâmicos (sempre vêm ANTES do ORDER BY)
    if f_dec in ("APROVADO", "REJEITADO"):
        sql_hist += f" AND a.DECISAO = '{f_dec}'"

    if f_user != "— Todos —":
        # extrai username entre parênteses se vier no formato "Nome (username)"
        if "(" in f_user and f_user.endswith(")"):
            username = f_user.split("(")[-1][:-1]
        else:
            username = f_user
        username_esc = username.replace("'", "''")
        sql_hist += f" AND a.VALIDADO_POR = '{username_esc}'"

    # ORDER BY só aqui no final (sem ponto e vírgula)
    sql_hist += " ORDER BY a.VALIDADO_EM DESC"



    
    try:
        df_hist = session.sql(sql_hist).to_pandas()
    except Exception as e:
        st.error(f"Falha ao carregar histórico: {e}")
        df_hist = pd.DataFrame()

    st.dataframe(
        df_hist,
        hide_index=True,
        use_container_width=True,
        column_config={
            "VALIDADO_EM_BR": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm"),
            # Se quiser, esconda a coluna original:
            # "VALIDADO_EM": None,
        }
    )


with tab_aprov:
    st.subheader("Itens aprovados")
    FQN_APR = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_APROVADOS"

    try:
        df_apr = session.table(FQN_APR).sort("APROVADO_EM", ascending=False).to_pandas()
    except Exception as e:
        st.error(f"Erro ao carregar aprovados: {e}")
        df_apr = pd.DataFrame()

    if df_apr.empty:
        st.info("Nenhum item aprovado.")
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            f_cod = st.text_input("Código do Produto (exato)", key="apr_f_cod")
        with c2:
            f_tipo = st.text_input("Tipo do Produto (contém)", key="apr_f_tipo")
        with c3:
            f_insu = st.text_input("Insumo (contém)", key="apr_f_insu")

        mask_apr = pd.Series(True, index=df_apr.index)
        if f_cod:
            mask_apr &= df_apr.get("CODIGO_PRODUTO", pd.Series("", index=df_apr.index)).astype(str).eq(f_cod.strip())
        if f_tipo:
            mask_apr &= df_apr.get("TIPO_PRODUTO", pd.Series("", index=df_apr.index)).astype(str).str.contains(f_tipo, case=False, regex=False)
        if f_insu:
            mask_apr &= df_apr.get("INSUMO", pd.Series("", index=df_apr.index)).astype(str).str.contains(f_insu, case=False, regex=False)

        st.dataframe(
            df_apr[mask_apr],
            hide_index=True,
            use_container_width=True,
            column_config={
                "APROVADO_EM": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm")
            }
        )


with tab_corr:
    st.subheader("Itens para correção")
    FQN_COR = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_CORRECOES"

    try:
        df_cor = session.table(FQN_COR).sort("REPROVADO_EM", ascending=False).to_pandas()
    except Exception as e:
        st.error(f"Erro ao carregar correções: {e}")
        df_cor = pd.DataFrame()

    if df_cor.empty:
        st.info("Nenhum item reprovado.")
    else:
        # filtros
        c1, c2, c3 = st.columns(3)
        with c1:
            f_cod2 = st.text_input("Código do Produto (exato)", key="cor_f_cod")
        with c2:
            f_stat = st.selectbox("Status da correção",["— Todos —","PENDENTE","CORRIGIDO"], index=0, key="cor_f_stat")
        with c3:
            f_mot  = st.text_input("Motivo (contém)", key="cor_f_mot")

        mask_cor = pd.Series(True, index=df_cor.index)
        if f_cod2:
            mask_cor &= df_cor.get("CODIGO_PRODUTO", pd.Series("", index=df_cor.index)).astype(str).eq(f_cod2.strip())
        if f_stat != "— Todos —":
            mask_cor &= df_cor.get("STATUS_CORRECAO", pd.Series("", index=df_cor.index)).astype(str).eq(f_stat)
        if f_mot:
            mask_cor &= df_cor.get("MOTIVO", pd.Series("", index=df_cor.index)).astype(str).str.contains(f_mot, case=False, regex=False)

        # seleção de linhas
        df_cor_view = df_cor[mask_cor].copy()
        df_cor_view.insert(0, "Selecionar", False)

        edited_cor = st.data_editor(
            df_cor_view,
            num_rows="fixed", hide_index=True, use_container_width=True,
            column_config={
                "REPROVADO_EM": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm")
            },
            key="editor_correcao_page"
        )

        sel_ids = edited_cor.loc[edited_cor["Selecionar"] == True, "ID"].tolist()

        st.markdown("---")
        cA, cB = st.columns([2,1])
        with cA:
            novo_status = st.selectbox("Definir status selecionados para:", ["PENDENTE","CORRIGIDO"], index=0)
        with cB:
            if st.button("💾 Atualizar status", disabled=(len(sel_ids)==0), key="cor_btn_atualizar"):
                try:
                    ids_csv = ", ".join(str(i) for i in sel_ids)
                    session.sql(f"UPDATE {FQN_COR} SET STATUS_CORRECAO = '{novo_status}' WHERE ID IN ({ids_csv})").collect()
                    st.success(f"Atualizado(s) {len(sel_ids)} item(ns) para {novo_status}.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Falha ao atualizar status: {e}")

# ---------------- Utility ----------------
def sql_str(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "NULL"
    s = str(v).replace("'", "''")
    return f"'{s}'"

def apply_decision(session, df_items, user, ids, decisao: str, obs: str | None):
    """
    Audita e move os itens:
      - APROVADO  -> TB_CATALOGO_APROVADOS  (e apaga da principal)
      - REJEITADO -> TB_CATALOGO_CORRECOES  (e apaga da principal)
    Tudo em uma transação.
    """
    if not ids:
        return

    # garanta fuso de Brasília para timestamps LTZ
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

    # 2) Colunas comuns entre principal e destino
    def get_cols(table_fqn: str) -> list[str]:
        return [c.name for c in session.table(table_fqn).schema]

    cols_main = get_cols(FQN_MAIN)

    if decisao == "APROVADO":
        target = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_APROVADOS"
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
            SET APROVADO_POR   = {sql_str(user['username'])},
                NOME_VALIDADOR = {sql_str(user['name'])}
            WHERE ID IN ({ids_csv})
        """
        toast_icon = "✅"
        destino_legenda = "Aprovados"
    else:
        target = "BASES_SPDO.DB_APP_CATALOGO.TB_CATALOGO_CORRECOES"
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
            SET REPROVADO_POR  = {sql_str(user['username'])},
                NOME_VALIDADOR = {sql_str(user['name'])},
                MOTIVO         = {sql_str(obs)}
            WHERE ID IN ({ids_csv})
        """
        toast_icon = "❌"
        destino_legenda = "Correção"

    # 3) Remoção da principal
    sql_delete_main = f"DELETE FROM {FQN_MAIN} WHERE ID IN ({ids_csv})"

    # 4) Executa em transação
    try:
        session.sql("BEGIN").collect()
        session.sql(sql_ins_audit).collect()
        session.sql(sql_move_insert).collect()
        session.sql(sql_move_meta).collect()
        session.sql(sql_delete_main).collect()
        session.sql("COMMIT").collect()
        st.toast(f"{len(ids)} item(ns) movidos para {destino_legenda}.", icon=toast_icon)
    except Exception as e:
        session.sql("ROLLBACK").collect()
        st.error(f"Falha ao mover itens: {e}")
