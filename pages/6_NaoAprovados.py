import streamlit as st
import pandas as pd
from src.db_snowflake import apply_common_filters, build_user_options, get_session, load_user_display_map
from src.auth import init_auth, is_authenticated, current_user
from src.utils import extrair_valores, gerar_sinonimo  # mesma base do 5_Atualizacao
from src.variables import FQN_MAIN, FQN_COR, FQN_APR

st.title("Não Aprovados")

init_auth()
if not is_authenticated():
    st.error("Faça login para continuar.")
    st.stop()
user = current_user()
session = get_session()


ORDER_CORRECOES = [
    "ID","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
    "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","DESCRICAO","ESPECIFICACAO",
    "MARCA","QTD_EMB_PRODUTO", "EMB_PRODUTO", "QTD_MED", "UN_MED", "QTD_EMB_COMERCIAL", "EMB_COMERCIAL",
    "SINONIMO","PALAVRA_CHAVE","REFERENCIA",
    "DATA_CADASTRO","USUARIO_CADASTRO",
    "DATA_REPROVACAO","USUARIO_REPROVACAO",      # novos
    "MOTIVO",
    "DATA_ATUALIZACAO","USUARIO_ATUALIZACAO",
]

EDITABLE_COR_COLS = [
    "GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA","TIPO_CODIGO","CODIGO_PRODUTO",
    "INSUMO","ITEM","DESCRICAO","ESPECIFICACAO","QTD_EMB_PRODUTO", "EMB_PRODUTO", 
    "QTD_MED", "UN_MED", "QTD_EMB_COMERCIAL", "EMB_COMERCIAL"
    ,"SINONIMO","PALAVRA_CHAVE","REFERENCIA"
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


def _sql_escape(val):
    import pandas as pd
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
        # monta um dict estilo "row_after" (igual 5_Atualizacao)
        row_after = row.to_dict()
        novo_desc = _build_desc(row_after)
        row_after["DESCRICAO"] = novo_desc
        return novo_desc, _build_sinonimo_like_update(row_after)

    out = df.apply(lambda r: pd.Series(_calc(r), index=["__DESC_NEW__", "__SIN_NEW__"]), axis=1)
    # aplica descrição nova apenas se a atual estiver vazia/nula (mesmo critério do helper)
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
    import pandas as pd
    if df_ids.empty or id_col not in df_ids or "SINONIMO" not in df_ids:
        return

    # Garantir colunas
    work = df_ids[[id_col, "SINONIMO"]].copy()
    has_desc = "DESCRICAO" in df_ids.columns
    if has_desc:
        work["__DESC_APPLY__"] = df_ids["DESCRICAO"]
    # prepara pares
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
        # Atualiza DESCRICAO somente quando veio calculada (evitar sobrepor se usuário alterou manualmente)
        when_desc = " ".join([f"WHEN {i} THEN {_sql_escape(d) if d is not None else 'DESCRICAO'}"
                              for (i, _, d) in pairs])
        sets.append(f"DESCRICAO = CASE {id_col} {when_desc} END")

    sql = f"""
        UPDATE {table_fqn}
        SET {", ".join(sets)}
        WHERE {id_col} IN ({ids_csv})
    """
    session.sql(sql).collect()


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

            df_cor_view = _recalc_sinonimo_df_inplace(df_cor_view)
            # (Opcional) persistir já em COR para refletir na base
            try:
                _persist_sinonimo_batch(session, FQN_COR, df_cor_view[["ID","DESCRICAO","SINONIMO"]])
            except Exception as e:
                st.warning(f"Não foi possível atualizar SINONIMO/descrição (correções): {e}")

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
                    try:
                        sel_df = edited_cor[edited_cor["ID"].isin(sel_ids)].copy()
                        sel_df = _recalc_sinonimo_df_inplace(sel_df)
                        _persist_sinonimo_batch(session, FQN_COR, sel_df[["ID","DESCRICAO","SINONIMO"]])
                    except Exception as e:
                        st.warning(f"Falha ao sincronizar SINONIMO antes do reenvio: {e}")
                    # <<< FIM >>>
                    resend_to_validacao(session, edited_cor, sel_ids, user)
                    st.rerun()
