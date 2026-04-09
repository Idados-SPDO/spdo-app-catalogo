import streamlit as st
import pandas as pd
from src.db_snowflake import apply_common_filters, build_user_options, get_session, load_user_display_map
from src.auth import current_user, require_roles
from src.utils import extrair_valores, gerar_sinonimo, gerar_palavra_chave
from src.variables import FQN_MAIN, FQN_COR, FQN_APR

st.title("Não Aprovados")

require_roles("ADMIN", "OPERACIONAL")
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

def _recalc_palavra_chave_df_inplace(df: pd.DataFrame) -> pd.DataFrame:
    if "PALAVRA_CHAVE" not in df.columns:
        df["PALAVRA_CHAVE"] = ""
    df["PALAVRA_CHAVE"] = df.apply(lambda r: gerar_palavra_chave(
        r.get("SUBFAMILIA"), r.get("ITEM"), r.get("MARCA"),
        r.get("EMB_PRODUTO"), r.get("QTD_MED"), r.get("UN_MED"),
        r.get("FAMILIA"),
    ), axis=1)
    return df

def _persist_sinonimo_batch(session, table_fqn: str, df_ids: pd.DataFrame, id_col: str = "ID"):
    """
    Atualiza no banco em lote:
      - Atualiza DESCRICAO (apenas quando calculada nova)
      - Atualiza SINONIMO (sempre com o valor recalculado)
      - Atualiza PALAVRA_CHAVE (quando presente no df_ids)
    Usa CASE ... WHEN ... THEN ... para eficiência.
    """
    if df_ids.empty or id_col not in df_ids or "SINONIMO" not in df_ids:
        return

    work = df_ids[[id_col, "SINONIMO"]].copy()
    has_desc = "DESCRICAO" in df_ids.columns
    has_pc   = "PALAVRA_CHAVE" in df_ids.columns
    if has_desc:
        work["__DESC_APPLY__"] = df_ids["DESCRICAO"]
    if has_pc:
        work["__PC_APPLY__"] = df_ids["PALAVRA_CHAVE"]

    pairs = []
    for _, r in work.iterrows():
        _id  = int(r[id_col])
        _sin = "" if pd.isna(r["SINONIMO"]) else str(r["SINONIMO"])
        _desc = None if (not has_desc or pd.isna(r["__DESC_APPLY__"])) else str(r["__DESC_APPLY__"])
        _pc   = None if (not has_pc   or pd.isna(r["__PC_APPLY__"]))  else str(r["__PC_APPLY__"])
        pairs.append((_id, _sin, _desc, _pc))

    if not pairs:
        return

    ids_csv  = ", ".join(str(i)  for (i, _, __, ___) in pairs)
    when_sin = " ".join([f"WHEN {i} THEN {_sql_escape(s)}" for (i, s, _, __) in pairs])

    sets = [f"SINONIMO = CASE {id_col} {when_sin} END"]
    if has_desc:
        when_desc = " ".join([f"WHEN {i} THEN {_sql_escape(d) if d is not None else 'DESCRICAO'}"
                              for (i, _, d, __) in pairs])
        sets.append(f"DESCRICAO = CASE {id_col} {when_desc} END")
    if has_pc:
        when_pc = " ".join([f"WHEN {i} THEN {_sql_escape(p) if p is not None else 'PALAVRA_CHAVE'}"
                            for (i, _, __, p) in pairs])
        sets.append(f"PALAVRA_CHAVE = CASE {id_col} {when_pc} END")

    sql = f"""
        UPDATE {table_fqn}
        SET {", ".join(sets)}
        WHERE {id_col} IN ({ids_csv})
    """
    session.sql(sql).collect()

KEY_SELECTED = "cor_selected_keys"
KEY_EDITOR = "cor_table_editor"
KEY_SELECT_ALL = "cor_select_all_visible"
KEY_VISIBLE_KEYS = "cor_visible_row_keys"

FILTER_KEYS = [
    "cor_f_id",
    "cor_sel_user",
    "cor_sel_insumo_dd",
    "cor_sel_codigo_dd",
    "cor_f_palavra",
    "cor_sel_grupo_dd",
    "cor_sel_categoria_dd",
    "cor_sel_segmento_dd",
    "cor_sel_familia_dd",
    "cor_sel_subfamilia_dd",
]

def reset_catalogo_page_state():
    for k in FILTER_KEYS + [KEY_SELECTED, KEY_EDITOR, KEY_SELECT_ALL, KEY_VISIBLE_KEYS]:
        st.session_state.pop(k, None)
    st.rerun()

##### Filtros
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

        ####
        st.subheader("Filtros")
        if st.button("🧹 Limpar filtros", key="cor_btn_limpar_filtros"):
            reset_catalogo_page_state()

        # Séries normalizadas (para opções e comparação) — df_cor inteiro
        s_insumo = norm_str_series(df_cor["INSUMO"]) if "INSUMO" in df_cor.columns else pd.Series(pd.NA, index=df_cor.index, dtype="string")
        s_codigo = norm_str_series(df_cor["CODIGO_PRODUTO"], drop_dot_zero=True) if "CODIGO_PRODUTO" in df_cor.columns else pd.Series(pd.NA, index=df_cor.index, dtype="string")

        opt_insumo = dropdown_options(s_insumo)
        opt_codigo = dropdown_options(s_codigo)

        # =========================
        # Linha 1 (4 colunas)
        # ID | Usuário | Insumo | Código
        # =========================
        r1 = st.columns(4)
        with r1[0]:
            sel_id = st.text_input("ID", key="cor_f_id")
        with r1[1]:
            sel_user = st.selectbox(
                "Usuário (cadastro)",
                build_user_options(df_cor, user_map),
                index=0,
                key="cor_sel_user",
            )
        with r1[2]:
            sel_insumo = st.selectbox(
                "Insumo",
                opt_insumo,
                index=0,
                key="cor_sel_insumo_dd",
            )
        with r1[3]:
            sel_codigo = st.selectbox(
                "Código do Produto (exato)",
                opt_codigo,
                index=0,
                key="cor_sel_codigo_dd",
            )

        # =========================
        # Linha 2 (4 colunas)
        # Palavra-chave | Grupo | Categoria | Segmento
        # =========================
        r2 = st.columns(4)
        with r2[0]:
            f_palavra = st.text_input("Palavra-chave (contém)", key="cor_f_palavra")

        # 1) mask base: usuário + palavra
        mask = apply_common_filters(
            df_cor,
            sel_user_name=sel_user,
            f_insumo="",
            f_codigo="",
            f_palavra=f_palavra,
            user_map=user_map,
        )

        # 2) aplica Insumo/Código exatos no mask base
        mask = apply_dropdown_to_mask(mask, s_insumo, sel_insumo)
        mask = apply_dropdown_to_mask(mask, s_codigo, sel_codigo)

        # 3) aplica ID (exato)
        sel_id_norm = (sel_id or "").strip()
        if sel_id_norm:
            if "ID" in df_cor.columns and sel_id_norm.isdigit():
                mask = mask & (df_cor["ID"].astype("Int64") == int(sel_id_norm))
            else:
                st.warning("ID inválido. Use um número inteiro.")
                mask = mask & False

        # 4) escopo inicial para cascata
        df_scope = df_cor[mask].copy()

        with r2[1]:
            s_grupo_sc, opt_grupo_sc = _series_and_opts(df_scope, "GRUPO")
            sel_grupo = _selectbox_with_reset("Grupo", opt_grupo_sc, key="cor_sel_grupo_dd")
            df_scope = _apply_selected(df_scope, s_grupo_sc, sel_grupo)

        with r2[2]:
            s_cat_sc, opt_cat_sc = _series_and_opts(df_scope, "CATEGORIA")
            sel_categoria = _selectbox_with_reset("Categoria", opt_cat_sc, key="cor_sel_categoria_dd")
            df_scope = _apply_selected(df_scope, s_cat_sc, sel_categoria)

        with r2[3]:
            s_seg_sc, opt_seg_sc = _series_and_opts(df_scope, "SEGMENTO")
            sel_segmento = _selectbox_with_reset("Segmento", opt_seg_sc, key="cor_sel_segmento_dd")
            df_scope = _apply_selected(df_scope, s_seg_sc, sel_segmento)

        # =========================
        # Linha 3 (4 colunas)
        # Família | Subfamília | (vazio) | (vazio)
        # =========================
        r3 = st.columns(4)
        with r3[0]:
            s_fam_sc, opt_fam_sc = _series_and_opts(df_scope, "FAMILIA")
            sel_familia = _selectbox_with_reset("Família", opt_fam_sc, key="cor_sel_familia_dd")
            df_scope = _apply_selected(df_scope, s_fam_sc, sel_familia)

        with r3[1]:
            s_sub_sc, opt_sub_sc = _series_and_opts(df_scope, "SUBFAMILIA")
            sel_subfamilia = _selectbox_with_reset("Subfamília", opt_sub_sc, key="cor_sel_subfamilia_dd")
            df_scope = _apply_selected(df_scope, s_sub_sc, sel_subfamilia)

        with r3[2]:
            st.empty()
        with r3[3]:
            st.empty()

        # Resultado final já filtrado pela cascata
        df_cor_view = df_scope

        ####
        if df_cor_view.empty:
            st.info("Nenhum item com os filtros aplicados.")
        else:
            if "Selecionar" not in df_cor_view.columns:
                df_cor_view.insert(0, "Selecionar", False)
                
            cnt = session.sql(f"SELECT COUNT(*) AS N FROM {FQN_COR}").collect()[0]["N"]
            st.caption(f"Total reprovados no banco: **{cnt}**")
            
            left_sel_cor, right_sel_cor = st.columns([1, 3])
            with left_sel_cor:
                select_all_cor = st.checkbox("Selecionar todos", key="cor_select_all")
            if select_all_cor:
                df_cor_view["Selecionar"] = True
            
            if st.button("Recarregar tabela"):
                st.rerun()

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

            # ── Pré-processamento: recalcular SINONIMO/PALAVRA_CHAVE no estado do editor ──
            # Injeta os valores recalculados antes do st.data_editor para que apareçam
            # na mesma renderização em que o usuário editou um campo dependente.
            _EDITOR_KEY   = "editor_correcao_page"
            _DEPS_SINONIMO = {"ITEM","ESPECIFICACAO","MARCA","QTD_MED","UN_MED","EMB_PRODUTO","QTD_EMB_COMERCIAL","EMB_COMERCIAL","DESCRICAO"}
            _DEPS_PALAVRA  = {"SUBFAMILIA","ITEM","MARCA","EMB_PRODUTO","QTD_MED","UN_MED","FAMILIA"}

            _editor_state = st.session_state.get(_EDITOR_KEY, {})
            _edited_rows  = _editor_state.get("edited_rows", {})

            for _ri_str, _changes in list(_edited_rows.items()):
                _ri = int(_ri_str)
                _changed = set(_changes.keys())
                if not (_changed & (_DEPS_SINONIMO | _DEPS_PALAVRA)):
                    continue
                if _ri >= len(df_cor_view):
                    continue
                _orig   = df_cor_view.iloc[_ri].to_dict()
                _merged = {**_orig, **_changes}
                _row_edits = st.session_state[_EDITOR_KEY]["edited_rows"][_ri_str]

                if _changed & _DEPS_SINONIMO:
                    _desc_calc = _build_desc(_merged)
                    _row_edits["SINONIMO"] = gerar_sinonimo(
                        _merged.get("ITEM"),
                        _desc_calc or _merged.get("DESCRICAO", ""),
                        _merged.get("MARCA"),
                        _merged.get("QTD_MED"),
                        _merged.get("UN_MED"),
                        _merged.get("EMB_PRODUTO"),
                        _merged.get("QTD_EMB_COMERCIAL"),
                        _merged.get("EMB_COMERCIAL"),
                    )

                if _changed & _DEPS_PALAVRA:
                    _row_edits["PALAVRA_CHAVE"] = gerar_palavra_chave(
                        _merged.get("SUBFAMILIA"),
                        _merged.get("ITEM"),
                        _merged.get("MARCA"),
                        _merged.get("EMB_PRODUTO"),
                        _merged.get("QTD_MED"),
                        _merged.get("UN_MED"),
                        _merged.get("FAMILIA"),
                    )
            # ── fim pré-processamento ──

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
                        sel_df = _recalc_palavra_chave_df_inplace(sel_df)
                        _persist_sinonimo_batch(
                            session, FQN_COR,
                            sel_df[["ID","DESCRICAO","SINONIMO","PALAVRA_CHAVE"]],
                        )
                    except Exception as e:
                        st.warning(f"Falha ao sincronizar SINONIMO/PALAVRA_CHAVE antes do reenvio: {e}")
                    resend_to_validacao(session, edited_cor, sel_ids, user)
                    st.rerun()
