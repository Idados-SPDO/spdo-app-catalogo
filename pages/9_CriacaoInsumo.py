import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime

from src.db_snowflake import get_session, listar_itens_df
from src.auth import require_roles, current_user
from src.variables import FQN_MAIN


# ==============================
# Helpers
# ==============================
def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")

def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "itens_sem_insumo") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.getvalue()

def sql_str(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "NULL"
    s = str(v).replace("'", "''")
    return f"'{s}'"

def _sql_escape(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + str(val).replace("'", "''") + "'"

def _persist_insumo_batch(session, table_fqn: str, df_before: pd.DataFrame, df_after: pd.DataFrame) -> int:
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
        (merged["INSUMO"] != merged["INSUMO_OLD"]) & (merged["INSUMO"] != "")
    , ["ID", "INSUMO"]].copy()

    if changed.empty:
        return 0

    cols_tbl = {c.name.upper() for c in session.table(table_fqn).schema}

    ids = [int(x) for x in changed["ID"].tolist()]
    ids_csv = ", ".join(str(i) for i in ids)

    when_ins = " ".join(
        [f"WHEN {int(r.ID)} THEN {_sql_escape(r.INSUMO)}" for r in changed.itertuples(index=False)]
    )

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
# Página
# ==============================
require_roles("ADMIN")

st.set_page_config(page_title="Catálogo • Criação de Insumo", layout="wide")
st.title("📦 Criação de Insumo")

session = get_session()
session.sql("ALTER SESSION SET TIMEZONE = 'America/Sao_Paulo'").collect()

df_all = listar_itens_df(session)
if df_all.empty:
    st.info("Nenhum item cadastrado ainda.")
    st.stop()

if "INSUMO" not in df_all.columns:
    st.error("A coluna INSUMO não existe no dataframe carregado (listar_itens_df).")
    st.stop()

df_missing = df_all[
    df_all["INSUMO"].isna() | df_all["INSUMO"].astype("string").str.strip().eq("")
].copy()

if df_missing.empty:
    st.success("Nenhum item pendente de preenchimento de INSUMO.")
    st.stop()

tab1, tab2 = st.tabs(["Visualização", "Importação/Exportação"])

with tab1:
    st.caption(f"Itens sem Insumo: **{len(df_missing)}**")

    # Colunas úteis para esta tela
    wanted_cols = [
        "ID", "CODIGO_PRODUTO", "ITEM", "DESCRICAO", "ESPECIFICACAO", "MARCA",
        "QTD_MED", "UN_MED", "EMB_PRODUTO",
        "INSUMO", "USUARIO_CADASTRO", "DATA_CADASTRO"
    ]
    cols_show = [c for c in wanted_cols if c in df_missing.columns]
    df_missing = df_missing[cols_show]

    # (Opcional) garantir QTD_MED numérico e formato limpo
    if "QTD_MED" in df_missing.columns:
        df_missing["QTD_MED"] = pd.to_numeric(df_missing["QTD_MED"], errors="coerce")

    col_cfg = {}
    for c in df_missing.columns:
        if c == "INSUMO":
            col_cfg[c] = st.column_config.TextColumn(
                label="INSUMO",
                help="Preencha o INSUMO (não pode ficar vazio).",
                required=True
            )
        elif c == "QTD_MED":
            col_cfg[c] = st.column_config.NumberColumn(format="%g", disabled=True)
        else:
            col_cfg[c] = st.column_config.Column(disabled=True)

    edited_insumo = st.data_editor(
        df_missing,
        num_rows="fixed",
        hide_index=True,
        use_container_width=True,
        key="editor_criacao_insumo",
        column_config=col_cfg,
    )

    st.markdown("---")
    if st.button("💾 Salvar INSUMOS", use_container_width=True):
        try:
            n = _persist_insumo_batch(session, FQN_MAIN, df_missing, edited_insumo)
            if n == 0:
                st.info("Nenhuma alteração válida detectada (ou INSUMO ficou vazio).")
            else:
                st.success(f"INSUMO atualizado para {n} item(ns).")
                st.rerun()
        except Exception as e:
            st.error(f"Falha ao salvar INSUMO: {e}")

with tab2:
    st.subheader("📥 Exportar itens sem INSUMO")

    export_cols = [
        "ID", "CODIGO_PRODUTO", "ITEM", "DESCRICAO", "ESPECIFICACAO", "MARCA",
        "QTD_MED", "UN_MED", "EMB_PRODUTO", "INSUMO"
    ]
    export_cols = [c for c in export_cols if c in df_missing.columns]
    df_export = df_missing[export_cols].copy()

    # Sugestão: deixar INSUMO em branco (template para preencher)
    if "INSUMO" in df_export.columns:
        df_export["INSUMO"] = ""

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    file_base = f"itens_sem_insumo_{ts}"

    c_dl1, c_dl2 = st.columns(2)

    with c_dl1:
        st.download_button(
            label="⬇️ Baixar CSV (template)",
            data=df_to_csv_bytes(df_export),
            file_name=f"{file_base}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with c_dl2:
        try:
            st.download_button(
                label="⬇️ Baixar Excel (template)",
                data=df_to_xlsx_bytes(df_export),
                file_name=f"{file_base}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception as e:
            st.info(f"Excel indisponível no ambiente (use CSV). Detalhe: {e}")



    st.markdown("---")
    st.subheader("📤 Importar planilha preenchida")

    up = st.file_uploader(
        "Faça upload do CSV/Excel preenchido (colunas obrigatórias: INSUMO e ID ou CODIGO_PRODUTO).",
        type=["csv", "xlsx"],
        key="uploader_insumo",
    )

    def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = (
            pd.Series(df.columns)
            .astype("string")
            .str.strip()
            .str.upper()
            .str.replace(r"\s+", "_", regex=True)
            .str.replace(r"[^A-Z0-9_]", "", regex=True)
            .tolist()
        )
        return df

    def _read_upload(file) -> pd.DataFrame:
        name = (file.name or "").lower()
        if name.endswith(".csv"):
            return pd.read_csv(file, sep=None, engine="python")
        if name.endswith(".xlsx"):
            return pd.read_excel(file)
        raise ValueError("Formato não suportado. Use CSV ou XLSX.")

    if up is not None:
        try:
            df_up_raw = _read_upload(up)
            df_up = _normalize_cols(df_up_raw)

            # Regras mínimas
            if "INSUMO" not in df_up.columns:
                st.error("A planilha precisa conter a coluna INSUMO.")
                st.stop()

            if ("ID" not in df_up.columns) and ("CODIGO_PRODUTO" not in df_up.columns):
                st.error("A planilha precisa conter ID ou CODIGO_PRODUTO (além de INSUMO).")
                st.stop()

            # Limpeza
            df_up["INSUMO"] = df_up["INSUMO"].astype("string").fillna("").str.strip()

            # Se não vier ID, resolve via CODIGO_PRODUTO usando o snapshot atual (df_all)
            if "ID" not in df_up.columns:
                map_cols = [c for c in ["CODIGO_PRODUTO", "ID"] if c in df_all.columns]
                df_map = df_all[map_cols].copy()
                df_map["CODIGO_PRODUTO"] = df_map["CODIGO_PRODUTO"].astype("string").fillna("").str.strip()
                df_up["CODIGO_PRODUTO"] = df_up["CODIGO_PRODUTO"].astype("string").fillna("").str.strip()

                df_up = df_up.merge(df_map, on="CODIGO_PRODUTO", how="left")

            # agora garantir ID
            df_up["ID"] = pd.to_numeric(df_up["ID"], errors="coerce")
            df_up = df_up.dropna(subset=["ID"]).copy()
            df_up["ID"] = df_up["ID"].astype(int)

            # Remover vazios de INSUMO (não faz sentido subir)
            df_up = df_up[df_up["INSUMO"].ne("")].copy()

            # Deduplicação por ID (pega a última ocorrência)
            df_up = df_up.sort_index().drop_duplicates(subset=["ID"], keep="last")

            # Aplicar somente para IDs que ainda estão sem INSUMO (controle)
            ids_missing = set(df_missing["ID"].astype(int).tolist()) if "ID" in df_missing.columns else set()
            df_up_in = df_up[df_up["ID"].isin(ids_missing)].copy()
            df_up_out = df_up[~df_up["ID"].isin(ids_missing)].copy()

            st.caption(f"Linhas válidas no upload: **{len(df_up)}**")
            st.caption(f"Linhas que batem com itens ainda sem INSUMO: **{len(df_up_in)}**")
            if not df_up_out.empty:
                st.warning(f"**{len(df_up_out)}** linha(s) foram ignoradas (ID não está mais pendente ou não existe).")

            st.write("Prévia do que será aplicado (ID x INSUMO):")
            st.dataframe(df_up_in[["ID", "INSUMO"]], use_container_width=True, hide_index=True)

            # Monta df_after alinhado ao df_missing (para reaproveitar _persist_insumo_batch)
            df_before_apply = df_missing[["ID", "INSUMO"]].copy()
            df_before_apply["INSUMO"] = df_before_apply["INSUMO"].astype("string").fillna("").str.strip()

            df_after_apply = df_before_apply.copy()
            map_ins = dict(zip(df_up_in["ID"], df_up_in["INSUMO"]))
            df_after_apply["INSUMO"] = df_after_apply.apply(
                lambda r: map_ins.get(int(r["ID"]), r["INSUMO"]),
                axis=1
            )

            st.markdown("### Aplicar importação")
            if st.button("🚀 Aplicar INSUMOS do upload no Catálogo", use_container_width=True, disabled=(len(df_up_in) == 0)):
                n = _persist_insumo_batch(session, FQN_MAIN, df_before_apply, df_after_apply)
                if n == 0:
                    st.info("Nenhuma alteração foi aplicada (nenhum INSUMO mudou ou estavam vazios).")
                else:
                    st.success(f"INSUMO atualizado para {n} item(ns) via upload.")
                    st.rerun()

        except Exception as e:
            st.error(f"Falha ao processar upload: {e}")
