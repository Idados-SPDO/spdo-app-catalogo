import pandas as pd
import streamlit as st
import xlsxwriter
from src.db_snowflake import codigo_produto_exists_any, fetch_existing_codigos_dual, get_session, insert_item
from src.utils import data_hoje, extrair_valores, campos_obrigatorios_ok, gerar_sinonimo, gerar_palavra_chave, _pick, _to_float_safe, _to_int_safe, gerar_template_excel_catalogo
from io import BytesIO
from src.auth import current_user, require_roles
import numpy as np

from src.variables import FQN_TBL_GRUPO, FQN_TBL_CATEGORIA, FQN_TBL_SEGMENTO, FQN_TBL_FAMILIA, FQN_TBL_SUBFAMILIA, FQN_TBL_TIPO_CODIGO,FQN_TBL_MARCA, FQN_TBL_EMB_PRODUTO , FQN_TBL_UN_MED , FQN_TBL_EMB_COMERCIAL

require_roles("OPERACIONAL", "ADMIN")

    
st.set_page_config(page_title="Catálogo • Cadastro", layout="wide")
st.title("📝 Cadastro de Insumos")


def append_reason(df: pd.DataFrame, mask: pd.Series, reason: str) -> None:
    """Concatena 'reason' na coluna EXPLICAÇÃO apenas nas linhas do mask,
    adicionando vírgula quando já existirem erros anteriores (vetorizado)."""
    s = df.loc[mask, "EXPLICAÇÃO"].astype(str).fillna("").str.strip()
    df.loc[mask, "EXPLICAÇÃO"] = np.where(
        s != "",
        s + ", " + reason,
        reason
    )

session = get_session()

@st.cache_data(show_spinner=False, ttl=600)
def get_catalog_options(table):
    df = session.table(table).to_pandas()
    label_cols = [c for c in df.columns if c.upper() != "ID"]
    if len(label_cols) != 1:
        raise ValueError(f"Tabela {table}: colunas inesperadas {list(df.columns)}")
    label_col = label_cols[0]
    return df[label_col].dropna().astype(str).str.strip().tolist()

tab_form, tab_excel = st.tabs(["✍️ Formulário manual", "📥 Formulário Excel"])


with tab_form:
    with st.form("form_cadastro", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)

        with c1:
            referencia  = st.text_input("REFERENCIA")
            grupo       = st.selectbox("GRUPO",get_catalog_options(FQN_TBL_GRUPO))
            categoria   = st.selectbox("CATEGORIA",get_catalog_options(FQN_TBL_CATEGORIA))
            segmento    = st.selectbox("SEGMENTO",get_catalog_options(FQN_TBL_SEGMENTO))
            familia     = st.selectbox("FAMILIA",get_catalog_options(FQN_TBL_FAMILIA))
            subfamilia  = st.selectbox("SUBFAMILIA",get_catalog_options(FQN_TBL_SUBFAMILIA))


        with c2:
            tipo_codigo    = st.selectbox("TIPO_CODIGO",get_catalog_options(FQN_TBL_TIPO_CODIGO))
            codigo_produto = st.text_input("CODIGO_PRODUTO")
            insumo         = st.text_input("INSUMO")  # opcional
            item           = st.text_input("ITEM")
            especificacao  = st.text_area("ESPECIFICACAO (CHAVE: VALOR; ...)", height=122)
            qtd_emb_produto= st.number_input("QTD_EMB_PRODUTO",  min_value=0, step=1)

        with c3:
            marca            = st.selectbox("MARCA",get_catalog_options(FQN_TBL_MARCA))
            emb_produto      = st.selectbox("EMB_PRODUTO",get_catalog_options(FQN_TBL_EMB_PRODUTO))
            un_med           = st.selectbox("UN_MED",get_catalog_options(FQN_TBL_UN_MED))
            qtd_med          = st.number_input("QTD_MED", min_value=0.00, step=0.01)
            emb_comercial    = st.selectbox("EMB_COMERCIAL",get_catalog_options(FQN_TBL_EMB_COMERCIAL))
            qtd_emb_comercial= st.number_input("QTD_EMB_COMERCIAL", min_value=0, step=1)

        submitted = st.form_submit_button("💾 Salvar")

        if submitted:
            # Apenas NÃO obrigatórios: REFERENCIA, INSUMO
            obrig = {
                "GRUPO": grupo,
                "CATEGORIA": categoria,
                "SEGMENTO": segmento,
                "FAMILIA": familia,
                "SUBFAMILIA": subfamilia,
                "TIPO_CODIGO": tipo_codigo,
                "CODIGO_PRODUTO": codigo_produto,
                "ITEM": item,
                "ESPECIFICACAO": especificacao,
                "MARCA": marca,
                "EMB_PRODUTO": emb_produto,
                "UN_MED": un_med,
                "QTD_MED": qtd_med,
                "EMB_COMERCIAL": emb_comercial,
                "QTD_EMB_COMERCIAL": qtd_emb_comercial,
                "QTD_EMB_PRODUTO": qtd_emb_produto,
            }
            ok, faltando = campos_obrigatorios_ok(obrig)
            if not ok:
                st.warning(f"Preencha: {', '.join(faltando)}")
            else:
                
                user = current_user()
                usuario_atual = user["name"] if user and "name" in user else None
                descricao = extrair_valores(especificacao)
                item_dict = {
                    "REFERENCIA": referencia or None,
                    "USUARIO_CADASTRO": usuario_atual,
                    "DATA_CADASTRO": data_hoje(),
                    "GRUPO": grupo,
                    "CATEGORIA": categoria,
                    "SEGMENTO": segmento,
                    "FAMILIA": familia,
                    "SUBFAMILIA": subfamilia,
                    "TIPO_CODIGO": tipo_codigo,
                    "CODIGO_PRODUTO": codigo_produto,
                    "INSUMO": insumo or None,
                    "ITEM": item,
                    "DESCRICAO": descricao,
                    "ESPECIFICACAO": especificacao,
                    "MARCA": marca,
                    "EMB_PRODUTO": emb_produto,
                    "UN_MED": un_med,
                    "QTD_MED": float(qtd_med) if qtd_med is not None else None,
                    "EMB_COMERCIAL": emb_comercial,
                    "QTD_EMB_COMERCIAL": int(qtd_emb_comercial) if qtd_emb_comercial is not None else None,
                    "QTD_EMB_PRODUTO": int(qtd_emb_produto) if qtd_emb_produto is not None else None,
                    "SINONIMO": gerar_sinonimo(item, descricao, marca, qtd_med, un_med, emb_produto, qtd_emb_comercial, emb_comercial),
                    "PALAVRA_CHAVE": gerar_palavra_chave(subfamilia, item, marca, emb_produto, qtd_med, un_med, familia),
                }
                codigo_norm = (codigo_produto or "").strip()
                exists, origem = codigo_produto_exists_any(session, codigo_norm)
                if exists:
                    if origem == "APROVADOS":
                        st.error(f"CODIGO_PRODUTO '{codigo_norm}' já está APROVADO. Não é permitido novo cadastro.")
                    else:
                        st.error(f"CODIGO_PRODUTO '{codigo_norm}' já existe em pendências. Ajuste e tente novamente.")
                else:
                    ok, msg = insert_item(session, item_dict)
                    st.success(msg) if ok else st.error(msg)

# =========================
# 2) LEITOR EXCEL (somente leitura no preview; com botão para subir para Snowflake)

# =========================
        EXPECTED = [
            "REFERENCIA","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
            "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","ESPECIFICACAO",
            "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL", "QTD_EMB_PRODUTO"
        ]
        def gerar_template_excel_catalogo_com_dropdowns(session) -> bytes:
            options_map = {
                "GRUPO": get_catalog_options(FQN_TBL_GRUPO),
                "CATEGORIA": get_catalog_options(FQN_TBL_CATEGORIA),
                "SEGMENTO": get_catalog_options(FQN_TBL_SEGMENTO),
                "FAMILIA": get_catalog_options(FQN_TBL_FAMILIA),
                "SUBFAMILIA": get_catalog_options(FQN_TBL_SUBFAMILIA),
                "TIPO_CODIGO": get_catalog_options(FQN_TBL_TIPO_CODIGO),
                "MARCA": get_catalog_options(FQN_TBL_MARCA),
                "EMB_PRODUTO": get_catalog_options(FQN_TBL_EMB_PRODUTO),
                "UN_MED": get_catalog_options(FQN_TBL_UN_MED),
                "EMB_COMERCIAL": get_catalog_options(FQN_TBL_EMB_COMERCIAL),
            }

            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                wb = writer.book

                ws = wb.add_worksheet("Modelo")
                ws_listas = wb.add_worksheet("LISTAS")
                ws_listas.hide()

                # escreve cabeçalho do template
                for col_idx, col_name in enumerate(EXPECTED):
                    ws.write(0, col_idx, col_name)

                # cria listas + named ranges + validação
                max_rows = 5000  # até onde o dropdown vale
                for list_col_idx, (col_name, values) in enumerate(options_map.items()):
                    # escreve a lista na aba escondida
                    ws_listas.write(0, list_col_idx, col_name)
                    for r, v in enumerate(values, start=1):
                        ws_listas.write(r, list_col_idx, v)

                    # named range (Excel não aceita referência direta a outra aba em validação, por isso nomeamos)
                    col_letter = xlsxwriter.utility.xl_col_to_name(list_col_idx) # type: ignore
                    first = f"LISTAS!${col_letter}$2"
                    last  = f"LISTAS!${col_letter}${len(values)+1}"
                    range_name = f"LIST_{col_name}"
                    wb.define_name(range_name, f"={first}:{last}")

                    # aplica validação na coluna do template
                    target_col_idx = EXPECTED.index(col_name)
                    ws.data_validation(
                        1, target_col_idx, max_rows, target_col_idx,  # linha 2 até max_rows+1
                        {
                            "validate": "list",
                            "source": f"={range_name}",
                            "error_title": "Valor inválido",
                            "error_message": "Selecione um valor da lista.",
                        }
                    )

                # salva
                writer.sheets["Modelo"] = ws
                writer.sheets["LISTAS"] = ws_listas

            return buf.getvalue()
        

with tab_excel:
    st.write("Carregue um arquivo **Excel** para visualizar e enviar os dados para o Snowflake.")

    st.subheader("📄 Template de Excel")
    st.caption("Baixe o modelo, preencha e depois envie no uploader abaixo.")
    st.download_button(
        "⬇️ Baixar template Excel",
        data=gerar_template_excel_catalogo_com_dropdowns(session),
        file_name="template_catalogo.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.markdown("---")

    file = st.file_uploader("Enviar Excel (.xlsx ou .xls)", type=["xlsx", "xls"])
    if file is not None:
        try:
            df_out = pd.read_excel(file, sheet_name=0, dtype=str).fillna("")
        except Exception as e:
            st.error(f"Erro ao ler o arquivo: {e}")
            st.stop()

        # Garante todas as colunas (mesma ordem do template)
        EXPECTED = [
            "REFERENCIA","GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
            "TIPO_CODIGO","CODIGO_PRODUTO","INSUMO","ITEM","ESPECIFICACAO",
            "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL", "QTD_EMB_PRODUTO"
        ]
        
        for c in EXPECTED:
            if c not in df_out.columns:
                df_out[c] = ""

        df_out = df_out[EXPECTED]


        def to_float_ok(x):
            try:
                return float(str(x).replace(",", "."))
            except:
                return None

        def to_int_ok(x):
            try:
                return int(float(str(x)))
            except:
                return None

        # Validação de obrigatórios (todos exceto REFERENCIA e INSUMO)
        REQUIRED = [
            "GRUPO","CATEGORIA","SEGMENTO","FAMILIA","SUBFAMILIA",
            "TIPO_CODIGO","CODIGO_PRODUTO","ITEM","ESPECIFICACAO",
            "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL", "QTD_EMB_PRODUTO"
        ]

        df_out["CODIGO_PRODUTO"] = df_out["CODIGO_PRODUTO"].astype(str).str.strip()

        # 3.1) Duplicados DENTRO DO ARQUIVO
        dups_in_file_mask = df_out.duplicated(subset=["CODIGO_PRODUTO"], keep=False) & (df_out["CODIGO_PRODUTO"] != "")

        # 3.2) Duplicados NA BASE (apenas para códigos não vazios)
        codigos_unicos_arquivo = sorted({c for c in df_out["CODIGO_PRODUTO"].tolist() if str(c).strip()})
        exist_pend, exist_aprv = fetch_existing_codigos_dual(session, codigos_unicos_arquivo)

        dups_in_db_pend_mask = df_out["CODIGO_PRODUTO"].isin(exist_pend)
        dups_in_db_aprv_mask = df_out["CODIGO_PRODUTO"].isin(exist_aprv)
        # Cria coluna de erros por linha
        missing_list = []
        motivos_dup = []
        for _, row in df_out.iterrows():
            miss = [c for c in REQUIRED if str(row[c]).strip() == ""]
            # coerção numérica
            if "QTD_MED" in row and str(row["QTD_MED"]).strip() != "" and to_float_ok(row["QTD_MED"]) is None:
                miss.append("QTD_MED (inválido)")
            if "QTD_EMB_COMERCIAL" in row and str(row["QTD_EMB_COMERCIAL"]).strip() != "" and to_int_ok(row["QTD_EMB_COMERCIAL"]) is None:
                miss.append("QTD_EMB_COMERCIAL (inválido)")
            if "QTD_EMB_PRODUTO" in row and str(row["QTD_EMB_PRODUTO"]).strip() != "" and to_int_ok(row["QTD_EMB_PRODUTO"]) is None:
                miss.append("QTD_EMB_PRODUTO (inválido)")

            missing_list.append(", ".join(miss))
            motivos_dup.append("") 

        df_out["EXPLICAÇÃO"] = missing_list
        
        allowed_map = {
            "GRUPO": get_catalog_options(FQN_TBL_GRUPO),
            "CATEGORIA": get_catalog_options(FQN_TBL_CATEGORIA),
            "SEGMENTO": get_catalog_options(FQN_TBL_SEGMENTO),
            "FAMILIA": get_catalog_options(FQN_TBL_FAMILIA),
            "SUBFAMILIA": get_catalog_options(FQN_TBL_SUBFAMILIA),
            "TIPO_CODIGO": get_catalog_options(FQN_TBL_TIPO_CODIGO),
            "MARCA": get_catalog_options(FQN_TBL_MARCA),
            "EMB_PRODUTO": get_catalog_options(FQN_TBL_EMB_PRODUTO),
            "UN_MED": get_catalog_options(FQN_TBL_UN_MED),
            "EMB_COMERCIAL": get_catalog_options(FQN_TBL_EMB_COMERCIAL),
        }

        # (opcional) comparação case-insensitive:
        def _norm_series(s: pd.Series) -> pd.Series:
            return s.astype(str).str.strip()  # use .str.upper() se quiser ignorar maiúsc/minúsc

        def _norm_set(values: list[str]) -> set[str]:
            return {str(v).strip() for v in values}  # use .upper() idem

        for col, allowed_vals in allowed_map.items():
            allowed = _norm_set(allowed_vals)
            s = _norm_series(df_out[col])
            invalid_mask = (s != "") & (~s.isin(list(allowed)))
            append_reason(df_out, invalid_mask, f"{col} fora do catálogo (dropdown)")
            
        append_reason(df_out, dups_in_file_mask, "CODIGO_PRODUTO duplicado no arquivo")

        append_reason(df_out, dups_in_db_aprv_mask, "CODIGO_PRODUTO já existe em APROVADOS")

        append_reason(df_out, dups_in_db_pend_mask, "CODIGO_PRODUTO já existe em PENDENTES")

        has_errors = df_out["EXPLICAÇÃO"].astype(str).str.strip() != ""

        st.success("Pré-visualização (nada foi salvo ainda).")
        st.write(f"**{len(df_out):,}** linha(s) × **{len(df_out.columns):,}** coluna(s).")
        st.dataframe(df_out.head(200), width="stretch")

        st.markdown("---")

        # --- máscaras de validade
        valid_mask = ~(has_errors)
        df_valid = df_out.loc[valid_mask].copy()
        df_errors = df_out.loc[has_errors].copy()

        # --- Download do Excel com erros (se houver)
        if not df_errors.empty:
            st.error(f"⚠️ Existem {int(has_errors.sum())} linha(s) com problemas (obrigatórios/numéricos/duplicidades).")
            with st.expander("Ver apenas linhas com erro"):
                st.dataframe(df_errors.head(500), width="stretch")

            # numeração da linha original do Excel (2 = cabeçalho + índice base-1)
            df_errors.insert(0, "__LINHA_EXCEL__", df_errors.reset_index().index + 2)

            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
                df_errors.to_excel(w, index=False, sheet_name="Erros")
            st.download_button(
                "⬇️ Baixar planilha com erros",
                data=buf.getvalue(),
                file_name="catalogo_erros.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.success("✅ Nenhum erro encontrado no arquivo.")

        # --- Botão: Enviar apenas as linhas válidas
        #     (fica desabilitado apenas se NÃO houver linhas válidas)
        can_upload_some = (not df_valid.empty) and (not has_errors.any())
        if st.button("⬆️ Enviar apenas linhas válidas", disabled=not can_upload_some):
            total_valid = len(df_valid)
            ok_count, fails = 0, []
            user = current_user()
            usuario_atual = user["name"] if user and "name" in user else None

            def to_float_ok(x):
                try:
                    return float(str(x).replace(",", "."))
                except:
                    return None

            def to_int_ok(x):
                try:
                    return int(float(str(x)))
                except:
                    return None

            for idx, row in df_valid.iterrows():
                item_dict = {
                    "REFERENCIA": row["REFERENCIA"] or None,
                    "DATA_CADASTRO": data_hoje(),
                    "USUARIO_CADASTRO": usuario_atual,
                    "GRUPO": row["GRUPO"],
                    "CATEGORIA": row["CATEGORIA"],
                    "SEGMENTO": row["SEGMENTO"],
                    "FAMILIA": row["FAMILIA"],
                    "SUBFAMILIA": row["SUBFAMILIA"],
                    "TIPO_CODIGO": row["TIPO_CODIGO"],
                    "CODIGO_PRODUTO": row["CODIGO_PRODUTO"],
                    "INSUMO": row["INSUMO"] or None,
                    "ITEM": row["ITEM"],
                    "DESCRICAO": extrair_valores(row["ESPECIFICACAO"]),
                    "ESPECIFICACAO": row["ESPECIFICACAO"],
                    "MARCA": row["MARCA"],
                    "EMB_PRODUTO": row["EMB_PRODUTO"],
                    "UN_MED": row["UN_MED"],
                    "QTD_MED": to_float_ok(row["QTD_MED"]),
                    "EMB_COMERCIAL": row["EMB_COMERCIAL"],
                    "QTD_EMB_COMERCIAL": to_int_ok(row["QTD_EMB_COMERCIAL"]),
                    "QTD_EMB_PRODUTO": to_int_ok(row["QTD_EMB_PRODUTO"]),
                    "SINONIMO": gerar_sinonimo(
                        row["ITEM"],
                        extrair_valores(row["ESPECIFICACAO"]),
                        row["MARCA"],
                        to_float_ok(row["QTD_MED"]),
                        row["UN_MED"],
                        row["EMB_PRODUTO"],
                        to_int_ok(row["QTD_EMB_COMERCIAL"]),
                        row["EMB_COMERCIAL"],
                    ),
                    "PALAVRA_CHAVE": gerar_palavra_chave(
                        row["SUBFAMILIA"],
                        row["ITEM"],
                        row["MARCA"],
                        row["EMB_PRODUTO"],
                        to_float_ok(row["QTD_MED"]),
                        row["UN_MED"],
                        row["FAMILIA"],
                    ),
                }

                ok, msg = insert_item(session, item_dict)
                if ok:
                    ok_count += 1
                else:
                    fails.append((idx + 2, msg))  # type: ignore # +2: cabeçalho + base 1

            if ok_count == total_valid:
                st.success(f"✅ Inseridos {ok_count}/{total_valid} registros válidos.")
            else:
                st.warning(f"Parcial: {ok_count}/{total_valid} válidos inseridos. {len(fails)} falharam.")
                with st.expander("Ver erros de inserção (linhas válidas)"):
                    for linha_excel, err in fails:
                        st.write(f"Linha {linha_excel}: {err}")

        # informação quando não há válidos
        if df_valid.empty:
            st.info("Nenhuma linha válida para inserir (corrija o Excel de erros e tente novamente).")
    else:
        st.info("Nenhum arquivo carregado ainda.")
