import pandas as pd
import streamlit as st
from src.db_snowflake import get_session, insert_item
from src.utils import data_hoje, extrair_valores, campos_obrigatorios_ok, gerar_sinonimo, gerar_palavra_chave, _pick, _to_float_safe, _to_int_safe, gerar_template_excel_catalogo
from io import BytesIO
from src.auth import init_auth, is_authenticated, current_user

init_auth()
if not is_authenticated():
    st.error("Faça login para continuar.")
    st.stop()
    
st.set_page_config(page_title="Catálogo • Cadastro", layout="wide")
st.title("📝 Cadastro de Insumos")


session = get_session()

tab_form, tab_excel = st.tabs(["✍️ Formulário manual", "📥 Formulário Excel"])


with tab_form:
    with st.form("form_cadastro", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)

        with c1:
            referencia  = st.text_input("REFERENCIA")
            grupo       = st.text_input("GRUPO")
            categoria   = st.text_input("CATEGORIA")
            segmento    = st.text_input("SEGMENTO")
            familia     = st.text_input("FAMILIA")
            subfamilia  = st.text_input("SUBFAMILIA")

        with c2:
            tipo_codigo    = st.text_input("TIPO_CODIGO")
            codigo_produto = st.text_input("CODIGO_PRODUTO")
            insumo         = st.text_input("INSUMO")  # opcional
            item           = st.text_input("ITEM")
            especificacao  = st.text_area("ESPECIFICACAO (CHAVE: VALOR; ...)", height=122)

        with c3:
            marca            = st.text_input("MARCA")
            emb_produto      = st.text_input("EMB_PRODUTO")
            un_med           = st.text_input("UN_MED")
            qtd_med          = st.number_input("QTD_MED", min_value=0.00, step=0.01)
            emb_comercial    = st.text_input("EMB_COMERCIAL")
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
                    "SINONIMO": gerar_sinonimo(item, descricao, marca, qtd_med, un_med, emb_produto, qtd_emb_comercial, emb_comercial),
                    "PALAVRA_CHAVE": gerar_palavra_chave(subfamilia, item, marca, emb_produto, qtd_med, un_med, familia),
                }
                ok, msg = insert_item(session, item_dict)
                st.success(msg) if ok else st.error(msg)

# =========================
# 2) LEITOR EXCEL (somente leitura no preview; com botão para subir para Snowflake)
# =========================
with tab_excel:
    st.write("Carregue um arquivo **Excel** para visualizar e enviar os dados para o Snowflake.")

    st.subheader("📄 Template de Excel")
    st.caption("Baixe o modelo, preencha e depois envie no uploader abaixo.")
    st.download_button(
        "⬇️ Baixar template Excel",
        data=gerar_template_excel_catalogo(),
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
            "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
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
            "MARCA","EMB_PRODUTO","UN_MED","QTD_MED","EMB_COMERCIAL","QTD_EMB_COMERCIAL",
        ]

        # Cria coluna de erros por linha
        missing_list = []
        for _, row in df_out.iterrows():
            miss = [c for c in REQUIRED if str(row[c]).strip() == ""]
            # Valida também coerção numérica:
            if "QTD_MED" in row and str(row["QTD_MED"]).strip() != "" and to_float_ok(row["QTD_MED"]) is None:
                miss.append("QTD_MED (inválido)")
            if "QTD_EMB_COMERCIAL" in row and str(row["QTD_EMB_COMERCIAL"]).strip() != "" and to_int_ok(row["QTD_EMB_COMERCIAL"]) is None:
                miss.append("QTD_EMB_COMERCIAL (inválido)")
            missing_list.append(", ".join(miss))

        df_out["__ERROS__"] = missing_list
        has_errors = df_out["__ERROS__"].str.strip() != ""

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
            st.error(f"⚠️ Existem {int(has_errors.sum())} linha(s) com campos obrigatórios faltando/inválidos.")
            with st.expander("Ver apenas linhas com erro"):
                st.dataframe(df_errors.head(500), width="stretch")

            # (opcional) numera a linha original do Excel visualmente (1 = cabeçalho, então +2)
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
        can_upload_some = not df_valid.empty
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
                    # guarda a linha (1-based para humano) + mensagem de erro
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
