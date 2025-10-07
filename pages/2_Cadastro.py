import pandas as pd
import streamlit as st
from src.db_snowflake import get_session, insert_item
from src.utils import data_hoje, extrair_valores, campos_obrigatorios_ok, gerar_sinonimo, gerar_palavra_chave, _pick, _to_float_safe, _to_int_safe, gerar_template_excel_catalogo

from src.auth import init_auth, is_authenticated

init_auth()
if not is_authenticated():
    st.error("Faça login para continuar.")
    st.stop()
    
st.set_page_config(page_title="Catálogo • Cadastro", layout="wide")
st.title("📝 Cadastro de Insumos")


session = get_session()

tab_form, tab_excel = st.tabs(["✍️ Formulário manual", "📥 Leitor Excel (somente leitura)"])

# =========================
# 1) FORMULÁRIO MANUAL (insere na base)
# =========================
with tab_form:
    with st.form("form_cadastro", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            referencia  = st.text_input("REFERENCIA")
            grupo       = st.text_input("GRUPO")
            categoria   = st.text_input("CATEGORIA")
            segmento    = st.text_input("SEGMENTO")
            familia     = st.text_input("FAMILIA")
            subfamilia  = st.text_input("SUBFAMILIA")

        with col2:
            codigo_produto = st.text_input("CODIGO DO PRODUTO")
            tipo_produto   = st.text_input("TIPO DO PRODUTO")  # <— NOVO CAMPO
            insumo         = st.text_input("INSUMO")
            item           = st.text_input("ITEM")
            especificacao  = st.text_area("ESPECIFICAÇÃO (CHAVE: VALOR; …)", height=122)
            marca          = st.text_input("MARCA")

        with col3:
            emb_produto       = st.text_input("EMBALAGEM DO PRODUTO")
            un_med            = st.text_input("UNIDADE DE MEDIDA")
            qtd_med           = st.number_input("QUANTIDADE DE MEDIDA", min_value=0.00, step=0.01)
            emb_comercial     = st.text_input("EMBALAGEM COMERCIAL")
            qtd_emb_comercial = st.number_input("QTD EMBALAGEM COMERCIAL", min_value=0, step=1)

        submitted = st.form_submit_button("💾 Salvar")

        if submitted:
            # Campos obrigatórios — MARCA e SUBFAMILIA podem ser vazios/hífen (não obrigue)
            obrig = {
                "GRUPO": grupo,
                "CATEGORIA": categoria,
                "SEGMENTO": segmento,
                "FAMILIA": familia,
                "CODIGO_PRODUTO": codigo_produto,
                "ITEM": item,
                "EMB_PRODUTO": emb_produto,
                "UN_MED": un_med,
                "QTD_MED": qtd_med,
                "EMB_COMERCIAL": emb_comercial,
                "QTD_EMB_COMERCIAL": qtd_emb_comercial,
                # "TIPO_PRODUTO": tipo_produto,  # deixe opcional; inclua aqui se quiser tornar obrigatório
            }
            ok, faltando = campos_obrigatorios_ok(obrig)
            if not ok:
                st.warning(f"Preencha: {', '.join(faltando)}")
            else:
                descricao = extrair_valores(especificacao)
                item_dict = {
                    "REFERENCIA": referencia,
                    "DATA_CADASTRO": data_hoje(),
                    "DATA_ATUALIZACAO": data_hoje(),
                    "GRUPO": grupo,
                    "CATEGORIA": categoria,
                    "SEGMENTO": segmento,
                    "FAMILIA": familia,
                    "SUBFAMILIA": subfamilia,
                    "CODIGO_PRODUTO": codigo_produto,
                    "TIPO_PRODUTO": tipo_produto,  # <— salvar no banco
                    "INSUMO": insumo,
                    "ITEM": item,
                    "DESCRICAO": descricao,
                    "ESPECIFICACAO": especificacao,
                    "MARCA": marca,
                    "EMB_PRODUTO": emb_produto,
                    "UN_MED": un_med,
                    "QTD_MED": float(qtd_med) if qtd_med is not None else None,
                    "EMB_COMERCIAL": emb_comercial,
                    "QTD_EMB_COMERCIAL": int(qtd_emb_comercial) if qtd_emb_comercial is not None else None,
                    # Usa as funções novas (com limpeza de hífen)
                    "SINONIMO": gerar_sinonimo(item, descricao, marca, qtd_med, un_med, emb_produto, qtd_emb_comercial, emb_comercial),
                    "PALAVRA_CHAVE": gerar_palavra_chave(subfamilia, item, marca, emb_produto, qtd_med, un_med, familia),
                }
                ok, msg = insert_item(session, item_dict)
                if ok: st.success(msg)
                else:  st.error(msg)

# =========================
# 2) LEITOR EXCEL (somente leitura no preview; com botão para subir para Snowflake)
# =========================
with tab_excel:
    st.write("Carregue um arquivo **Excel** para visualizar e enviar os dados para o Snowflake.")

    st.subheader("📄 Template de Excel")
    st.caption("Baixe o modelo, preencha (atenção ao bloco ESPECIFICACAO nas colunas 13..18) e depois envie no uploader abaixo.")
    st.download_button(
        "⬇️ Baixar template Excel",
        data=gerar_template_excel_catalogo(),
        file_name="template_catalogo.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.markdown("---")

    file = st.file_uploader("Enviar Excel (.xlsx ou .xls)", type=["xlsx", "xls"])
    if file is not None:
        # Lê sempre a primeira planilha, com header padrão do Excel (linha de cabeçalho)
        try:
            xls = pd.ExcelFile(file)
            sheet = xls.sheet_names[0]
            df_raw = pd.read_excel(file, sheet_name=sheet, header=0)
        except Exception as e:
            st.error(f"Erro ao ler o arquivo: {e}")
            st.stop()

        if df_raw.empty:
            st.warning("A planilha está vazia.")
            st.stop()

        # --- Regras por posição (1-based → 0-based)
        all_cols = list(df_raw.columns)
        if len(all_cols) < 18:
            st.error("A planilha precisa ter pelo menos 18 colunas para aplicar as regras (13 a 18).")
            st.stop()

        first5_cols = all_cols[:5]        # 1..5
        spec_block  = all_cols[12:18]     # 13..18

        # A LINHA 0 DE DADOS contém os nomes das especificações no bloco 13..18
        # (por isso, pegamos os 'headers' do bloco a partir de df_raw.loc[0, spec_block])
        spec_headers = [str(x).strip() for x in df_raw.loc[0, spec_block].tolist()] # type: ignore

        # Descarta a linha 0 (usada como cabeçalho do bloco de especificações)
        df_work = df_raw.drop(index=0).reset_index(drop=True)

        # Monta ESPECIFICACAO como lista de dicts [{CHAVE: VALOR}, ...]
        def build_specs_row(row, cols, headers):
            vals = row[cols].tolist()
            out = []
            for k, v in zip(headers, vals):
                if pd.notna(v) and str(v).strip() != "":
                    out.append({k: v})
            return out

        df_work["ESPECIFICACAO"] = df_work.apply(
            lambda r: build_specs_row(r, spec_block, spec_headers), axis=1
        )
        def specs_text(lst):
            if not isinstance(lst, list) or not lst:
                return ""
            parts = []
            for d in lst:
                for k, v in d.items():
                    parts.append(f"{k}: {v}")
            return "; ".join(parts)

        df_work["ESPECIFICACAO_TXT"] = df_work["ESPECIFICACAO"].apply(specs_text)
        df_work["SINONIMO"] = df_work.apply(
    lambda r: gerar_sinonimo(
        _pick(r, "ITEM"),
        r.get("DESCRICAO"),
        _pick(r, "MARCA"),
        _to_float_safe(_pick(r, "QTD_MED")),
        _pick(r, "UN_MED"),
        _pick(r, "EMB_PRODUTO"),
        _to_int_safe(_pick(r, "QTD_EMB_COMERCIAL")),
        _pick(r, "EMB_COMERCIAL"),
    ),
    axis=1
)

        df_work["PALAVRA_CHAVE"] = df_work.apply(
            lambda r: gerar_palavra_chave(
                _pick(r, "SUBFAMILIA"),
                _pick(r, "ITEM"),
                _pick(r, "MARCA"),
                _pick(r, "EMB_PRODUTO"),
                _to_float_safe(_pick(r, "QTD_MED")),
                _pick(r, "UN_MED"),
                _pick(r, "FAMILIA"),  # <— passe FAMILIA para o fallback da subfamília
            ),
            axis=1
        )

        # Remove 5 primeiras colunas + bloco 13..18 original
        cols_to_drop = first5_cols + spec_block
        df_out = df_work.drop(columns=cols_to_drop, errors="ignore")

        # Normaliza EAN/EAN_PRODUTO como string
        for ean_col in ["EAN", "CODIGO_PRODUTO"]:
            if ean_col in df_out.columns:
                df_out[ean_col] = df_out[ean_col].apply(
                    lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float))
                    else (str(x) if pd.notna(x) else "")
                )

        # Deixa nomes de colunas únicos (evita erro de duplicidade no Streamlit/Arrow)
        def make_unique(names):
            seen = {}
            out = []
            for n in names:
                n = str(n)
                if n not in seen:
                    seen[n] = 1
                    out.append(n)
                else:
                    seen[n] += 1
                    out.append(f"{n}_{seen[n]}")
            return out

        df_out.columns = make_unique(df_out.columns)

        # Preview
        st.success("Pré-visualização (nada foi salvo ainda).")
        st.write(f"**{len(df_out):,}** linha(s) × **{len(df_out.columns):,}** coluna(s) — planilha **{sheet}**.")
        st.dataframe(df_out.head(200), use_container_width=True)

        # --- Botão: Enviar para Snowflake (inserção em lote usando insert_item)
        st.markdown("---")
        if st.button("⬆️ Enviar dados para Snowflake"):
            total = len(df_out)
            ok_count = 0
            fails = []

            # mapeia nomes de colunas flexíveis -> nomes esperados na tabela
            def pick(row, *names):
                for n in names:
                    if n in row and pd.notna(row[n]) and str(row[n]).strip() != "":
                        return row[n]
                return None

            for idx, row in df_out.iterrows():
                item_dict = {
                    "REFERENCIA": pick(row, "REFERENCIA"),
                    "DATA_CADASTRO": data_hoje(),
                    "DATA_ATUALIZACAO": data_hoje(),
                    "GRUPO": pick(row, "GRUPO"),
                    "CATEGORIA": pick(row, "CATEGORIA"),
                    "SEGMENTO": pick(row, "SEGMENTO"),
                    "FAMILIA": pick(row, "FAMILIA"),
                    "SUBFAMILIA": pick(row, "SUBFAMILIA"),
                    "CODIGO_PRODUTO": pick(row, "CODIGO_PRODUTO", "EAN"),
                    "INSUMO": pick(row, "INSUMO"),
                    "ITEM": pick(row, "ITEM"),
                    "DESCRICAO": extrair_valores(pick(row, "ESPECIFICACAO_TXT") or ""),
                    "ESPECIFICACAO": pick(row, "ESPECIFICACAO_TXT"),
                    "MARCA": pick(row, "MARCA"),
                    "EMB_PRODUTO": pick(row, "EMB_PRODUTO"),
                    "UN_MED": pick(row, "UN_MED"),
                    "QTD_MED": pick(row, "QTD_MED"),
                    "EMB_COMERCIAL": pick(row, "EMB_COMERCIAL"),
                    "QTD_EMB_COMERCIAL": pick(row, "QTD_EMB_COMERCIAL"),
                    "TIPO_PRODUTO": pick(row, "TIPO_PRODUTO"),
                }

                ok, msg = insert_item(session, item_dict)
                if ok:
                    ok_count += 1
                else:
                    fails.append((idx, msg))

            if ok_count == total:
                st.success(f"✅ Todos os {ok_count} registro(s) foram inseridos com sucesso.")
            else:
                st.warning(f"Parcial: {ok_count}/{total} inseridos. {len(fails)} falharam.")
                with st.expander("Ver erros"):
                    for i, err in fails:
                        st.write(f"Linha {i+1}: {err}")
    else:
        st.info("Nenhum arquivo carregado ainda.")