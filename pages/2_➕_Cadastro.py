import io
import pandas as pd
import streamlit as st

# ====== SE VOCÊ USA SUAS FUNÇÕES ======
from src.db_snowflake import get_session, ensure_table, insert_item
from src.utils import data_hoje, extrair_valores, campos_obrigatorios_ok, gerar_sinonimo, gerar_palavra_chave

st.set_page_config(page_title="Catálogo • Cadastro", layout="wide")
st.title("📝 Cadastro de Insumos")

# ====== Sessão e tabela (apenas para o formulário manual) ======
session = get_session()
ensure_table(session)

# =========================
# Abas: Manual x Excel (somente leitura)
# =========================
tab_form, tab_excel = st.tabs(["✍️ Formulário manual", "📥 Leitor Excel (somente leitura)"])

# =========================
# 1) FORMULÁRIO MANUAL (insere na base)
# =========================
with tab_form:
    with st.form("form_cadastro", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            referencia = st.text_input("REFERENCIA")
            grupo = st.text_input("GRUPO")
            categoria = st.text_input("CATEGORIA")
            segmento = st.text_input("SEGMENTO")
            familia = st.text_input("FAMILIA")
            subfamilia = st.text_input("SUBFAMILIA")
        with col2:
            ean_produto = st.text_input("EAN DO PRODUTO")
            insumo = st.text_input("INSUMO")
            item = st.text_input("ITEM")
            especificacao = st.text_area("ESPECIFICAÇÃO (CHAVE: VALOR; …)", height=122)
            marca = st.text_input("MARCA")
        with col3:
            emb_produto = st.text_input("EMBALAGEM DO PRODUTO")
            un_med = st.text_input("UNIDADE DE MEDIDA")
            qtd_med = st.number_input("QUANTIDADE DE MEDIDA", min_value=0.00, step=0.01)
            emb_comercial = st.text_input("EMBALAGEM COMERCIAL")
            qtd_emb_comercial = st.number_input("QTD EMBALAGEM COMERCIAL", min_value=0, step=1)

        submitted = st.form_submit_button("💾 Salvar")

        if submitted:
            obrig = {
                "GRUPO": grupo,
                "CATEGORIA": categoria,
                "SEGMENTO": segmento,
                "FAMILIA": familia,
                "SUBFAMILIA": subfamilia,
                "EAN_PRODUTO": ean_produto,
                "ITEM": item,
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
                    "EAN_PRODUTO": ean_produto,
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
                    "SINONIMO": gerar_sinonimo(item, descricao, marca, qtd_med, un_med, emb_produto, qtd_emb_comercial, emb_comercial),
                    "PALAVRA_CHAVE": gerar_palavra_chave(subfamilia, item, marca, emb_produto, qtd_med, un_med)
                }
                ok, msg = insert_item(session, item_dict)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)

# =========================
# 2) LEITOR EXCEL (somente leitura no preview; com botão para subir para Snowflake)
# =========================
with tab_excel:
    st.write("Carregue um arquivo **Excel** para visualizar e enviar os dados para o Snowflake.")

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
        spec_headers = [str(x).strip() for x in df_raw.loc[0, spec_block].tolist()]

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

        # Versão texto "CHAVE: VALOR; ..."
        def specs_text(lst):
            if not isinstance(lst, list) or not lst:
                return ""
            parts = []
            for d in lst:
                for k, v in d.items():
                    parts.append(f"{k}: {v}")
            return "; ".join(parts)

        df_work["ESPECIFICACAO_TXT"] = df_work["ESPECIFICACAO"].apply(specs_text)

        # Remove 5 primeiras colunas + bloco 13..18 original
        cols_to_drop = first5_cols + spec_block
        df_out = df_work.drop(columns=cols_to_drop, errors="ignore")

        # Normaliza EAN/EAN_PRODUTO como string
        for ean_col in ["EAN", "EAN_PRODUTO"]:
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
                    # EAN pode vir como EAN ou EAN_PRODUTO
                    "EAN_PRODUTO": pick(row, "EAN_PRODUTO", "EAN"),
                    "INSUMO": pick(row, "INSUMO"),
                    "ITEM": pick(row, "ITEM"),
                    # DESCRICAO: se quiser os valores apenas, pode usar sua extrair_valores(ESPECIFICACAO_TXT)
                    "DESCRICAO": extrair_valores(pick(row, "ESPECIFICACAO_TXT") or ""),
                    # ESPECIFICACAO: texto legível
                    "ESPECIFICACAO": pick(row, "ESPECIFICACAO_TXT"),
                    "MARCA": pick(row, "MARCA"),
                    "EMB_PRODUTO": pick(row, "EMB_PRODUTO"),
                    "UN_MED": pick(row, "UN_MED"),
                    "QTD_MED": pick(row, "QTD_MED"),
                    "EMB_COMERCIAL": pick(row, "EMB_COMERCIAL"),
                    "QTD_EMB_COMERCIAL": pick(row, "QTD_EMB_COMERCIAL"),
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
