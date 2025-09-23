import streamlit as st
from pathlib import Path
from src.db_snowflake import get_session, ensure_table
import sys
import os
sys.path.append(os.path.abspath('.'))

st.set_page_config(
    page_title="Catálogo de Insumos",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- logo local (asset/...) ---
logo_path = Path("assets") / "logo_ibre.png"   # ajuste o nome do arquivo
if logo_path.exists():
    st.logo(str(logo_path))
else:
    st.warning(f"Logo não encontrada em {logo_path}. Coloque sua imagem nessa pasta ou ajuste o caminho.")

st.title("📦 Catálogo de Insumos")
st.caption("Multipage app • Snowflake + Snowpark")

# --- (opcional) esconder menu padrão do multipage ---
st.markdown(
    """
    <style>
      /* algumas versões usam div/nav/section com esse testid */
      div[data-testid="stSidebarNav"], nav[data-testid="stSidebarNav"], section[data-testid="stSidebarNav"] {
        display: none !important;
      }
    </style>
    """,
    unsafe_allow_html=True
)

# --- navegação customizada (lateral) ---
with st.sidebar:
    st.header("Navegação")
    # ajuste os nomes dos arquivos conforme os seus em pages/
    st.page_link("main.py", label="🏠 Index")            # esta própria página
    st.page_link("pages/1_📚_Catálogo.py",  label="📚 Catálogo")
    st.page_link("pages/2_➕_Cadastro.py",  label="➕ Cadastro")
    st.page_link("pages/3_🛠️_Atualização.py", label="🛠️ Atualização")

session = get_session()
ensure_table(session)
st.success("Conexão com Snowflake ativa e tabela garantida.")

st.markdown(
    """
    ### Dicas
    - Use **➕ Cadastro** para inserir novos itens.
    - Consulte e filtre em **📚 Catálogo**.
    - Atualize registros em **🛠️ Atualização**.
    - Este projeto usa **Snowpark**; em ambiente Snowflake, a sessão ativa é detectada automaticamente.
    """
)
