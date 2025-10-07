# main.py
import streamlit as st
from pathlib import Path
from src.auth import init_auth, is_authenticated, current_user, logout_user

st.set_page_config(page_title="Catálogo de Insumos", layout="wide", initial_sidebar_state="expanded")

# logo
logo_path = Path("assets") / "logo_ibre.png"
if logo_path.exists():
    try:
        st.logo(str(logo_path))
    except Exception:
        st.sidebar.image(str(logo_path), use_column_width=True)

# auth
init_auth()

# 1) Defina as páginas
LOGIN_PAGE = [st.Page("pages/0_Login.py", title="🔐 Login")]
APP_PAGES  = [
    st.Page("pages/0_Home.py",        title="🏠 Início"),
    st.Page("pages/1_Catalogo.py",    title="📚 Catálogo"),
    st.Page("pages/2_Cadastro.py",    title="➕ Cadastro"),
    st.Page("pages/3_Atualizacao.py", title="🛠️ Atualização"),
    st.Page("pages/4_Validacao.py",   title="✅ Validação"),
]

# 2) Registre a navegação ANTES de usar page_link
nav = st.navigation({"Navegação": APP_PAGES} if is_authenticated() else {"Acesso": LOGIN_PAGE})

# 3) Sidebar global
with st.sidebar:
    if is_authenticated():
        u = current_user()
        st.caption("Logado como")
        st.markdown(f"**{u['name']}** (`{u['username']}`)")
        if st.button("🚪 Sair", use_container_width=True, key="logout_sidebar"):
            logout_user()
            st.rerun()
    else:
        st.info("Faça login para ver o menu.")
        # use o objeto Page já registrado, não a string do caminho
        st.page_link(LOGIN_PAGE[0], label="Ir para Login", icon="🔐")

# 4) Rode a navegação
nav.run()
