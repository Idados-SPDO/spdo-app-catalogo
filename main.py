# main.py
import streamlit as st
from pathlib import Path
from src.auth import init_auth, is_authenticated, current_user, logout_user

st.set_page_config(
    page_title="Catálogo de Insumos",
    layout="wide",
    initial_sidebar_state="expanded",
)

# logo
logo_path = Path("assets") / "logo_ibre.png"
if logo_path.exists():
    try:
        st.logo(str(logo_path))
    except Exception:
        st.sidebar.image(str(logo_path), use_container_width=True)

init_auth()

LOGIN_PAGE = st.Page("pages/0_Login.py", title="🔐 Login")

PAGES = {
    "home":         st.Page("pages/1_Home.py",         title="🏠 Início"),
    "catalogo":     st.Page("pages/4_Catalogo.py",     title="📚 Catálogo"),
    "cadastro":     st.Page("pages/2_Cadastro.py",     title="➕ Cadastro"),
    "validacao":    st.Page("pages/3_Validacao.py",    title="✅ Validação"),
    "nao_aprovados":st.Page("pages/6_NaoAprovados.py", title="❌ Não Aprovados"),
    "atualizacao":  st.Page("pages/5_Atualizacao.py",  title="🛠️ Atualização"),
    "exclusao": st.Page("pages/7_Exclusao.py", title="🗑️ Exclusão"),
    "usuarios": st.Page("pages/8_Usuarios.py", title="👤 Usuários")
}

ROLE_MATRIX = {
    "USER":        ["home", "catalogo"],
    "OPERACIONAL": ["home", "catalogo", "cadastro", "nao_aprovados"],
    "ADMIN":       list(PAGES.keys()),
}

def pages_for_role(role: str):
    role = (role or "USER").upper().strip()
    keys = ROLE_MATRIX.get(role, ROLE_MATRIX["USER"])
    return [PAGES[k] for k in keys]

if is_authenticated():
    u = current_user()
    role = u.get("role", "USER")
    app_pages = pages_for_role(role)
    nav = st.navigation({"Navegação": app_pages})
else:
    nav = st.navigation({"Acesso": [LOGIN_PAGE]})

# 4) Sidebar global
with st.sidebar:
    if is_authenticated():
        u = current_user()
        role = (u.get("role") or "USER").upper()
        st.caption("Logado como")
        st.markdown(f"**{u['name']}** (`{u['username']}`)")
        st.caption(f"Permissão: **{role}**")

        if st.button("🚪 Sair", use_container_width=True, key="logout_sidebar"):
            logout_user()
            st.rerun()
    else:
        st.info("Faça login para ver o menu.")
        st.page_link(LOGIN_PAGE, label="Login", icon="🔐")

# 5) Rode a navegação
nav.run()
