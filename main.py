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

PAGE_META = {
    "home": {
        "page": st.Page("pages/1_Home.py", title="🏠 Início"),
        "module": "Módulo - Visualização",
    },
    "catalogo": {
        "page": st.Page("pages/4_Catalogo.py", title="📚 Catálogo"),
        "module": "Módulo - Visualização",
    },
    "cadastro": {
        "page": st.Page("pages/2_Cadastro.py", title="➕ Cadastro"),
        "module": "Módulo - Operacional",
    },
    "criacao_insumo": {
        "page": st.Page("pages/9_CriacaoInsumo.py", title="📦 Criação de Insumos"),
        "module": "Módulo - Operacional",
    },
    "nao_aprovados": {
        "page": st.Page("pages/6_NaoAprovados.py", title="❌ Não Aprovados"),
        "module": "Módulo - Operacional",
    },
    "validacao": {
        "page": st.Page("pages/3_Validacao.py", title="✅ Validação"),
        "module": "Módulo - Operacional",
    },
    "atualizacao": {
        "page": st.Page("pages/5_Atualizacao.py", title="🛠️ Atualização"),
        "module": "Módulo - Admin",
    },
    "exclusao": {
        "page": st.Page("pages/7_Exclusao.py", title="🗑️ Exclusão"),
        "module": "Módulo - Admin",
    },
    "usuarios": {
        "page": st.Page("pages/8_Usuarios.py", title="👤 Usuários"),
        "module": "Módulo - Admin",
    },
    "tabelas": {
        "page": st.Page("pages/10_Tabelas.py", title="👤 Tabelas"),
        "module": "Módulo - Operacional",
    },
}


ROLE_MATRIX = {
    "USER":        ["home", "catalogo"],
    "OPERACIONAL": ["home", "catalogo", "cadastro", "criacao_insumo","nao_aprovados", "validacao", "tabelas"],
    "ADMIN":       list(PAGE_META.keys()),
}

MODULE_ORDER = ["Módulo - Visualização", "Módulo - Operacional", "Módulo - Admin"]

def nav_for_role(role: str) -> dict:
    role = (role or "USER").upper().strip()
    allowed_keys = ROLE_MATRIX.get(role, ROLE_MATRIX["USER"])

    grouped = {m: [] for m in MODULE_ORDER}
    for k in allowed_keys:
        meta = PAGE_META.get(k)
        if not meta:
            continue
        grouped[meta["module"]].append(meta["page"])

    return {m: pages for m, pages in grouped.items() if pages}

if is_authenticated():
    u = current_user()
    role = u.get("role", "USER")
    nav = st.navigation(nav_for_role(role))
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
