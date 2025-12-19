# pages/0_Login.py
import streamlit as st
from src.auth import init_auth, login_user, is_authenticated, current_user, logout_user
from src.db_snowflake import (
    get_session,
    users_list_usernames,
    users_update_password,
    users_get,
    users_create_or_update,  # 👈 NOVO
)

st.set_page_config(page_title="Login • Catálogo", layout="wide")
init_auth()

st.title("🔐 Login")

session = get_session()

# =========================
# Se já estiver logado
# =========================
if is_authenticated():
    user = current_user()
    st.success(f"Você já está logado como **{user['name']}** ({user['username']}).")
    if st.button("🚪 Sair", key="btn_logout_login_page"):
        logout_user()
        st.rerun()
    st.info("Use o menu superior/esquerdo para navegar.")

# =========================
# Não autenticado
# =========================
else:
    # carrega usuários existentes
    usernames = users_list_usernames(session)

    # -------- Estado dos modais --------
    if "open_pwd" not in st.session_state:
        st.session_state["open_pwd"] = False
    if "pwd_nonce" not in st.session_state:
        st.session_state["pwd_nonce"] = 0

    if "open_create_user" not in st.session_state:
        st.session_state["open_create_user"] = False
    if "create_user_nonce" not in st.session_state:
        st.session_state["create_user_nonce"] = 0

    # =========================
    # 2.1) Nenhum usuário cadastrado
    # =========================
    if not usernames:
        st.warning(
            "Nenhum usuário cadastrado ainda. "
            "Crie o primeiro usuário (ex.: um ADMIN) para começar a usar o sistema."
        )

        if st.button("➕ Criar primeiro usuário", key="btn_open_create_user_bootstrap"):
            st.session_state["open_create_user"] = True
            st.session_state["create_user_nonce"] += 1
            st.rerun()

    # =========================
    # 2.2) Já existem usuários → login normal
    # =========================
    else:
        # --- SELETOR DE USUÁRIO ---
        st.subheader("Acesso")
        username_login = st.selectbox("Usuário", usernames, key="login_sel_user")

        # --- Formulário de login ---
        with st.form("login_form"):
            senha_login = st.text_input("Senha", type="password", key="login_pwd_curr")
            submitted_login = st.form_submit_button("Entrar", use_container_width=True)

        if submitted_login:
            u = (username_login or "").strip()
            p = (senha_login or "").strip()
            if not p:
                st.warning("Informe a senha.")
            else:
                if login_user(u, p):
                    info = users_get(session, u)
                    st.success(f"Bem-vindo(a), {info['name']}!")
                    st.rerun()
                else:
                    st.error("Usuário ou senha inválidos.")

        # -------- Botões extras (troca de senha / criar usuário) --------
        chosen_user = st.session_state.get("login_sel_user", usernames[0])

        cols_btn = st.columns(2)
        with cols_btn[0]:
            if st.button("🔧 Trocar senha do usuário selecionado", key="btn_open_pwd"):
                st.session_state["open_pwd"] = True
                st.session_state["pwd_nonce"] += 1
        with cols_btn[1]:
            if st.button("➕ Criar novo usuário", key="btn_open_create_user"):
                st.session_state["open_create_user"] = True
                st.session_state["create_user_nonce"] += 1

    # =========================
    # Dialog: Trocar senha
    # =========================
    @st.dialog("Trocar senha")
    def change_pwd_dialog(user_to_change: str):
        st.write(f"Usuário: **{user_to_change}**")

        nonce = st.session_state.get("pwd_nonce", 0)
        key_pwd  = f"dlg_new_pwd_{nonce}"
        key_conf = f"dlg_new_conf_{nonce}"

        new_pwd  = st.text_input("Nova senha", type="password", key=key_pwd)
        new_conf = st.text_input("Confirmar nova senha", type="password", key=key_conf)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Salvar", type="primary", key=f"dlg_save_{nonce}"):
                n = (new_pwd or "").strip()
                c = (new_conf or "").strip()
                if not n or not c:
                    st.warning("Preencha a nova senha e a confirmação."); return
                if n != c:
                    st.warning("Nova senha e confirmação não conferem."); return
                if len(n) < 4:
                    st.warning("A nova senha deve ter pelo menos 4 caracteres."); return
                try:
                    users_update_password(session, user_to_change.strip(), n)
                    st.success("Senha atualizada com sucesso!")
                    st.session_state["open_pwd"] = False
                    st.session_state["pwd_nonce"] += 1
                    st.rerun()
                except Exception as e:
                    st.error(f"Falha ao atualizar senha: {e}")
        with col2:
            if st.button("Cancelar", key=f"dlg_cancel_{nonce}"):
                st.session_state["open_pwd"] = False
                st.session_state["pwd_nonce"] += 1
                st.rerun()

    # =========================
    # Dialog: Criar usuário
    # =========================
    @st.dialog("Criar usuário")
    def create_user_dialog():
        nonce = st.session_state.get("create_user_nonce", 0)

        key_uname = f"dlg_create_username_{nonce}"
        key_name  = f"dlg_create_name_{nonce}"
        key_role  = f"dlg_create_role_{nonce}"
        key_pwd   = f"dlg_create_pwd_{nonce}"
        key_conf  = f"dlg_create_conf_{nonce}"

        st.write("Preencha os dados do novo usuário:")

        username = st.text_input("Username (login)", key=key_uname)
        name     = st.text_input("Nome completo", key=key_name)
        role     = st.text_input("Papel (ROLE)", value="USER", key=key_role)
        pwd      = st.text_input("Senha", type="password", key=key_pwd)
        conf     = st.text_input("Confirmar senha", type="password", key=key_conf)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Criar usuário", type="primary", key=f"dlg_create_{nonce}"):
                u = (username or "").strip()
                n = (name or "").strip()
                r = (role or "").strip() or "USER"
                p = (pwd or "").strip()
                c = (conf or "").strip()

                if not u or not n or not p:
                    st.warning("Preencha username, nome e senha."); return
                if p != c:
                    st.warning("Senha e confirmação não conferem."); return
                if len(p) < 4:
                    st.warning("A senha deve ter pelo menos 4 caracteres."); return

                # Evita sobrescrever usuário existente sem querer
                existing = set(users_list_usernames(session))
                if u in existing:
                    st.warning("Este username já está cadastrado."); return

                try:
                    # Cria usuário com hash/salt no banco
                    users_create_or_update(session, u, n, r, p)

                    # Login automático após criação
                    if login_user(u, p):
                        st.success(f"Usuário **{n}** criado e logado com sucesso!")
                    else:
                        st.success("Usuário criado com sucesso, mas o login automático falhou. Tente entrar manualmente.")

                    st.session_state["open_create_user"] = False
                    st.session_state["create_user_nonce"] += 1
                    st.rerun()
                except Exception as e:
                    st.error(f"Falha ao criar usuário: {e}")

        with col2:
            if st.button("Cancelar", key=f"dlg_cancel_create_{nonce}"):
                st.session_state["open_create_user"] = False
                st.session_state["create_user_nonce"] += 1
                st.rerun()

    # --- Chamada dos dialogs, se flags estiverem ligadas ---
    if st.session_state.get("open_pwd") and usernames:
        # só faz sentido trocar senha se já houver usuários
        chosen_user = st.session_state.get("login_sel_user", usernames[0])
        change_pwd_dialog(chosen_user)

    if st.session_state.get("open_create_user"):
        create_user_dialog()
