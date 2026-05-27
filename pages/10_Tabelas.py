import streamlit as st
import pandas as pd
import re
from src.db_snowflake import get_session
from src.auth import current_user

st.set_page_config(page_title="Catálogo • Tabelas", layout="wide")
st.title("📝 Tabelas")

session = get_session()

cols = ["GRUPO", "CATEGORIA", "SEGMENTO", "FAMILIA", "SUBFAMILIA", "TIPO_CODIGO",
        "MARCA", "FABRICANTE", "EMB_PRODUTO", "UN_MED", "EMB_COMERCIAL"]

tbs = [f'BASES_SPDO.DB_GESTAO_DADOS_EXTERNOS_APP_CATALOGO.TBL_CATALOGO_{c.upper()}_H' for c in cols]


def user_has_role(u: dict, role: str) -> bool:
    role = role.upper()
    r = u.get("role") or u.get("roles") or u.get("perfil")
    if isinstance(r, str):
        return r.upper() == role
    if isinstance(r, (list, tuple, set)):
        return any(str(x).upper() == role for x in r)
    return False

user = current_user()
is_admin = user_has_role(user, "ADMIN")

def _parse_fqn(fqn: str):
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"FQN inválido: {fqn} (esperado DB.SCHEMA.TABLE)")
    return parts[0], parts[1], parts[2]


def _esc_ident(name: str) -> str:
    if re.fullmatch(r"[A-Z_][A-Z0-9_]*", name.upper()):
        return name.upper()
    return f'"{name}"'


def _esc_str(s: str) -> str:
    return (s or "").replace("'", "''")


@st.cache_data(show_spinner=False, ttl=300)
def load_table(fqn: str) -> pd.DataFrame:
    return session.table(fqn).to_pandas()


def apply_changes(target_fqn: str, id_col: str, value_col: str, original: pd.DataFrame, edited: pd.DataFrame):
    """
    - UPDATE quando ID existe e value mudou
    - INSERT quando ID está vazio/nulo e value preenchido
    - (opcional) DELETE não implementado aqui (se quiser eu adiciono)
    """
    db, schema, table = _parse_fqn(target_fqn)
    full_table = f"{_esc_ident(db)}.{_esc_ident(schema)}.{_esc_ident(table)}"
    id_sql = _esc_ident(id_col)
    val_sql = _esc_ident(value_col)

    # normaliza
    o = original[[id_col, value_col]].copy()
    e = edited[[id_col, value_col]].copy()

    o[value_col] = o[value_col].astype(str).map(lambda x: x.strip())
    e[value_col] = e[value_col].astype(str).map(lambda x: x.strip())

    # --- UPDATES (mesmo ID, valor mudou)
    o_map = dict(zip(o[id_col].astype("Int64").dropna().astype(int), o[value_col]))
    e_ids = e[id_col].astype("Int64")

    updates = []
    for idx, row in e.iterrows():
        rid = row.get(id_col)
        if pd.isna(rid):
            continue
        rid = int(rid)
        new_val = (row.get(value_col) or "").strip()
        if not new_val:
            continue
        old_val = o_map.get(rid)
        if old_val is not None and new_val != old_val:
            updates.append((rid, new_val))

    # --- INSERTS (sem ID e com valor)
    inserts_vals = (
        e[e_ids.isna()][value_col]
        .astype(str)
        .map(lambda x: x.strip())
        .replace({"": None, "nan": None, "None": None}) # type: ignore
        .dropna()
        .tolist()
    )

    # dedup de inserts e também evita inserir algo que já existe
    existing_vals = set(o[value_col].astype(str).map(lambda x: x.strip()).tolist())
    inserts_unique = []
    seen = set()
    for v in inserts_vals:
        if v in existing_vals:
            continue
        if v not in seen:
            seen.add(v)
            inserts_unique.append(v)

    # executa UPDATES
    for rid, new_val in updates:
        session.sql(
            f"UPDATE {full_table} SET {val_sql} = '{_esc_str(new_val)}' WHERE {id_sql} = {rid}"
        ).collect()

    # executa INSERTS (ID autoincrementa)
    for v in inserts_unique:
        session.sql(
            f"INSERT INTO {full_table} ({val_sql}) VALUES ('{_esc_str(v)}')"
        ).collect()

    return len(updates), len(inserts_unique)


tabs = st.tabs(cols)

for tab, label, tb in zip(tabs, cols, tbs):
    with tab:
        df_db = load_table(tb)

        # identifica colunas
        if "ID" not in df_db.columns:
            st.error(f"A tabela {tb} não tem coluna ID. Você precisa criá-la com AUTOINCREMENT/IDENTITY.")
            st.stop()

        if label in df_db.columns:
            colname = label
        else:
            # fallback: pega a primeira coluna que não é ID
            non_id = [c for c in df_db.columns if c.upper() != "ID"]
            if len(non_id) != 1:
                st.error(f"Não consegui identificar a coluna principal da tabela {tb}. Colunas: {list(df_db.columns)}")
                st.stop()
            colname = non_id[0]

        # garante ordenação por ID e mantém cópia original para diff
        df_db = df_db.sort_values("ID", na_position="last").reset_index(drop=True)
        original = df_db.copy()
        st.write(f"Total de {colname}: **{len(df_db)}**")

        if is_admin:
            edited = st.data_editor(
                df_db,
                use_container_width=True,
                num_rows="dynamic",
                key=f"editor_{tb}",
                column_config={
                    "ID": st.column_config.NumberColumn("ID", disabled=True),
                },
                disabled=["ID"],  # redundante, mas ajuda
            )
        else: 
            st.dataframe(df_db, column_config={"ID": st.column_config.NumberColumn("ID", disabled=True)}, key=f"editor_{tb}")
            edited = df_db
        c1, c2 = st.columns([1, 3])
        with c1:
            
            if is_admin:
                if st.button("💾 Salvar alterações", key=f"save_{tb}"):
                    # validações simples
                    
                    if colname not in edited.columns:
                        st.error(f"Coluna {colname} não encontrada no editor.")
                    else:
                        try:
                            n_upd, n_ins = apply_changes(tb, "ID", colname, original, edited)
                            st.success(f"Salvo! Updates: {n_upd} • Novos itens: {n_ins}")
                            st.cache_data.clear()
                        except Exception as e:
                            st.error(f"Erro ao salvar: {e}")