import streamlit as st
import pandas as pd
import oracledb
import datetime
import os
from dotenv import load_dotenv

# incarcare configurari
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DSN = os.getenv("DB_DSN")

if not DB_USER or not DB_PASSWORD:
    st.error("Eroare: Nu s-au gasit variabilele in fisierul .env")
    st.stop()

# configurare conexiune baza de date
@st.cache_resource
def get_connection():
    try:
        connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)
        return connection
    except Exception as e:
        st.error(f"Eroare conectare DB: {e}")
        return None

conn = get_connection()

# functii auxiliare pentru sql
def run_query(query, params=None, fetch_df=True):
    if conn is None: return None
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        if fetch_df:
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        else:
            conn.commit()
            return cursor.rowcount
    except Exception as e:
        st.error(f"Eroare SQL: {e}")
        return None
    finally:
        cursor.close()

def get_table_primary_key(table_name):
    sql = f"""
        SELECT COLUMN_NAME FROM ALL_CONS_COLUMNS A
        JOIN ALL_CONSTRAINTS C ON A.CONSTRAINT_NAME = C.CONSTRAINT_NAME
        WHERE C.TABLE_NAME = :t AND C.CONSTRAINT_TYPE = 'P'
    """
    df = run_query(sql, {'t': table_name})
    if not df.empty:
        return df.iloc[0]['COLUMN_NAME']
    return None

# interfata grafica principala
st.set_page_config(page_title="Gestiune Transport", layout="wide")
st.title("Sistem Gestiune Transport")

menu = st.sidebar.radio("Navigare", [
    "1. Gestiune Tabele (CRUD)", 
    "2. Interogari Complexe", 
    "3. On Delete Cascade Demo", 
    "4. Vizualizari"
])

# sectiunea 1 - operatii crud
if menu == "1. Gestiune Tabele (CRUD)":
    
    tables_df = run_query("SELECT table_name FROM user_tables ORDER BY table_name")
    
    if tables_df is not None and not tables_df.empty:
        table_name = st.selectbox("Selectare Tabel", tables_df['TABLE_NAME'])
        
        st.subheader(f"Date din {table_name}")
        data = run_query(f"SELECT * FROM {table_name}")
        st.dataframe(data, use_container_width=True)
        
        pk_col = get_table_primary_key(table_name)
        
        if pk_col:
            st.info(f"Cheie Primara identificata: {pk_col}")
            
            if not data.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Stergere Inregistrare")
                    id_to_delete = st.selectbox("Selecteaza ID de sters", data[pk_col].unique(), key='del_sel')
                    if st.button("Sterge"):
                        sql_del = f"DELETE FROM {table_name} WHERE {pk_col} = :id"
                        res = run_query(sql_del, {'id': int(id_to_delete)}, fetch_df=False)
                        if res: 
                            st.success("Sters cu succes! Da refresh.")
                            st.rerun()

                with col2:
                    st.subheader("Modificare Inregistrare")
                    id_to_edit = st.selectbox("Selecteaza ID de modificat", data[pk_col].unique(), key='edit_sel')
                    
                    filtered_data = data[data[pk_col] == id_to_edit]
                    
                    if not filtered_data.empty:
                        current_row = filtered_data.iloc[0]
                        
                        with st.form("edit_form"):
                            new_values = {}
                            cols_meta = run_query(f"SELECT column_name, data_type FROM user_tab_columns WHERE table_name = '{table_name}'")
                            
                            for index, row in cols_meta.iterrows():
                                c_name = row['COLUMN_NAME']
                                c_type = row['DATA_TYPE']
                                
                                if c_name == pk_col:
                                    continue
                                    
                                val = current_row[c_name]
                                
                                if 'DATE' in c_type or 'TIMESTAMP' in c_type:
                                    if isinstance(val, str):
                                        try: val = datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S').date()
                                        except: val = datetime.date.today()
                                    elif isinstance(val, pd.Timestamp):
                                        val = val.date()
                                    elif val is None:
                                        val = datetime.date.today()
                                    new_values[c_name] = st.date_input(c_name, value=val)
                                
                                elif 'NUMBER' in c_type or 'INTEGER' in c_type:
                                    new_values[c_name] = st.number_input(c_name, value=float(val) if val else 0.0)
                                
                                else:
                                    new_values[c_name] = st.text_input(c_name, value=str(val) if val else "")
                            
                            submitted = st.form_submit_button("Salveaza Modificari")
                            
                            if submitted:
                                set_clause = ", ".join([f"{k} = :{k}" for k in new_values.keys()])
                                sql_update = f"UPDATE {table_name} SET {set_clause} WHERE {pk_col} = :pk_val"
                                new_values['pk_val'] = int(id_to_edit)
                                
                                res = run_query(sql_update, new_values, fetch_df=False)
                                if res:
                                    st.success("Modificat cu succes!")
                                    st.rerun()
                    else:
                        st.warning("Nu s-au putut prelua datele.")
            else:
                st.warning("Tabelul este gol.")
        else:
            st.warning("Nu s-a putut identifica automat cheia primara.")

# sectiunea 2 - interogari complexe
elif menu == "2. Interogari Complexe":
    
    tab1, tab2 = st.tabs(["Cerere 3 Tabele & Filtre", "Cerere Group By & Having"])
    
    with tab1:
        st.markdown("### Raport Vehicule Mercedes cu Costuri Mari")
        sql_c = """
        SELECT M.TIP_MENTENANTA, M.DESCRIERE, M.COST, V.MODEL, V.MARCA, V.NR_KILOMETRI,
               CASE WHEN A2.VEHICUL_ID IS NOT NULL THEN 'Autocar'
                    WHEN T.VEHICUL_ID IS NOT NULL THEN 'TIR' END AS TIP_VEHICUL
        FROM MENTENANTA M
        JOIN VEHICUL V on M.VEHICUL_ID = V.VEHICUL_ID
        LEFT JOIN AUTOCAR A2 on V.VEHICUL_ID = A2.VEHICUL_ID
        LEFT JOIN TIR T on V.VEHICUL_ID = T.VEHICUL_ID
        WHERE V.MARCA = 'Mercedes' AND M.COST >= 300
        """
        st.code(sql_c, language='sql')
        df_c = run_query(sql_c)
        st.dataframe(df_c)

    with tab2:
        st.markdown("### Locatii cu Trafic Intens de Pasageri")
        sql_d = """
        SELECT C.LOC_PLECARE,
               SUM(C.COST_TOTAL)   AS "COST_TOTAL",
               SUM(TP.NR_PASAGERI) AS "NR_PASAGERI"
        FROM CURSA C
        JOIN TRANSPORT_PERSOANE TP on C.CURSA_ID = TP.CURSA_ID
        GROUP BY C.LOC_PLECARE
        HAVING SUM(TP.NR_PASAGERI) > 10
        """
        st.code(sql_d, language='sql')
        df_d = run_query(sql_d)
        st.dataframe(df_d)

# sectiunea 3 - on delete cascade
elif menu == "3. On Delete Cascade Demo":
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Vehicule")
        df_v = run_query("SELECT VEHICUL_ID, MARCA, MODEL FROM VEHICUL")
        st.dataframe(df_v)
        
    with col2:
        st.subheader("Mentenanta")
        df_m = run_query("SELECT MENTENANTA_ID, VEHICUL_ID, DESCRIERE FROM MENTENANTA")
        st.dataframe(df_m)
        
    st.divider()
    
    if df_v is not None and not df_v.empty:
        vehicul_to_delete = st.selectbox("Alege ID Vehicul", df_v['VEHICUL_ID'])
        
        count_mentenanta = df_m[df_m['VEHICUL_ID'] == vehicul_to_delete].shape[0]
        st.write(f"Vehiculul {vehicul_to_delete} are **{count_mentenanta}** mentenante.")
        
        if st.button("Executa Stergere"):
            run_query(f"DELETE FROM VEHICUL WHERE VEHICUL_ID = {vehicul_to_delete}", fetch_df=False)
            st.success("Vehicul sters.")
            
            check_df = run_query(f"SELECT * FROM MENTENANTA WHERE VEHICUL_ID = {vehicul_to_delete}")
            
            if check_df.empty:
                st.success("Confirmare: MENTENANTA a fost stearsa automat (Cascade).")
            else:
                st.error("Eroare: Mentenanta nu a fost stearsa.")
            
            st.button("Refresh")

# sectiunea 4 - vizualizari
elif menu == "4. Vizualizari":
    st.header("Utilizarea Vizualizarilor")
    
    mode = st.radio("Tip Vizualizare", ["Vizualizare Compusa (Permite Update)", "Vizualizare Complexa (Doar Citire)"], horizontal=True)
    
    if mode == "Vizualizare Compusa (Permite Update)":
        st.subheader("V_FLOTA_TIRURI")
        
        df_view = run_query("SELECT * FROM V_FLOTA_TIRURI")
        st.dataframe(df_view)
        
        if df_view is not None and not df_view.empty:
            st.markdown("#### Testare Update prin View")
            row_id = st.selectbox("Alege Vehicul ID", df_view['VEHICUL_ID'])
            new_weight = st.number_input("Noua Greutate Maxima", value=20.0)
            
            if st.button("Executa UPDATE"):
                sql_upd = "UPDATE V_FLOTA_TIRURI SET GREUTATE_MAXIMA = :g WHERE VEHICUL_ID = :id"
                res = run_query(sql_upd, {'g': new_weight, 'id': int(row_id)}, fetch_df=False)
                if res:
                    st.success("Update realizat cu succes!")
                    st.rerun()
    else:
        st.subheader("V_RAPORT_COSTURI_VEHICUL")
        df_view_c = run_query("SELECT * FROM V_RAPORT_COSTURI_VEHICUL")
        st.dataframe(df_view_c)