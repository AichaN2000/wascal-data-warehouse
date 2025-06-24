import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import numpy as np

# Configuration de la page
st.set_page_config(
    page_title="WASCAL Data Warehouse - Reporting",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration des utilisateurs
USERS = {
    "admin": "wascal2024",
    "analyste": "data123",
    "utilisateur": "user123"
}

# Configuration de connexion PostgreSQL
DB_CONFIG = {
    "host": "wascal-datawarehouse.ce5k6qqm8o1c.us-east-1.rds.amazonaws.com",
    "port": "5432",
    "database": "postgres",
    "user": "wascal_admin",
    "password": st.secrets["postgres_password"]
}

# CSS personnalisé complet - Thème Bleu et Blanc
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Variables CSS - Thème Bleu et Blanc avec texte amélioré */
    :root {
        --primary-color: #1e3a8a;
        --secondary-color: #3b82f6;
        --accent-color: #60a5fa;
        --success-color: #10b981;
        --warning-color: #f59e0b;
        --danger-color: #ef4444;
        --main-bg: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 50%, #93c5fd 100%);
        --card-bg: #ffffff;
        --sidebar-bg: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%);
        --text-primary: #1e40af;
        --text-secondary: #1e3a8a;
        --text-dark: #0f172a;
        --text-white: #ffffff;
        --border-color: #e2e8f0;
        --shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);
        --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
    }
    
    /* Fond principal de l'application */
    .main {
        font-family: 'Inter', sans-serif;
        background: var(--main-bg) !important;
        color: var(--text-primary) !important;
        min-height: 100vh;
    }
    
    .stApp {
        background: var(--main-bg) !important;
    }
    
    /* Sidebar en bleu */
    .css-1d391kg {
        background: var(--sidebar-bg) !important;
        border-right: 3px solid var(--primary-color) !important;
    }
    
    .css-17eq0hr {
        background: var(--sidebar-bg) !important;
    }
    
    /* Forcer la sidebar à rester ouverte */
    .css-1cypcdb {
        width: 21rem !important;
        min-width: 21rem !important;
    }
    
    /* Style pour les boutons de navigation */
    .stButton > button {
        background: var(--card-bg) !important;
        color: var(--text-primary) !important;
        border: 2px solid var(--card-bg) !important;
        border-radius: 12px !important;
        padding: 0.75rem 1rem !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        transition: all 0.3s ease !important;
        box-shadow: var(--shadow) !important;
        margin-bottom: 0.5rem !important;
        width: 100% !important;
    }

    .stButton > button:hover {
        background: rgba(255, 255, 255, 0.9) !important;
        color: var(--primary-color) !important;
        border-color: var(--text-white) !important;
        transform: translateY(-2px) !important;
        box-shadow: var(--shadow-lg) !important;
    }

    .stButton > button:active,
    .stButton > button:focus {
        background: rgba(255, 255, 255, 0.8) !important;
        color: var(--primary-color) !important;
        border-color: var(--text-white) !important;
        transform: translateY(0) !important;
    }
    
    /* Header principal compact - SEULEMENT sur dashboard */
    .main-header {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 50%, #60a5fa 100%);
        padding: 1.5rem 2rem;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        text-align: center;
        color: var(--text-white);
        box-shadow: var(--shadow-lg);
        position: relative;
        overflow: hidden;
        border: 1px solid var(--primary-color);
    }
    
    .main-header::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><defs><pattern id="grain" width="100" height="100" patternUnits="userSpaceOnUse"><circle cx="25" cy="25" r="1" fill="white" opacity="0.1"/><circle cx="75" cy="75" r="1" fill="white" opacity="0.1"/></pattern></defs><rect width="100" height="100" fill="url(%23grain)"/></svg>');
        opacity: 0.3;
    }
    
    .main-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        letter-spacing: -0.02em;
        position: relative;
        z-index: 1;
        color: var(--text-white);
    }
    
    .main-subtitle {
        font-size: 1rem;
        opacity: 0.95;
        font-weight: 400;
        max-width: 700px;
        margin: 0 auto;
        position: relative;
        z-index: 1;
        line-height: 1.4;
        color: var(--text-white);
    }
    
    /* PAGE DE CONNEXION - Compacte */
    .login-container {
        max-width: 450px;
        margin: 3rem auto;
        background: var(--card-bg);
        padding: 2rem;
        border-radius: 20px;
        border: 3px solid var(--primary-color);
        box-shadow: var(--shadow-lg);
        text-align: center;
    }
    
    .login-header {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%);
        padding: 1.5rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        color: var(--text-white);
    }
    
    .login-title {
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0.3rem;
    }
    
    .login-subtitle {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    
    .login-form-title {
        font-size: 1.8rem;
        font-weight: 600;
        color: var(--primary-color);
        margin-bottom: 0.5rem;
    }
    
    .login-form-subtitle {
        color: var(--text-secondary);
        margin-bottom: 1.5rem;
        font-size: 1rem;
    }
    
    /* Comptes de demo compacts */
    .demo-accounts {
        margin-top: 1.5rem;
        padding: 1rem;
        background: rgba(30, 58, 138, 0.05);
        border-radius: 10px;
        border: 1px solid var(--primary-color);
        text-align: left;
    }
    
    .demo-title {
        color: var(--primary-color);
        font-weight: 600;
        margin-bottom: 0.5rem;
        font-size: 1rem;
    }
    
    .demo-account {
        margin: 0.3rem 0;
        color: var(--text-secondary);
        font-size: 0.85rem;
    }
    
    /* Status indicators dans la sidebar */
    .status-success {
        color: var(--text-white);
        font-weight: 600;
        background: rgba(16, 185, 129, 0.3);
        padding: 0.5rem 1rem;
        border-radius: 8px;
        border: 1px solid rgba(255, 255, 255, 0.3);
        margin-bottom: 1rem;
    }
    
    .status-error {
        color: var(--text-white);
        font-weight: 600;
        background: rgba(239, 68, 68, 0.3);
        padding: 0.5rem 1rem;
        border-radius: 8px;
        border: 1px solid rgba(255, 255, 255, 0.3);
        margin-bottom: 1rem;
    }
    
    /* Info connexion sidebar */
    .connection-info {
        background: rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 1rem;
        margin: 1rem 0;
        border: 1px solid rgba(255, 255, 255, 0.2);
    }
    
    .connection-info h4 {
        color: var(--text-white) !important;
        margin-bottom: 0.5rem !important;
        font-size: 1.1rem !important;
    }
    
    .connection-info p {
        color: rgba(255, 255, 255, 0.9) !important;
        margin: 0.2rem 0 !important;
        font-size: 0.9rem !important;
    }
    
    /* Cards métriques avec fond blanc */
    .metrics-container {
        background: var(--card-bg);
        border-radius: 16px;
        border: 2px solid var(--primary-color);
        box-shadow: var(--shadow-lg);
        padding: 2rem;
        margin-bottom: 2rem;
    }
    
    .metrics-title {
        color: var(--text-primary);
        font-size: 1.5rem;
        font-weight: 600;
        margin-bottom: 1.5rem;
        text-align: center;
        border-bottom: 2px solid var(--primary-color);
        padding-bottom: 0.5rem;
    }
    
    /* Section containers avec fond blanc */
    .section-container {
        background: var(--card-bg);
        border-radius: 16px;
        border: 2px solid var(--primary-color);
        box-shadow: var(--shadow-lg);
        padding: 2rem;
        margin-bottom: 2rem;
    }
    
    .section-header {
        margin: 0 0 2rem 0;
        text-align: center;
        border-bottom: 2px solid var(--primary-color);
        padding-bottom: 1rem;
    }
    
    .section-title {
        font-size: 2.2rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 0.5rem;
    }
    
    .section-subtitle {
        color: var(--text-secondary);
        font-size: 1.1rem;
    }
    
    /* Cards avec bordures bleues et fond blanc */
    .data-card {
        background: var(--card-bg);
        border-radius: 16px;
        border: 3px solid var(--primary-color);
        box-shadow: var(--shadow-lg);
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        transition: all 0.3s ease;
    }
    
    .data-card:hover {
        transform: translateY(-5px);
        border-color: var(--secondary-color);
        box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);
    }
    
    /* Titres à l'intérieur des cards */
    .data-card h1, .data-card h2, .data-card h3, .data-card h4, .data-card h5, .data-card h6 {
        color: var(--text-primary) !important;
        margin-top: 0 !important;
        margin-bottom: 1rem !important;
        padding-bottom: 0.5rem !important;
        border-bottom: 2px solid var(--primary-color) !important;
    }
    
    .data-card .element-container {
        margin: 0 !important;
    }
    
    .data-card .stMarkdown {
        margin: 0 !important;
        color: var(--text-primary) !important;
    }
    
    .chart-title {
        font-size: 1.4rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid var(--border-color);
    }
    
    /* Container pour les charts */
    .chart-container {
        background: var(--card-bg);
        border-radius: 12px;
        border: 2px solid var(--border-color);
        padding: 1rem;
        margin-bottom: 1rem;
    }
    
    /* Inputs de connexion */
    .stTextInput > div > div > input {
        background-color: var(--card-bg) !important;
        border: 2px solid var(--primary-color) !important;
        border-radius: 8px !important;
        color: var(--text-primary) !important;
        padding: 0.75rem !important;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: var(--secondary-color) !important;
        box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.1) !important;
    }
    
    /* Métriques Streamlit avec bordures */
    [data-testid="metric-container"] {
        background: var(--card-bg) !important;
        border: 3px solid var(--primary-color) !important;
        padding: 1.5rem !important;
        border-radius: 16px !important;
        box-shadow: var(--shadow-lg) !important;
        margin-bottom: 1rem !important;
        transition: all 0.3s ease !important;
    }
    
    [data-testid="metric-container"]:hover {
        transform: translateY(-3px) !important;
        border-color: var(--accent-color) !important;
        box-shadow: 0 1rem 3rem rgba(46, 134, 171, 0.3) !important;
    }
    
    [data-testid="metric-container"] label {
        color: var(--text-secondary) !important;
        font-weight: 600 !important;
    }
    
    [data-testid="metric-container"] [data-testid="metric-value"] {
        color: var(--primary-color) !important;
        font-weight: 700 !important;
    }
    
    /* Style pour les selectbox et inputs */
    .stSelectbox > div > div {
        background-color: var(--card-bg) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 8px;
        color: var(--text-primary) !important;
    }
    
    .stMultiSelect > div > div {
        background-color: var(--card-bg) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 8px;
        color: var(--text-primary) !important;
    }
    
    /* Texte général - Plus lisible avec couleurs bleues */
    .stMarkdown, h1, h2, h3, h4, h5, h6, p {
        color: var(--text-primary) !important;
    }
    
    /* Titres principaux plus foncés */
    h1, h2, h3 {
        color: var(--text-dark) !important;
        font-weight: 600 !important;
    }
    
    /* Titres secondaires en bleu */
    h4, h5, h6 {
        color: var(--text-primary) !important;
        font-weight: 500 !important;
    }
    
    /* Paragraphes et texte normal */
    p, .stMarkdown p {
        color: var(--text-secondary) !important;
        font-weight: 400 !important;
    }
    
    /* Labels et textes d'information */
    .stSelectbox label, .stMultiSelect label, .stTextInput label {
        color: var(--text-primary) !important;
        font-weight: 500 !important;
    }
    
    /* Footer moderne */
    .modern-footer {
        margin-top: 4rem;
        padding: 3rem 2rem;
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%);
        border-radius: 20px;
        text-align: center;
        color: var(--text-white);
        border: 2px solid var(--primary-color);
        box-shadow: var(--shadow-lg);
    }
    
    .footer-content {
        max-width: 800px;
        margin: 0 auto;
    }
    
    .footer-title {
        font-size: 1.4rem;
        font-weight: 600;
        color: var(--text-white);
        margin-bottom: 1rem;
    }
    
    .footer-text {
        margin-bottom: 0.5rem;
        line-height: 1.6;
        color: var(--text-white);
    }
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    
    /* Masquer les conteneurs vides de Streamlit */
    .element-container:empty {
        display: none !important;
    }

    .stMarkdown:empty {
        display: none !important;
    }

    .stColumn > div:empty {
        display: none !important;
    }

    .data-card:empty {
        display: none !important;
    }

    .element-container:not(:has(*)) {
        display: none !important;
    }

    .stColumn > div {
        min-height: auto !important;
    }

    .stColumn > div > div:empty {
        display: none !important;
    }

    .css-1r6slb0:empty {
        display: none !important;
    }

    .css-12oz5g7:empty {
        display: none !important;
    }
</style>
""", unsafe_allow_html=True)

# Fonction pour vérifier la connexion
def check_login(username, password):
    return username in USERS and USERS[username] == password

# Fonction pour exécuter des requêtes avec gestion d'erreur améliorée
@st.cache_data(ttl=600)
def run_query(query):
    conn = None
    try:
        # Créer une nouvelle connexion pour chaque requête
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_session(autocommit=True)
        
        df = pd.read_sql_query(query, conn)
        return df
        
    except psycopg2.OperationalError as e:
        st.error(f"Erreur de connexion PostgreSQL: {e}")
        return pd.DataFrame()
    except psycopg2.Error as e:
        st.error(f"Erreur PostgreSQL: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erreur générale: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

# Test de connexion initial
def test_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        return True
    except Exception as e:
        return False

# PAGE DE CONNEXION COMPACTE - TOUT DANS UN SEUL CADRE
def show_login_page():
    # Centrer le formulaire
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div class="login-container">
            <div class="login-header">
                <div class="login-title">🌍 WASCAL</div>
                <div class="login-subtitle">Data Warehouse</div>
            </div>
            <div class="login-form-title">Connexion</div>
            <div class="login-form-subtitle">Accédez au système de reporting intelligent</div>
        </div>
        """, unsafe_allow_html=True)
        
        with st.form("login_form"):
            username = st.text_input("👤 Nom d'utilisateur", placeholder="Entrez votre nom d'utilisateur")
            password = st.text_input("🔒 Mot de passe", type="password", placeholder="Entrez votre mot de passe")
            login_button = st.form_submit_button("🚀 Se connecter", use_container_width=True)
            
            if login_button:
                if check_login(username, password):
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.success("✅ Connexion réussie!")
                    st.rerun()
                else:
                    st.error("❌ Nom d'utilisateur ou mot de passe incorrect")

# PAGE INFORMATIONS DE CONNEXION
def show_connection_info():
    st.markdown("""
    <div class="section-container">
        <div class="section-header">
            <div class="section-title">🔌 Informations de Connexion</div>
            <div class="section-subtitle">Détails de la connexion système</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="data-card">
            <h3>👤 Session Utilisateur</h3>
        </div>
        """, unsafe_allow_html=True)
        
        st.metric("Utilisateur connecté", st.session_state.get('username', 'Inconnu'))
        st.metric("Statut", "✅ Connecté")
        st.metric("Type de session", "Active")
        
        if st.button("🚪 Déconnexion", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    with col2:
        st.markdown("""
        <div class="data-card">
            <h3>🗄️ Base de Données</h3>
        </div>
        """, unsafe_allow_html=True)
        
        db_status = "✅ Connectée" if test_connection() else "❌ Déconnectée"
        st.metric("Statut PostgreSQL", db_status)
        st.metric("Serveur", "AWS RDS")
        st.metric("Base", "postgres")
        st.metric("Région", "us-east-1")

# Fonction pour obtenir les métriques principales
def get_main_metrics():
    queries = {
        "total_mesures": "SELECT COUNT(*) FROM wascal.table_des_faits;",
        "sources_actives": "SELECT COUNT(DISTINCT id_source) FROM wascal.table_des_faits;",
        "regions_couvertes": "SELECT COUNT(DISTINCT id_geographique) FROM wascal.table_des_faits;",
        "derniere_maj": "SELECT MAX(date) FROM wascal.dim_temps dt JOIN wascal.table_des_faits tf ON dt.id_temps = tf.id_temps;"
    }
    
    metrics = {}
    for key, query in queries.items():
        try:
            result = run_query(query)
            if not result.empty and len(result.columns) > 0:
                metrics[key] = result.iloc[0, 0]
            else:
                metrics[key] = 0
        except Exception as e:
            metrics[key] = 0
    return metrics

# Initialiser l'état de session
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "page" not in st.session_state:
    st.session_state.page = "dashboard"

# LOGIQUE PRINCIPALE
if not st.session_state.logged_in:
    show_login_page()
else:
    # SIDEBAR AVEC NAVIGATION
    # Test de connexion au démarrage
    if test_connection():
        st.sidebar.markdown("""
        <div class="status-success">
            ✅ Connexion PostgreSQL OK
        </div>
        """, unsafe_allow_html=True)
    else:
        st.sidebar.markdown("""
        <div class="status-error">
            ❌ Problème de connexion PostgreSQL
        </div>
        """, unsafe_allow_html=True)
    
    # Informations utilisateur dans sidebar
    st.sidebar.markdown(f"""
    <div class="connection-info">
        <h4>👤 {st.session_state.username}</h4>
        <p>Session active</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Navigation par boutons fixes
    if st.sidebar.button("🏠 Dashboard Principal", use_container_width=True):
        st.session_state.page = "dashboard"
    if st.sidebar.button("📊 Analyse par Type de Données", use_container_width=True):
        st.session_state.page = "analyse"
    if st.sidebar.button("🌍 Vue Géographique", use_container_width=True):
        st.session_state.page = "geo"
    if st.sidebar.button("📈 Tendances Temporelles", use_container_width=True):
        st.session_state.page = "tendances"
    if st.sidebar.button("📋 Sources de Données", use_container_width=True):
        st.session_state.page = "sources"
    if st.sidebar.button("🔌 Connexion", use_container_width=True):
        st.session_state.page = "connexion"

    page = st.session_state.page

    # CONTENU DES PAGES
    if page == "connexion":
        show_connection_info()
    
    elif page == "dashboard":
        # En-tête principal SEULEMENT sur dashboard
        st.markdown("""
        <div class="main-header">
            <div class="main-title">🌍 WASCAL Data Warehouse</div>
            <div class="main-subtitle">
                Système de Reporting Intelligent pour les Données Climatiques et Agricoles d'Afrique de l'Ouest
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Container pour les métriques principales
        st.markdown("""
        <div class="metrics-container">
            <div class="metrics-title">📈 Vue d'ensemble du Data Warehouse WASCAL</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Métriques principales
        metrics = get_main_metrics()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="📊 Total Mesures",
                value=f"{metrics.get('total_mesures', 0):,}",
                delta="Depuis le début"
            )
        
        with col2:
            st.metric(
                label="🏢 Sources Actives",
                value=f"{metrics.get('sources_actives', 0)}",
                delta="ANACIM, ANSD, DAPSA..."
            )
        
        with col3:
            st.metric(
                label="🌍 Régions Couvertes",
                value=f"{metrics.get('regions_couvertes', 0)}",
                delta="Zones géographiques"
            )
        
        with col4:
            st.metric(
                label="📅 Dernière MAJ",
                value=str(metrics.get('derniere_maj', 'N/A'))[:10] if metrics.get('derniere_maj') else 'N/A',
                delta="Date la plus récente"
            )
        
        # Container pour les graphiques de synthèse
        st.markdown("""
        <div class="section-container">
            <div class="section-header">
                <div class="section-title">📊 Analyses de Synthèse</div>
                <div class="section-subtitle">Répartition des données par source et géographie</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Graphiques de synthèse avec BORDURES COLORÉES
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            <div class="data-card">
            """, unsafe_allow_html=True)
            
            st.subheader("📊 Répartition par Source de Données")
            
            query_sources = """
            SELECT 
                s.acronyme,
                s.nom_source,
                COUNT(*) as nb_mesures
            FROM wascal.table_des_faits f
            JOIN wascal.dim_source_donnees s ON f.id_source = s.id_source
            GROUP BY s.acronyme, s.nom_source
            ORDER BY nb_mesures DESC
            """
            df_sources = run_query(query_sources)
            
            if not df_sources.empty:
                fig_sources = px.pie(
                    df_sources, 
                    values='nb_mesures', 
                    names='acronyme',
                    title="Distribution des mesures par organisme",
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig_sources.update_traces(textposition='inside', textinfo='percent+label')
                fig_sources.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#2c3e50'
                )
                st.plotly_chart(fig_sources, use_container_width=True)
            else:
                st.info("Aucune donnée de source disponible")
                
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="data-card">
            """, unsafe_allow_html=True)
            
            st.subheader("🌍 Répartition Géographique")
            
            query_geo = """
            SELECT 
                g.region,
                COUNT(*) as nb_mesures
            FROM wascal.table_des_faits f
            JOIN wascal.dim_geographique g ON f.id_geographique = g.id_geographique
            GROUP BY g.region
            ORDER BY nb_mesures DESC
            """
            df_geo = run_query(query_geo)
            
            if not df_geo.empty:
                fig_geo = px.bar(
                    df_geo,
                    x='region',
                    y='nb_mesures',
                    title="Nombre de mesures par région",
                    color='nb_mesures',
                    color_continuous_scale='viridis'
                )
                fig_geo.update_layout(
                    xaxis_tickangle=-45,
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#2c3e50'
                )
                st.plotly_chart(fig_geo, use_container_width=True)
            else:
                st.info("Aucune donnée géographique disponible")
                
            st.markdown("</div>", unsafe_allow_html=True)

    # Sections conditionnelles basées sur les boutons
    elif page == "analyse":
        st.markdown("""
        <div class="section-container">
            <div class="section-header">
                <div class="section-title">📊 Analyse des Données par Type</div>
                <div class="section-subtitle">Exploration détaillée par catégorie de données</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Requête principale
        query_data = """
        SELECT 
            t.date,
            g.region,
            g.commune,
            g.pays,
            f.temperature_celsius,
            f.pluviometri_mm,
            f.humidite_pourcentage,
            f.vitesse_vent_kmh,
            f.pib_regional_fcfa,
            f.population_totale,
            f.taux_chomage_pourcentage,
            f.production_tonnes,
            f.surface_cultivee_hectares,
            f.rendement_tonne_par_hectare,
            f.niveau_eau_metres,
            f.debit_m3par_seconde,
            f.qualite_eau_ph,
            s.acronyme as source,
            s.type_source,
            td.categorie,
            td.sous_categorie
        FROM wascal.table_des_faits f
        JOIN wascal.dim_temps t ON f.id_temps = t.id_temps
        JOIN wascal.dim_geographique g ON f.id_geographique = g.id_geographique
        JOIN wascal.dim_source_donnees s ON f.id_source = s.id_source
        JOIN wascal.dim_type_donnees td ON f.id_type_donnees = td.id_type_donnees
        ORDER BY t.date DESC
        """
        
        df_data = run_query(query_data)
        
        if not df_data.empty:
            # Sélection du type d'analyse
            st.markdown("""
            <div class="chart-container">
                <div class="chart-title">📊 Choisissez le type d'analyse</div>
            </div>
            """, unsafe_allow_html=True)
            
            analyse_type = st.selectbox(
                "Type de données à analyser",
                ["🌡️ Données Climatiques", "🌾 Données Agricoles", "💰 Données Économiques", "💧 Données Hydrologiques", "📊 Vue d'ensemble"]
            )
            
            # Filtres
            st.markdown("""
            <div class="chart-container">
                <div class="chart-title">🔍 Filtres</div>
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                regions_available = sorted([r for r in df_data['region'].dropna().unique() if pd.notna(r)])
                if regions_available:
                    regions_selected = st.multiselect(
                        "Sélectionnez les régions",
                        options=regions_available,
                        default=regions_available
                    )
                else:
                    st.warning("Aucune région disponible")
                    regions_selected = []
            
            with col2:
                sources_available = sorted([s for s in df_data['source'].dropna().unique() if pd.notna(s)])
                if sources_available:
                    sources_selected = st.multiselect(
                        "Sélectionnez les sources",
                        options=sources_available,
                        default=sources_available
                    )
                else:
                    st.warning("Aucune source disponible")
                    sources_selected = []
            
            # Filtrage des données
            if regions_selected and sources_selected:
                df_filtered = df_data[
                    (df_data['region'].isin(regions_selected)) &
                    (df_data['source'].isin(sources_selected))
                ]
            else:
                df_filtered = df_data
            
            if not df_filtered.empty:
                
                # DONNÉES CLIMATIQUES
                if analyse_type == "🌡️ Données Climatiques":
                    st.markdown("""
                    <div class="section-container">
                        <div class="section-header">
                            <div class="section-title">🌡️ Analyse des Données Climatiques</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    climat_cols = ['temperature_celsius', 'pluviometri_mm', 'humidite_pourcentage', 'vitesse_vent_kmh']
                    climat_data = df_filtered[['date', 'region'] + climat_cols].copy()
                    climat_data = climat_data.dropna(subset=climat_cols, how='all')
                    
                    if not climat_data.empty:
                        # Graphiques climatiques
                        fig_climat = make_subplots(
                            rows=2, cols=2,
                            subplot_titles=('Température (°C)', 'Pluviométrie (mm)', 'Humidité (%)', 'Vitesse du Vent (km/h)'),
                            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                                   [{"secondary_y": False}, {"secondary_y": False}]]
                        )
                        
                        # Température
                        temp_data = climat_data.dropna(subset=['temperature_celsius'])
                        if not temp_data.empty:
                            for region in temp_data['region'].unique():
                                if pd.notna(region):
                                    region_data = temp_data[temp_data['region'] == region]
                                    fig_climat.add_trace(
                                        go.Scatter(x=region_data['date'], y=region_data['temperature_celsius'],
                                                 mode='lines+markers', name=f'{region}'),
                                        row=1, col=1
                                    )
                        
                        # Pluviométrie
                        pluie_data = climat_data.dropna(subset=['pluviometri_mm'])
                        if not pluie_data.empty:
                            for region in pluie_data['region'].unique():
                                if pd.notna(region):
                                    region_data = pluie_data[pluie_data['region'] == region]
                                    fig_climat.add_trace(
                                        go.Bar(x=region_data['date'], y=region_data['pluviometri_mm'],
                                              name=f'{region}', showlegend=False),
                                        row=1, col=2
                                    )
                        
                        # Humidité
                        humidite_data = climat_data.dropna(subset=['humidite_pourcentage'])
                        if not humidite_data.empty:
                            for region in humidite_data['region'].unique():
                                if pd.notna(region):
                                    region_data = humidite_data[humidite_data['region'] == region]
                                    fig_climat.add_trace(
                                        go.Scatter(x=region_data['date'], y=region_data['humidite_pourcentage'],
                                                 mode='lines+markers', name=f'{region}', showlegend=False),
                                        row=2, col=1
                                    )
                        
                        # Vitesse du vent
                        vent_data = climat_data.dropna(subset=['vitesse_vent_kmh'])
                        if not vent_data.empty:
                            for region in vent_data['region'].unique():
                                if pd.notna(region):
                                    region_data = vent_data[vent_data['region'] == region]
                                    fig_climat.add_trace(
                                        go.Scatter(x=region_data['date'], y=region_data['vitesse_vent_kmh'],
                                                 mode='lines+markers', name=f'{region}', showlegend=False),
                                        row=2, col=2
                                    )
                        
                        fig_climat.update_layout(
                            height=600, 
                            title_text="Analyse Climatique par Région",
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            font_color='#2c3e50'
                        )
                        st.plotly_chart(fig_climat, use_container_width=True)
                        
                        # Statistiques climatiques
                        st.markdown("""
                        <div class="chart-container">
                            <div class="chart-title">📈 Statistiques Climatiques</div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if not temp_data.empty:
                                temp_stats = temp_data.groupby('region')['temperature_celsius'].agg(['mean', 'min', 'max']).round(2)
                                st.write("**Températures par région:**")
                                st.dataframe(temp_stats)
                        
                        with col2:
                            if not pluie_data.empty:
                                pluie_stats = pluie_data.groupby('region')['pluviometri_mm'].agg(['sum', 'mean']).round(2)
                                st.write("**Pluviométrie par région:**")
                                st.dataframe(pluie_stats)
                    else:
                        st.info("Aucune donnée climatique disponible pour les filtres sélectionnés")
                
                # DONNÉES AGRICOLES
                elif analyse_type == "🌾 Données Agricoles":
                    st.markdown("""
                    <div class="section-container">
                        <div class="section-header">
                            <div class="section-title">🌾 Analyse des Données Agricoles</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    agricole_cols = ['production_tonnes', 'surface_cultivee_hectares', 'rendement_tonne_par_hectare']
                    agricole_data = df_filtered[['date', 'region'] + agricole_cols].copy()
                    agricole_data = agricole_data.dropna(subset=agricole_cols, how='all')
                    
                    if not agricole_data.empty:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("""
                            <div class="chart-container">
                                <div class="chart-title">🌾 Production Agricole</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Production agricole
                            production_data = agricole_data.dropna(subset=['production_tonnes'])
                            if not production_data.empty:
                                production_sum = production_data.groupby('region')['production_tonnes'].sum().reset_index()
                                fig_production = px.bar(
                                    production_sum,
                                    x='region',
                                    y='production_tonnes',
                                    title="Production Agricole Totale par Région (tonnes)",
                                    color='production_tonnes',
                                    color_continuous_scale='Greens'
                                )
                                fig_production.update_layout(
                                    xaxis_tickangle=-45,
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    font_color='#2c3e50'
                                )
                                st.plotly_chart(fig_production, use_container_width=True)
                        
                        with col2:
                            st.markdown("""
                            <div class="chart-container">
                                <div class="chart-title">🌾 Surface Cultivée</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Surface cultivée
                            surface_data = agricole_data.dropna(subset=['surface_cultivee_hectares'])
                            if not surface_data.empty:
                                surface_sum = surface_data.groupby('region')['surface_cultivee_hectares'].sum().reset_index()
                                fig_surface = px.pie(
                                    surface_sum,
                                    values='surface_cultivee_hectares',
                                    names='region',
                                    title="Répartition des Surfaces Cultivées (hectares)"
                                )
                                fig_surface.update_layout(
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    font_color='#2c3e50'
                                )
                                st.plotly_chart(fig_surface, use_container_width=True)
                        
                        # Rendement agricole
                        rendement_data = agricole_data.dropna(subset=['rendement_tonne_par_hectare'])
                        if not rendement_data.empty:
                            st.markdown("""
                            <div class="chart-container">
                                <div class="chart-title">📊 Évolution du Rendement Agricole</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            fig_rendement = px.line(
                                rendement_data,
                                x='date',
                                y='rendement_tonne_par_hectare',
                                color='region',
                                title="Rendement Agricole par Région (tonnes/hectare)"
                            )
                            fig_rendement.update_layout(
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font_color='#2c3e50'
                            )
                            st.plotly_chart(fig_rendement, use_container_width=True)
                    else:
                        st.info("Aucune donnée agricole disponible pour les filtres sélectionnés")
                
                # DONNÉES ÉCONOMIQUES
                elif analyse_type == "💰 Données Économiques":
                    st.markdown("""
                    <div class="section-container">
                        <div class="section-header">
                            <div class="section-title">💰 Analyse des Données Économiques</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    economique_cols = ['population_totale', 'pib_regional_fcfa', 'taux_chomage_pourcentage']
                    economique_data = df_filtered[['date', 'region'] + economique_cols].copy()
                    economique_data = economique_data.dropna(subset=economique_cols, how='all')
                    
                    if not economique_data.empty:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("""
                            <div class="chart-container">
                                <div class="chart-title">👥 Population</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Population
                            pop_data = economique_data.dropna(subset=['population_totale'])
                            if not pop_data.empty:
                                pop_recent = pop_data.groupby('region')['population_totale'].last().reset_index()
                                fig_pop = px.bar(
                                    pop_recent,
                                    x='region',
                                    y='population_totale',
                                    title="Population Totale par Région",
                                    color='population_totale',
                                    color_continuous_scale='Blues'
                                )
                                fig_pop.update_layout(
                                    xaxis_tickangle=-45,
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    font_color='#2c3e50'
                                )
                                st.plotly_chart(fig_pop, use_container_width=True)
                        
                        with col2:
                            st.markdown("""
                            <div class="chart-container">
                                <div class="chart-title">💰 PIB Régional</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # PIB régional
                            pib_data = economique_data.dropna(subset=['pib_regional_fcfa'])
                            if not pib_data.empty:
                                pib_recent = pib_data.groupby('region')['pib_regional_fcfa'].last().reset_index()
                                fig_pib = px.bar(
                                    pib_recent,
                                    x='region',
                                    y='pib_regional_fcfa',
                                    title="PIB Régional (FCFA)",
                                    color='pib_regional_fcfa',
                                    color_continuous_scale='Oranges'
                                )
                                fig_pib.update_layout(
                                    xaxis_tickangle=-45,
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    font_color='#2c3e50'
                                )
                                st.plotly_chart(fig_pib, use_container_width=True)
                        
                        # Taux de chômage
                        chomage_data = economique_data.dropna(subset=['taux_chomage_pourcentage'])
                        if not chomage_data.empty:
                            st.markdown("""
                            <div class="chart-container">
                                <div class="chart-title">📈 Évolution du Taux de Chômage</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            fig_chomage = px.line(
                                chomage_data,
                                x='date',
                                y='taux_chomage_pourcentage',
                                color='region',
                                title="Taux de Chômage par Région (%)"
                            )
                            fig_chomage.update_layout(
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font_color='#2c3e50'
                            )
                            st.plotly_chart(fig_chomage, use_container_width=True)
                    else:
                        st.info("Aucune donnée économique disponible pour les filtres sélectionnés")
                
                # DONNÉES HYDROLOGIQUES
                elif analyse_type == "💧 Données Hydrologiques":
                    st.markdown("""
                    <div class="section-container">
                        <div class="section-header">
                            <div class="section-title">💧 Analyse des Données Hydrologiques</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    hydro_cols = ['niveau_eau_metres', 'debit_m3par_seconde', 'qualite_eau_ph']
                    hydro_data = df_filtered[['date', 'region'] + hydro_cols].copy()
                    hydro_data = hydro_data.dropna(subset=hydro_cols, how='all')
                    
                    if not hydro_data.empty:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("""
                            <div class="chart-container">
                                <div class="chart-title">🌊 Niveau d'Eau</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Niveau d'eau
                            niveau_data = hydro_data.dropna(subset=['niveau_eau_metres'])
                            if not niveau_data.empty:
                                fig_niveau = px.line(
                                    niveau_data,
                                    x='date',
                                    y='niveau_eau_metres',
                                    color='region',
                                    title="Évolution du Niveau d'Eau (mètres)"
                                )
                                fig_niveau.update_layout(
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    font_color='#2c3e50'
                                )
                                st.plotly_chart(fig_niveau, use_container_width=True)
                        
                        with col2:
                            st.markdown("""
                            <div class="chart-container">
                                <div class="chart-title">🌊 Débit d'Eau</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Débit d'eau
                            debit_data = hydro_data.dropna(subset=['debit_m3par_seconde'])
                            if not debit_data.empty:
                                fig_debit = px.line(
                                    debit_data,
                                    x='date',
                                    y='debit_m3par_seconde',
                                    color='region',
                                    title="Débit d'Eau (m³/seconde)"
                                )
                                fig_debit.update_layout(
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    font_color='#2c3e50'
                                )
                                st.plotly_chart(fig_debit, use_container_width=True)
                        
                        # Qualité de l'eau
                        qualite_data = hydro_data.dropna(subset=['qualite_eau_ph'])
                        if not qualite_data.empty:
                            st.markdown("""
                            <div class="chart-container">
                                <div class="chart-title">🧪 Qualité de l'Eau (pH)</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            fig_qualite = px.scatter(
                                qualite_data,
                                x='date',
                                y='qualite_eau_ph',
                                color='region',
                                title="Évolution du pH de l'Eau par Région"
                            )
                            fig_qualite.update_layout(
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font_color='#2c3e50'
                            )
                            st.plotly_chart(fig_qualite, use_container_width=True)
                    else:
                        st.info("Aucune donnée hydrologique disponible pour les filtres sélectionnés")
                
                # VUE D'ENSEMBLE
                elif analyse_type == "📊 Vue d'ensemble":
                    st.markdown("""
                    <div class="section-container">
                        <div class="section-header">
                            <div class="section-title">📊 Vue d'ensemble de toutes les données</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Comptage des données disponibles par type
                    data_counts = {
                        'Climatique': len(df_filtered.dropna(subset=['temperature_celsius', 'pluviometri_mm', 'humidite_pourcentage', 'vitesse_vent_kmh'], how='all')),
                        'Agricole': len(df_filtered.dropna(subset=['production_tonnes', 'surface_cultivee_hectares', 'rendement_tonne_par_hectare'], how='all')),
                        'Économique': len(df_filtered.dropna(subset=['population_totale', 'pib_regional_fcfa', 'taux_chomage_pourcentage'], how='all')),
                        'Hydrologique': len(df_filtered.dropna(subset=['niveau_eau_metres', 'debit_m3par_seconde', 'qualite_eau_ph'], how='all'))
                    }
                    
                    # Graphique de répartition
                    df_counts = pd.DataFrame(list(data_counts.items()), columns=['Type', 'Nombre_mesures'])
                    df_counts = df_counts[df_counts['Nombre_mesures'] > 0]
                    
                    if not df_counts.empty:
                        fig_overview = px.pie(
                            df_counts,
                            values='Nombre_mesures',
                            names='Type',
                            title="Répartition des Mesures par Type de Données"
                        )
                        fig_overview.update_layout(
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            font_color='#2c3e50'
                        )
                        st.plotly_chart(fig_overview, use_container_width=True)
                        
                        # Tableau récapitulatif
                        st.markdown("""
                        <div class="chart-container">
                            <div class="chart-title">📋 Récapitulatif des données</div>
                        </div>
                        """, unsafe_allow_html=True)
                        st.dataframe(df_counts, use_container_width=True)
                    else:
                        st.info("Aucune donnée disponible pour créer la vue d'ensemble")
                
                # Tableau des données détaillées
                st.markdown("""
                <div class="section-container">
                    <div class="section-header">
                        <div class="section-title">📋 Données Détaillées </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # Sélectionner les colonnes pertinentes selon le type d'analyse
                if analyse_type == "🌡️ Données Climatiques":
                    cols_to_show = ['date', 'region', 'commune', 'source', 'temperature_celsius', 'pluviometri_mm', 'humidite_pourcentage', 'vitesse_vent_kmh']
                elif analyse_type == "🌾 Données Agricoles":
                    cols_to_show = ['date', 'region', 'commune', 'source', 'production_tonnes', 'surface_cultivee_hectares', 'rendement_tonne_par_hectare']
                elif analyse_type == "💰 Données Économiques":
                    cols_to_show = ['date', 'region', 'commune', 'source', 'population_totale', 'pib_regional_fcfa', 'taux_chomage_pourcentage']
                elif analyse_type == "💧 Données Hydrologiques":
                    cols_to_show = ['date', 'region', 'commune', 'source', 'niveau_eau_metres', 'debit_m3par_seconde', 'qualite_eau_ph']
                else:  # Vue d'ensemble
                    cols_to_show = ['date', 'region', 'commune', 'source', 'type_source', 'categorie']
                
                # Filtrer les colonnes existantes
                cols_to_show = [col for col in cols_to_show if col in df_filtered.columns]
                
                st.dataframe(
                    df_filtered[cols_to_show].head(100),
                    use_container_width=True
                )
            else:
                st.warning("Aucune donnée ne correspond aux filtres sélectionnés")
        else:
            st.info("Aucune donnée disponible dans la base de données")

    elif page == "geo":
        st.markdown("""
        <div class="section-container">
            <div class="section-header">
                <div class="section-title">🌍 Analyse Géographique des Données</div>
                <div class="section-subtitle">Exploration spatiale des données WASCAL</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Données géographiques avec coordonnées
        query_geo_detail = """
        SELECT 
            g.pays,
            g.region,
            g.commune,
            g.latitude,
            g.longitude,
            COUNT(f.id_geographique) as nb_mesures,
            AVG(f.temperature_celsius) as temp_moyenne,
            AVG(f.pluviometri_mm) as pluie_moyenne
        FROM wascal.dim_geographique g
        LEFT JOIN wascal.table_des_faits f ON g.id_geographique = f.id_geographique
        GROUP BY g.pays, g.region, g.commune, g.latitude, g.longitude
        HAVING COUNT(f.id_geographique) > 0
        """
        
        df_geo_detail = run_query(query_geo_detail)
        
        if not df_geo_detail.empty:
            # Carte interactive
            st.markdown("""
            <div class="chart-container">
                <div class="chart-title">🗺️ Carte Interactive des Stations</div>
            </div>
            """, unsafe_allow_html=True)
            
            fig_map = px.scatter_mapbox(
                df_geo_detail,
                lat="latitude",
                lon="longitude",
                hover_name="commune",
                hover_data=["region", "nb_mesures", "temp_moyenne", "pluie_moyenne"],
                color="nb_mesures",
                size="nb_mesures",
                color_continuous_scale="viridis",
                zoom=6,
                height=500,
                title="Localisation des Stations de Mesure WASCAL"
            )
            
            fig_map.update_layout(
                mapbox_style="open-street-map",
                margin={"r":0,"t":50,"l":0,"b":0},
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#2c3e50'
            )
            
            st.plotly_chart(fig_map, use_container_width=True)
            
            # Statistiques par région
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                <div class="chart-container">
                    <div class="chart-title">📊 Mesures par Région</div>
                </div>
                """, unsafe_allow_html=True)
                
                fig_region = px.treemap(
                    df_geo_detail,
                    path=['region', 'commune'],
                    values='nb_mesures',
                    title="Hiérarchie des Mesures par Zone"
                )
                fig_region.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#2c3e50'
                )
                st.plotly_chart(fig_region, use_container_width=True)
            
            with col2:
                st.markdown("""
                <div class="chart-container">
                    <div class="chart-title">🌡️ Températures Moyennes</div>
                </div>
                """, unsafe_allow_html=True)
                
                df_temp_clean = df_geo_detail.dropna(subset=['temp_moyenne'])
                if not df_temp_clean.empty:
                    fig_temp_map = px.bar(
                        df_temp_clean,
                        x='commune',
                        y='temp_moyenne',
                        color='temp_moyenne',
                        title="Température Moyenne par Commune",
                        color_continuous_scale='RdYlBu_r'
                    )
                    fig_temp_map.update_layout(
                        xaxis_tickangle=-45,
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font_color='#2c3e50'
                    )
                    st.plotly_chart(fig_temp_map, use_container_width=True)
                else:
                    st.info("Pas de données de température disponibles")
        else:
            st.info("Aucune donnée géographique disponible")

    elif page == "tendances":
        st.markdown("""
        <div class="section-container">
            <div class="section-header">
                <div class="section-title">📈 Analyse des Tendances Temporelles</div>
                <div class="section-subtitle">Évolution des données dans le temps</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Données temporelles
        query_temporal = """
        SELECT 
            t.date,
            t.annee,
            t.mois,
            t.saison,
            AVG(f.temperature_celsius) as temp_moyenne,
            SUM(f.pluviometri_mm) as pluie_totale,
            AVG(f.humidite_pourcentage) as humidite_moyenne,
            COUNT(*) as nb_mesures
        FROM wascal.table_des_faits f
        JOIN wascal.dim_temps t ON f.id_temps = t.id_temps
        GROUP BY t.date, t.annee, t.mois, t.saison
        ORDER BY t.date
        """
        
        df_temporal = run_query(query_temporal)
        
        if not df_temporal.empty:
            # Évolution temporelle
            st.markdown("""
            <div class="chart-container">
                <div class="chart-title">📊 Évolution des Variables Climatiques</div>
            </div>
            """, unsafe_allow_html=True)
            
            fig_evolution = make_subplots(
                rows=3, cols=1,
                subplot_titles=('Température Moyenne (°C)', 'Pluviométrie Totale (mm)', 'Humidité Moyenne (%)'),
                vertical_spacing=0.08
            )
            
            # Température
            fig_evolution.add_trace(
                go.Scatter(x=df_temporal['date'], y=df_temporal['temp_moyenne'],
                         mode='lines+markers', name='Température', line=dict(color='red')),
                row=1, col=1
            )
            
            # Pluviométrie
            fig_evolution.add_trace(
                go.Bar(x=df_temporal['date'], y=df_temporal['pluie_totale'],
                      name='Pluviométrie', marker_color='blue', showlegend=False),
                row=2, col=1
            )
            
            # Humidité
            fig_evolution.add_trace(
                go.Scatter(x=df_temporal['date'], y=df_temporal['humidite_moyenne'],
                         mode='lines+markers', name='Humidité', line=dict(color='green'), showlegend=False),
                row=3, col=1
            )
            
            fig_evolution.update_layout(
                height=700, 
                title_text="Évolution Temporelle des Variables Climatiques",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#2c3e50'
            )
            st.plotly_chart(fig_evolution, use_container_width=True)
            
            # Analyse saisonnière
            st.markdown("""
            <div class="section-container">
                <div class="section-header">
                    <div class="section-title">🍂 Analyse Saisonnière</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            df_saison = df_temporal.groupby('saison').agg({
                'temp_moyenne': 'mean',
                'pluie_totale': 'sum',
                'humidite_moyenne': 'mean',
                'nb_mesures': 'sum'
            }).reset_index()
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                <div class="chart-container">
                    <div class="chart-title">🌡️ Température par Saison</div>
                </div>
                """, unsafe_allow_html=True)
                
                fig_saison_temp = px.bar(
                    df_saison,
                    x='saison',
                    y='temp_moyenne',
                    title="Température Moyenne par Saison",
                    color='temp_moyenne',
                    color_continuous_scale='RdYlBu_r'
                )
                fig_saison_temp.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#2c3e50'
                )
                st.plotly_chart(fig_saison_temp, use_container_width=True)
            
            with col2:
                st.markdown("""
                <div class="chart-container">
                    <div class="chart-title">🌧️ Pluviométrie par Saison</div>
                </div>
                """, unsafe_allow_html=True)
                
                fig_saison_pluie = px.bar(
                    df_saison,
                    x='saison',
                    y='pluie_totale',
                    title="Pluviométrie Totale par Saison",
                    color='pluie_totale',
                    color_continuous_scale='Blues'
                )
                fig_saison_pluie.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#2c3e50'
                )
                st.plotly_chart(fig_saison_pluie, use_container_width=True)
        else:
            st.info("Aucune donnée temporelle disponible")

    elif page == "sources":
        st.markdown("""
        <div class="section-container">
            <div class="section-header">
                <div class="section-title">📋 Gestion des Sources de Données</div>
                <div class="section-subtitle">Informations détaillées sur les sources partenaires</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Informations sur les sources
        query_sources_detail = """
        SELECT 
            s.nom_source,
            s.acronyme,
            s.type_source,
            s.contact,
            s.url,
            s.date_derniere_maj,
            COUNT(f.id_source) as nb_mesures_total,
            COUNT(DISTINCT f.id_geographique) as nb_zones_couvertes
        FROM wascal.dim_source_donnees s
        LEFT JOIN wascal.table_des_faits f ON s.id_source = f.id_source
        GROUP BY s.id_source, s.nom_source, s.acronyme, s.type_source, s.contact, s.url, s.date_derniere_maj
        ORDER BY nb_mesures_total DESC
        """
        
        df_sources_detail = run_query(query_sources_detail)
        
        if not df_sources_detail.empty:
            # Vue d'ensemble des sources
            st.markdown("""
            <div class="chart-container">
                <div class="chart-title">🏢 Vue d'Ensemble des Sources</div>
            </div>
            """, unsafe_allow_html=True)
            
            # Métriques par source
            for _, source in df_sources_detail.iterrows():
                with st.expander(f"📊 {source['acronyme']} - {source['nom_source']}"):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric(
                            "Mesures Totales",
                            f"{source['nb_mesures_total']:,}",
                            delta=f"Type: {source['type_source']}"
                        )
                    
                    with col2:
                        st.metric(
                            "Zones Couvertes",
                            f"{source['nb_zones_couvertes']}",
                            delta="Régions géographiques"
                        )
                    
                    # Informations de contact
                    with col3:
                        if source['date_derniere_maj']:
                            st.metric(
                                "Dernière MAJ",
                                str(source['date_derniere_maj'])[:10],
                                delta="Date de mise à jour"
                            )
                    
                    if source['contact']:
                        st.write(f"📧 **Contact :** {source['contact']}")
                    if source['url']:
                        st.write(f"🌐 **Site Web :** {source['url']}")
            
            # Graphique de contribution
            st.markdown("""
            <div class="chart-container">
                <div class="chart-title">📈 Contribution de Chaque Source</div>
            </div>
            """, unsafe_allow_html=True)
            
            fig_contrib = px.bar(
                df_sources_detail,
                x='nb_mesures_total',
                y='acronyme',
                orientation='h',
                title="Nombre de Mesures par Source de Données",
                color='nb_mesures_total',
                color_continuous_scale='viridis'
            )
            
            fig_contrib.update_layout(
                height=400,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#2c3e50'
            )
            st.plotly_chart(fig_contrib, use_container_width=True)
            
            # Tableau détaillé
            st.markdown("""
            <div class="chart-container">
                <div class="chart-title">📋 Détails des Sources</div>
            </div>
            """, unsafe_allow_html=True)
            
            st.dataframe(
                df_sources_detail[[
                    'acronyme', 'nom_source', 'type_source', 
                    'nb_mesures_total', 'nb_zones_couvertes'
                ]],
                use_container_width=True
            )
        else:
            st.info("Aucune information sur les sources disponible")

    # Footer moderne
    st.markdown("""
    <div class="modern-footer">
        <div class="footer-content">
            <div class="footer-title">🌍 WASCAL Data Warehouse</div>
            <p class="footer-text">Développé pour l'analyse intelligente des données climatiques et agricoles d'Afrique de l'Ouest</p>
            <p class="footer-text">📊 Sources de données : ANACIM • ANSD • DAPSA • ISRA • DGPRE</p>
            <p class="footer-text">⚡ Propulsé par Streamlit & PostgreSQL sur AWS</p>
            <p class="footer-text">🎓 Projet académique - Université de recherche WASCAL</p>
        </div>
    </div>
    """, unsafe_allow_html=True)