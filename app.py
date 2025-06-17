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

# Configuration de connexion PostgreSQL
DB_CONFIG = {
    "host": "wascal-datawarehouse.ce5k6qqm8o1c.us-east-1.rds.amazonaws.com",
    "port": "5432",
    "database": "postgres",
    "user": "wascal_admin",
    "password": st.secrets["postgres_password"]
}

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
        st.error(f"Test de connexion échoué: {e}")
        return False

# CSS personnalisé moderne
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Variables CSS */
    :root {
        --primary-color: #1f77b4;
        --secondary-color: #667eea;
        --accent-color: #764ba2;
        --success-color: #28a745;
        --warning-color: #ffc107;
        --danger-color: #dc3545;
        --light-bg: #f8f9fa;
        --card-bg: #ffffff;
        --text-primary: #2c3e50;
        --text-secondary: #6c757d;
        --border-color: #dee2e6;
        --shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
        --shadow-lg: 0 1rem 3rem rgba(0, 0, 0, 0.175);
    }
    
    /* Base styles */
    .main {
        font-family: 'Inter', sans-serif;
    }
    
    /* Header principal moderne */
    .main-header {
        background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 50%, var(--accent-color) 100%);
        padding: 2.5rem 2rem;
        border-radius: 20px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
        box-shadow: var(--shadow-lg);
        position: relative;
        overflow: hidden;
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
        font-size: 3.5rem;
        font-weight: 700;
        margin-bottom: 1rem;
        letter-spacing: -0.02em;
        position: relative;
        z-index: 1;
    }
    
    .main-subtitle {
        font-size: 1.3rem;
        opacity: 0.95;
        font-weight: 400;
        max-width: 700px;
        margin: 0 auto;
        position: relative;
        z-index: 1;
        line-height: 1.6;
    }
    
    /* Cards métriques modernes */
    .metric-card {
        background: var(--card-bg);
        padding: 2rem;
        border-radius: 16px;
        border: 1px solid var(--border-color);
        box-shadow: var(--shadow);
        text-align: center;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
        height: 100%;
        margin-bottom: 1rem;
    }
    
    .metric-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, var(--primary-color), var(--secondary-color));
        transform: scaleX(0);
        transition: transform 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-8px);
        box-shadow: var(--shadow-lg);
    }
    
    .metric-card:hover::before {
        transform: scaleX(1);
    }
    
    .metric-icon {
        font-size: 2.5rem;
        margin-bottom: 1rem;
        opacity: 0.8;
        display: block;
    }
    
    .metric-value {
        font-size: 2.8rem;
        font-weight: 700;
        color: var(--primary-color);
        margin-bottom: 0.5rem;
        line-height: 1;
    }
    
    .metric-label {
        font-size: 1rem;
        color: var(--text-secondary);
        font-weight: 500;
        margin-bottom: 0.5rem;
    }
    
    .metric-delta {
        font-size: 0.875rem;
        color: var(--text-secondary);
        opacity: 0.8;
    }
    
    /* Sidebar moderne */
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, var(--light-bg) 0%, #e9ecef 100%);
        border-radius: 12px;
        margin: 1rem;
        padding: 1rem;
    }
    
    /* Section headers */
    .section-header {
        margin: 3rem 0 2rem 0;
        text-align: center;
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
    
    /* Charts containers */
    .chart-container {
        background: var(--card-bg);
        border-radius: 16px;
        padding: 1.5rem;
        border: 1px solid var(--border-color);
        box-shadow: var(--shadow);
        margin-bottom: 2rem;
    }
    
    .chart-title {
        font-size: 1.4rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid var(--border-color);
    }
    
    /* Status indicators */
    .status-success {
        color: var(--success-color);
        font-weight: 600;
    }
    
    .status-error {
        color: var(--danger-color);
        font-weight: 600;
    }
    
    /* Footer moderne */
    .modern-footer {
        margin-top: 4rem;
        padding: 3rem 2rem;
        background: linear-gradient(135deg, var(--light-bg) 0%, #e9ecef 100%);
        border-radius: 20px;
        text-align: center;
        color: var(--text-secondary);
        border-top: 1px solid var(--border-color);
    }
    
    .footer-content {
        max-width: 800px;
        margin: 0 auto;
    }
    
    .footer-title {
        font-size: 1.4rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 1rem;
    }
    
    .footer-text {
        margin-bottom: 0.5rem;
        line-height: 1.6;
    }
    
    /* Responsive design */
    @media (max-width: 768px) {
        .main-title {
            font-size: 2.5rem;
        }
        
        .main-subtitle {
            font-size: 1.1rem;
        }
        
        .main-header {
            padding: 2rem 1rem;
        }
        
        .metric-card {
            margin-bottom: 1rem;
        }
    }
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    
    /* Improve Streamlit components */
    .stSelectbox > div > div {
        border-radius: 8px;
    }
    
    .stMultiSelect > div > div {
        border-radius: 8px;
    }
    
    /* Custom alerts */
    .stAlert {
        border-radius: 12px;
        border: none;
        box-shadow: var(--shadow);
    }
    
    /* Streamlit metrics customization */
    [data-testid="metric-container"] {
        background: var(--card-bg);
        border: 1px solid var(--border-color);
        padding: 1rem;
        border-radius: 12px;
        box-shadow: var(--shadow);
    }
</style>
""", unsafe_allow_html=True)

# En-tête principal
st.markdown("""
<div class="main-header">
    <div class="main-title">🌍 WASCAL Data Warehouse</div>
    <div class="main-subtitle">
        Système de Reporting Intelligent pour les Données Climatiques et Agricoles d'Afrique de l'Ouest
    </div>
</div>
""", unsafe_allow_html=True)

# Test de connexion au démarrage
if test_connection():
    st.sidebar.success("✅ Connexion PostgreSQL OK")
else:
    st.sidebar.error("❌ Problème de connexion PostgreSQL")
    st.stop()

# Sidebar pour la navigation
st.sidebar.title("📊 Navigation")
page = st.sidebar.selectbox(
    "Choisissez une section",
    ["🏠 Dashboard Principal", "📊 Analyse par Type de Données", "🌍 Vue Géographique", "📈 Tendances Temporelles", "📋 Sources de Données"]
)

# Fonction pour obtenir les métriques principales avec requêtes sécurisées
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
            st.warning(f"Erreur pour {key}: {e}")
            metrics[key] = 0
    return metrics

# Dashboard Principal
if page == "🏠 Dashboard Principal":
    st.header("📈 Vue d'ensemble du Data Warehouse WASCAL")
    
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
    
    # Graphiques de synthèse
    col1, col2 = st.columns(2)
    
    with col1:
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
            st.plotly_chart(fig_sources, use_container_width=True)
        else:
            st.info("Aucune donnée de source disponible")
    
    with col2:
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
            fig_geo.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_geo, use_container_width=True)
        else:
            st.info("Aucune donnée géographique disponible")

# Analyse par Type de Données
elif page == "📊 Analyse par Type de Données":
    st.header("📊 Analyse des Données par Type")
    
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
        st.subheader("📊 Choisissez le type d'analyse")
        
        analyse_type = st.selectbox(
            "Type de données à analyser",
            ["🌡️ Données Climatiques", "🌾 Données Agricoles", "💰 Données Économiques", "💧 Données Hydrologiques", "📊 Vue d'ensemble"]
        )
        
        # Filtres
        st.subheader("🔍 Filtres")
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
                st.write("### 🌡️ Analyse des Données Climatiques")
                
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
                    
                    fig_climat.update_layout(height=600, title_text="Analyse Climatique par Région")
                    st.plotly_chart(fig_climat, use_container_width=True)
                    
                    # Statistiques climatiques
                    st.write("#### 📈 Statistiques Climatiques")
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
                st.write("### 🌾 Analyse des Données Agricoles")
                
                agricole_cols = ['production_tonnes', 'surface_cultivee_hectares', 'rendement_tonne_par_hectare']
                agricole_data = df_filtered[['date', 'region'] + agricole_cols].copy()
                agricole_data = agricole_data.dropna(subset=agricole_cols, how='all')
                
                if not agricole_data.empty:
                    col1, col2 = st.columns(2)
                    
                    with col1:
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
                            fig_production.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig_production, use_container_width=True)
                    
                    with col2:
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
                            st.plotly_chart(fig_surface, use_container_width=True)
                    
                    # Rendement agricole
                    rendement_data = agricole_data.dropna(subset=['rendement_tonne_par_hectare'])
                    if not rendement_data.empty:
                        st.write("#### 📊 Évolution du Rendement Agricole")
                        fig_rendement = px.line(
                            rendement_data,
                            x='date',
                            y='rendement_tonne_par_hectare',
                            color='region',
                            title="Rendement Agricole par Région (tonnes/hectare)"
                        )
                        st.plotly_chart(fig_rendement, use_container_width=True)
                else:
                    st.info("Aucune donnée agricole disponible pour les filtres sélectionnés")
            
            # DONNÉES ÉCONOMIQUES
            elif analyse_type == "💰 Données Économiques":
                st.write("### 💰 Analyse des Données Économiques")
                
                economique_cols = ['population_totale', 'pib_regional_fcfa', 'taux_chomage_pourcentage']
                economique_data = df_filtered[['date', 'region'] + economique_cols].copy()
                economique_data = economique_data.dropna(subset=economique_cols, how='all')
                
                if not economique_data.empty:
                    col1, col2 = st.columns(2)
                    
                    with col1:
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
                            fig_pop.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig_pop, use_container_width=True)
                    
                    with col2:
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
                            fig_pib.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig_pib, use_container_width=True)
                    
                    # Taux de chômage
                    chomage_data = economique_data.dropna(subset=['taux_chomage_pourcentage'])
                    if not chomage_data.empty:
                        st.write("#### 📈 Évolution du Taux de Chômage")
                        fig_chomage = px.line(
                            chomage_data,
                            x='date',
                            y='taux_chomage_pourcentage',
                            color='region',
                            title="Taux de Chômage par Région (%)"
                        )
                        st.plotly_chart(fig_chomage, use_container_width=True)
                else:
                    st.info("Aucune donnée économique disponible pour les filtres sélectionnés")
            
            # DONNÉES HYDROLOGIQUES
            elif analyse_type == "💧 Données Hydrologiques":
                st.write("### 💧 Analyse des Données Hydrologiques")
                
                hydro_cols = ['niveau_eau_metres', 'debit_m3par_seconde', 'qualite_eau_ph']
                hydro_data = df_filtered[['date', 'region'] + hydro_cols].copy()
                hydro_data = hydro_data.dropna(subset=hydro_cols, how='all')
                
                if not hydro_data.empty:
                    col1, col2 = st.columns(2)
                    
                    with col1:
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
                            st.plotly_chart(fig_niveau, use_container_width=True)
                    
                    with col2:
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
                            st.plotly_chart(fig_debit, use_container_width=True)
                    
                    # Qualité de l'eau
                    qualite_data = hydro_data.dropna(subset=['qualite_eau_ph'])
                    if not qualite_data.empty:
                        st.write("#### 🧪 Qualité de l'Eau (pH)")
                        fig_qualite = px.scatter(
                            qualite_data,
                            x='date',
                            y='qualite_eau_ph',
                            color='region',
                            title="Évolution du pH de l'Eau par Région"
                        )
                        st.plotly_chart(fig_qualite, use_container_width=True)
                else:
                    st.info("Aucune donnée hydrologique disponible pour les filtres sélectionnés")
            
            # VUE D'ENSEMBLE
            elif analyse_type == "📊 Vue d'ensemble":
                st.write("### 📊 Vue d'ensemble de toutes les données")
                
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
                    st.plotly_chart(fig_overview, use_container_width=True)
                    
                    # Tableau récapitulatif
                    st.write("#### 📋 Récapitulatif des données")
                    st.dataframe(df_counts, use_container_width=True)
                else:
                    st.info("Aucune donnée disponible pour créer la vue d'ensemble")
            
            # Tableau des données détaillées
            st.subheader("📋 Données Détaillées (100 dernières entrées)")
            
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

# Vue Géographique
elif page == "🌍 Vue Géographique":
    st.header("🌍 Analyse Géographique des Données")
    
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
        st.subheader("🗺️ Carte Interactive des Stations")
        
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
        
        fig_map.update_layout(mapbox_style="open-street-map")
        fig_map.update_layout(margin={"r":0,"t":50,"l":0,"b":0})
        
        st.plotly_chart(fig_map, use_container_width=True)
        
        # Statistiques par région
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Mesures par Région")
            fig_region = px.treemap(
                df_geo_detail,
                path=['region', 'commune'],
                values='nb_mesures',
                title="Hiérarchie des Mesures par Zone"
            )
            st.plotly_chart(fig_region, use_container_width=True)
        
        with col2:
            st.subheader("🌡️ Températures Moyennes")
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
                fig_temp_map.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_temp_map, use_container_width=True)
            else:
                st.info("Pas de données de température disponibles")
    else:
        st.info("Aucune donnée géographique disponible")

# Tendances Temporelles
elif page == "📈 Tendances Temporelles":
    st.header("📈 Analyse des Tendances Temporelles")
    
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
        st.subheader("📊 Évolution des Variables Climatiques")
        
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
        
        fig_evolution.update_layout(height=700, title_text="Évolution Temporelle des Variables Climatiques")
        st.plotly_chart(fig_evolution, use_container_width=True)
        
        # Analyse saisonnière
        st.subheader("🍂 Analyse Saisonnière")
        
        df_saison = df_temporal.groupby('saison').agg({
            'temp_moyenne': 'mean',
            'pluie_totale': 'sum',
            'humidite_moyenne': 'mean',
            'nb_mesures': 'sum'
        }).reset_index()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_saison_temp = px.bar(
                df_saison,
                x='saison',
                y='temp_moyenne',
                title="Température Moyenne par Saison",
                color='temp_moyenne',
                color_continuous_scale='RdYlBu_r'
            )
            st.plotly_chart(fig_saison_temp, use_container_width=True)
        
        with col2:
            fig_saison_pluie = px.bar(
                df_saison,
                x='saison',
                y='pluie_totale',
                title="Pluviométrie Totale par Saison",
                color='pluie_totale',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_saison_pluie, use_container_width=True)
    else:
        st.info("Aucune donnée temporelle disponible")

# Sources de Données
elif page == "📋 Sources de Données":
    st.header("📋 Gestion des Sources de Données")
    
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
        st.subheader("🏢 Vue d'Ensemble des Sources")
        
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
        st.subheader("📈 Contribution de Chaque Source")
        
        fig_contrib = px.bar(
            df_sources_detail,
            x='nb_mesures_total',
            y='acronyme',
            orientation='h',
            title="Nombre de Mesures par Source de Données",
            color='nb_mesures_total',
            color_continuous_scale='viridis'
        )
        
        fig_contrib.update_layout(height=400)
        st.plotly_chart(fig_contrib, use_container_width=True)
        
        # Tableau détaillé
        st.subheader("📋 Détails des Sources")
        st.dataframe(
            df_sources_detail[[
                'acronyme', 'nom_source', 'type_source', 
                'nb_mesures_total', 'nb_zones_couvertes'
            ]].style.format({
                'nb_mesures_total': '{:,}',
                'nb_zones_couvertes': '{:,}'
            }),
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