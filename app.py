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

# CSS personnalisé
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 0.5rem;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .sidebar .sidebar-content {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
</style>
""", unsafe_allow_html=True)

# En-tête principal
st.markdown('<h1 class="main-header">🌍 WASCAL Data Warehouse</h1>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center; font-size: 1.2rem; color: #666;">Système de Reporting pour les Données Climatiques et Agricoles d\'Afrique de l\'Ouest</p>', unsafe_allow_html=True)

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
    ["🏠 Dashboard Principal", "🌡️ Analyse Climatique", "🌍 Vue Géographique", "📈 Tendances Temporelles", "📋 Sources de Données"]
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

# Analyse Climatique
elif page == "🌡️ Analyse Climatique":
    st.header("🌡️ Analyse des Données Climatiques")
    
    # Données climatiques
    query_climat = """
    SELECT 
        t.date,
        g.region,
        g.commune,
        f.temperature_celsius,
        f.pluviometri_mm,
        f.humidite_pourcentage,
        f.vitesse_vent_kmh,
        s.acronyme as source
    FROM wascal.table_des_faits f
    JOIN wascal.dim_temps t ON f.id_temps = t.id_temps
    JOIN wascal.dim_geographique g ON f.id_geographique = g.id_geographique
    JOIN wascal.dim_source_donnees s ON f.id_source = s.id_source
    WHERE f.temperature_celsius IS NOT NULL 
       OR f.pluviometri_mm IS NOT NULL 
       OR f.humidite_pourcentage IS NOT NULL
    ORDER BY t.date DESC
    """
    
    df_climat = run_query(query_climat)
    
    if not df_climat.empty:
        # Filtres
        col1, col2 = st.columns(2)
        with col1:
            regions_selected = st.multiselect(
                "Sélectionnez les régions",
                options=df_climat['region'].unique().tolist(),
                default=df_climat['region'].unique().tolist()
            )
        
        with col2:
            sources_selected = st.multiselect(
                "Sélectionnez les sources",
                options=df_climat['source'].unique().tolist(),
                default=df_climat['source'].unique().tolist()
            )
        
        # Filtrage des données
        df_filtered = df_climat[
            (df_climat['region'].isin(regions_selected)) &
            (df_climat['source'].isin(sources_selected))
        ]
        
        # Graphiques climatiques
        if not df_filtered.empty:
            # Température et pluviométrie
            fig_climat = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Températures par Région', 'Pluviométrie par Région', 
                              'Humidité par Région', 'Vitesse du Vent par Région'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            # Température
            for region in df_filtered['region'].unique():
                data_region = df_filtered[df_filtered['region'] == region]
                fig_climat.add_trace(
                    go.Scatter(x=data_region['date'], y=data_region['temperature_celsius'],
                             mode='lines+markers', name=f'Temp {region}'),
                    row=1, col=1
                )
            
            # Pluviométrie
            for region in df_filtered['region'].unique():
                data_region = df_filtered[df_filtered['region'] == region]
                fig_climat.add_trace(
                    go.Bar(x=data_region['date'], y=data_region['pluviometri_mm'],
                          name=f'Pluie {region}', showlegend=False),
                    row=1, col=2
                )
            
            # Humidité
            for region in df_filtered['region'].unique():
                data_region = df_filtered[df_filtered['region'] == region]
                fig_climat.add_trace(
                    go.Scatter(x=data_region['date'], y=data_region['humidite_pourcentage'],
                             mode='lines+markers', name=f'Humidité {region}', showlegend=False),
                    row=2, col=1
                )
            
            # Vitesse du vent
            for region in df_filtered['region'].unique():
                data_region = df_filtered[df_filtered['region'] == region]
                fig_climat.add_trace(
                    go.Scatter(x=data_region['date'], y=data_region['vitesse_vent_kmh'],
                             mode='lines+markers', name=f'Vent {region}', showlegend=False),
                    row=2, col=2
                )
            
            fig_climat.update_layout(height=600, title_text="Analyse Climatique Complète")
            fig_climat.update_xaxes(title_text="Date")
            fig_climat.update_yaxes(title_text="°C", row=1, col=1)
            fig_climat.update_yaxes(title_text="mm", row=1, col=2)
            fig_climat.update_yaxes(title_text="%", row=2, col=1)
            fig_climat.update_yaxes(title_text="km/h", row=2, col=2)
            
            st.plotly_chart(fig_climat, use_container_width=True)
            
            # Tableau des données
            st.subheader("📋 Données Détaillées")
            st.dataframe(df_filtered.style.format({
                'temperature_celsius': '{:.1f}°C',
                'pluviometri_mm': '{:.1f}mm',
                'humidite_pourcentage': '{:.1f}%',
                'vitesse_vent_kmh': '{:.1f}km/h'
            }), use_container_width=True)
        else:
            st.warning("Aucune donnée climatique ne correspond aux filtres sélectionnés")
    else:
        st.info("Aucune donnée climatique disponible")

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
        COUNT(DISTINCT f.id_geographique) as nb_zones_couvertes,
        COUNT(DISTINCT f.id_type_donnees) as nb_types_donnees
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
                
                with col3:
                    st.metric(
                        "Types de Données",
                        f"{source['nb_types_donnees']}",
                        delta="Catégories différentes"
                    )
                
                # Informations de contact
                if source['contact']:
                    st.write(f"📧 **Contact :** {source['contact']}")
                if source['url']:
                    st.write(f"🌐 **Site Web :** {source['url']}")
                if source['date_derniere_maj']:
                    st.write(f"📅 **Dernière MAJ :** {source['date_derniere_maj']}")
        
        # Graphique de contribution
        st.subheader("📈 Contribution de Chaque Source")
        
        fig_contrib = px.horizontal_bar(
            df_sources_detail,
            x='nb_mesures_total',
            y='acronyme',
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
                'nb_mesures_total', 'nb_zones_couvertes', 'nb_types_donnees'
            ]].style.format({
                'nb_mesures_total': '{:,}',
                'nb_zones_couvertes': '{:,}',
                'nb_types_donnees': '{:,}'
            }),
            use_container_width=True
        )
    else:
        st.info("Aucune information sur les sources disponible")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; margin-top: 2rem;'>
    <p>🌍 <strong>WASCAL Data Warehouse</strong> - Développé pour l'analyse des données climatiques et agricoles d'Afrique de l'Ouest</p>
    <p>📊 Données provenant de : ANACIM, ANSD, DAPSA, ISRA, DGPRE</p>
    <p>⚡ Powered by Streamlit & PostgreSQL sur AWS</p>
</div>
""", unsafe_allow_html=True)