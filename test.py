# Importations
import streamlit as st
import pyodbc
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

# Fonctions
def create_connection_string(server, database, username, password):
    """
    Crée une chaîne de connexion pour la base de données SQL Server.

    Args:
        server (str): Nom du serveur.
        database (str): Nom de la base de données.
        username (str): Nom d'utilisateur.
        password (str): Mot de passe.

    Returns:
        str: Chaîne de connexion formatée.
    """
    return (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        'TrustServerCertificate=yes;'
    )

def execute_query(query, connection_string):
    """
    Établit une connexion à la base de données et exécute la requête SQL fournie.

    Args:
        query (str): La requête SQL à exécuter.
        connection_string (str): La chaîne de connexion à la base de données.

    Returns:
        pd.DataFrame: Le DataFrame contenant les résultats de la requête.
    """
    try:
        conn = pyodbc.connect(connection_string)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erreur lors de l'exécution de la requête : {e}")
        return None

# Configuration de la page
st.set_page_config(page_title="Page des KPIs")
st.title("KPI Papier Rolland")
st.subheader("Version 1.0 2024-12-23r")

# Chaînes de connexion
connection_string_bi_staging = create_connection_string(
    server='cpopapbi01',
    database='BI_Staging',
    username='PowerBI',
    password='Stj2020!'
)

connection_string_bi_prep = create_connection_string(
    server='cpopapbi01',
    database='BI_PREP',
    username='PowerBI',
    password='Stj2020!'
)

connection_string_prod_pdtazure = create_connection_string(
    server='SRV-SQLPROD-01',
    database='PROD_PDTAzure',
    username='PDTViewer',
    password='PDTViewer'
)



def mappingBT(df_rts):
    # Vérifier si la colonne 'FUNCTIONAL_LOCATION' existe dans le DataFrame
    if 'FUNCTIONALLOCATIONID' in df_rts.columns:
        # Extraire les trois premiers caractères de 'FUNCTIONAL_LOCATION'
        df_rts['Prefix'] = df_rts['FUNCTIONALLOCATIONID'].str[:3]
    
        # Dictionnaire de mappage des préfixes aux centres de coût avec corrections orthographiques
        mapping = {
            '080': 'Recyclage général',
            '090': 'Plan de pigment',
            '150': 'Préparation des pâtes M6',
            '160': 'Préparation des pâtes M6',
            '170': 'Préparation des pâtes M7',
            '180': 'Préparation des pâtes M8',
            '190': 'Adjuvant',
            '200': 'Département de production général',
            '220': 'Rebobineuse de reprise',
            '250': 'Récupération et bobineuse M6',
            '260': 'M6',
            '270': 'M7',
            '280': 'M8',
            '300': 'Finition B',
            '400': 'Finition A',
            '500': 'Centrale thermique',
            '510': 'Alimentation eau fraîche',
            '520': 'Alimentation électrique',
            '540': 'Département entretien',
            '636': 'Recyclage des huiles usées',
            '660': 'Contrôle qualité',
            '675': 'Laboratoire technique',
            '760': 'Bâtiment',
            'P10': 'Projet'
        }
    
        # Appliquer le mappage pour créer la colonne 'Centre de coût'
        df_rts['Centre de coût'] = df_rts['Prefix'].map(mapping)
    
        # Supprimer la colonne temporaire 'Prefix'
        df_rts.drop(columns=['Prefix'], inplace=True)
    
        # Afficher le DataFrame mis à jour
        st.dataframe(df_rts)
    else:
        st.error("La colonne 'FUNCTIONAL_LOCATION' est absente du DataFrame.")
    
    
    
    return df_rts

# Section 1: Données des RTs
st.header("1. Données des RTs")
st.write(
    "Cette section présente les enregistrements de la table 'ROLDynawayWorksheetKPIRequest' de la base de données 'BI_Staging'."
)

# Exécuter la requête pour obtenir les données
query_rts = "SELECT * FROM dbo.ROLDynawayWorksheetKPIRequest;"
df_rtsnum1 = execute_query(query_rts, connection_string_bi_staging)

if df_rtsnum1 is not None:
    # Vérifier si la colonne 'FUNCTIONAL_LOCATION' existe dans le DataFrame
    if 'FUNCTIONAL_LOCATION' in df_rtsnum1.columns:
        # Extraire les trois premiers caractères de 'FUNCTIONAL_LOCATION'
        df_rtsnum1['Prefix'] = df_rtsnum1['FUNCTIONAL_LOCATION'].str[:3]
        
        # Dictionnaire de mappage des préfixes aux centres de coût
        mapping = {
            '080': 'Recyclage général',
            '090': 'Plan de pigment',
            '150': 'Préparation des pâtes M6',
            '160': 'Préparation des pâtes M6',
            '170': 'Préparation des pâtes M7',
            '180': 'Préparation des pâtes M8',
            '190': 'Adjuvant',
            '200': 'Département de production général',
            '220': 'Rebobineuse de reprise',
            '250': 'Récupération et bobineuse M6',
            '260': 'M6',
            '270': 'M7',
            '280': 'M8',
            '300': 'Finition B',
            '400': 'Finition A',
            '500': 'Centrale thermique',
            '510': 'Alimentation eau fraîche',
            '520': 'Alimentation électrique',
            '540': 'Département entretien',
            '636': 'Recyclage des huiles usées',
            '660': 'Contrôle qualité',
            '675': 'Laboratoire technique',
            '760': 'Bâtiment',
            'P10': 'Projet'
        }

        # Appliquer le mappage
        df_rtsnum1['Centre de coût'] = df_rtsnum1['Prefix'].map(mapping)
        df_rtsnum1.drop(columns=['Prefix'], inplace=True)
    else:
        st.error("La colonne 'FUNCTIONAL_LOCATION' est absente du DataFrame.")

    # S'assurer que 'WORKORDER_ID' est de type chaîne
    if 'WORKORDER_ID' in df_rtsnum1.columns:
        df_rtsnum1['WORKORDER_ID'] = df_rtsnum1['WORKORDER_ID'].astype(str)
    else:
        st.error("La colonne 'WORKORDER_ID' est absente du DataFrame.")
    
    # Appliquer les filtres initiaux
    if {'WORKORDER_ID', 'STAGEID'}.issubset(df_rtsnum1.columns):
        filtre_workorder = ~df_rtsnum1['WORKORDER_ID'].str.startswith('BT-')
        valeurs_exclues = ['ANNULE', 'COMPLETE', 'FERMER']
        filtre_stageid = ~df_rtsnum1['STAGEID'].isin(valeurs_exclues)
        df_rts = df_rtsnum1[filtre_workorder & filtre_stageid]

        st.write("Aperçu des données de RTs :")
        st.dataframe(df_rts)
        # Vérifier la présence des colonnes nécessaires
        required_columns = {'Centre de coût', 'STAGEID', 'REQUESTID', 'ACTUALSTART', 'REQUEST_TYPE'}
        if not required_columns.issubset(df_rts.columns):
            st.error(f"Les colonnes suivantes sont manquantes dans les données : {required_columns - set(df_rts.columns)}")
        else:
            # Convertir 'ACTUALSTART' en datetime
            df_rts['ACTUALSTART'] = pd.to_datetime(df_rts['ACTUALSTART'], errors='coerce')

            # Supprimer les lignes avec des dates invalides
            df_rts.dropna(subset=['ACTUALSTART'], inplace=True)

            # Sélecteur de plage de dates
            min_date = df_rts['ACTUALSTART'].min().date()
            max_date = df_rts['ACTUALSTART'].max().date()
            date_range = st.slider(
                "Sélectionnez une plage de dates :",
                min_value=min_date,
                max_value=max_date,
                value=(min_date, max_date),
                format="YYYY-MM-DD"
            )

            # Filtrer les données
            df_filtre = df_rts[
                (df_rts['ACTUALSTART'].dt.date >= date_range[0]) &
                (df_rts['ACTUALSTART'].dt.date <= date_range[1])
            ]

            if not df_filtre.empty:
                # Grouper les données
                df_grouped = df_filtre.groupby(['Centre de coût', 'STAGEID']).agg(
                    Nombre_Occurrences=('REQUESTID', 'nunique')
                ).reset_index()

                # Pivot et affichage du graphique
                df_pivot = df_grouped.pivot(index='Centre de coût', columns='STAGEID', values='Nombre_Occurrences').fillna(0)
                st.bar_chart(df_pivot)
            else:
                st.write("Aucune donnée disponible pour les sélections effectuées.")
    else:
        st.error("Les colonnes nécessaires pour le filtrage sont absentes.")
else:
    st.error("Aucune donnée n'a été chargée depuis la base de données.")
    




# Section 2: Données de BT 
st.header("2. Données de BT")
st.write(
    "Cette section présente les enregistrements de la table 'ROLDynawayWorksheetAll' de la base de données 'BI_Staging'."
)

query_bt = "SELECT * FROM dbo.ROLDynawayWorksheetAll;"
df_bt = execute_query(query_bt, connection_string_bi_staging)

if df_bt is not None:
    st.write("Aperçu des données de BT :")
    df_bt=mappingBT(df_bt)

    # Vérifier la présence des colonnes nécessaires
    required_columns = {'CREATEDDATETIME', 'Centre de coût', 'STAGEID'}
    if not required_columns.issubset(df_bt.columns):
        st.error(f"Les colonnes suivantes sont manquantes dans les données : {required_columns - set(df_bt.columns)}")
    else:
        # Convertir 'CREATEDDATETIME' en datetime
        df_bt['CREATEDDATETIME'] = pd.to_datetime(df_bt['CREATEDDATETIME'], errors='coerce')

        # Supprimer les lignes avec des dates invalides
        df_bt = df_bt.dropna(subset=['CREATEDDATETIME'])

        # Ajouter un sélecteur de plage de dates
        min_date = df_bt['CREATEDDATETIME'].min().date()
        max_date = df_bt['CREATEDDATETIME'].max().date()
        date_range = st.slider(
            "Sélectionnez une plage de dates :",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="YYYY-MM-DD"
        )

        # Filtrer les données selon la plage de dates sélectionnée
        df_bt = df_bt[(df_bt['CREATEDDATETIME'].dt.date >= date_range[0]) & (df_bt['CREATEDDATETIME'].dt.date <= date_range[1])]

        # Regrouper les données par Centre de Coût et STAGEID
        grouped_data = df_bt.groupby(["Centre de coût", "STAGEID"]).size().reset_index(name="Occurrences")

        # Créer une table pivot pour un affichage clair dans un graphique
        pivot_table = grouped_data.pivot(index="Centre de coût", columns="STAGEID", values="Occurrences").fillna(0)

        # Afficher le graphique
        st.bar_chart(pivot_table)
    
    

# Section 3: Données de Perte de Temps
st.header("3. Données de Perte de Temps")
st.write(
    "Cette section présente les enregistrements de la vue 'vPerteTempsExtraction' de la base de données 'PROD_PDTAzure'."
)

query_perte_temps = "SELECT * FROM dbo.vPerteTempsExtraction;"
df_perte_temps = execute_query(query_perte_temps, connection_string_prod_pdtazure)

if df_perte_temps is not None:
    st.write("Aperçu des données de perte de temps :")
    st.dataframe(df_perte_temps)

    # Vérifier la présence des colonnes nécessaires
    required_columns = {'DateDebut', 'DureeSecondaire', 'Nom', 'Secteur'}
    if not required_columns.issubset(df_perte_temps.columns):
        st.error(f"Les colonnes suivantes sont manquantes dans les données : {required_columns - set(df_perte_temps.columns)}")
    else:
        # Convertir 'DateDebut' en datetime
        df_perte_temps['DateDebut'] = pd.to_datetime(df_perte_temps['DateDebut'], errors='coerce')

        # Supprimer les lignes avec des dates invalides
        df_perte_temps = df_perte_temps.dropna(subset=['DateDebut'])

        # Ajouter un sélecteur multiple pour la colonne 'Secteur'
        secteurs = df_perte_temps['Secteur'].unique()
        secteurs_selectionnes = st.multiselect('Sélectionnez un ou plusieurs secteurs :', secteurs)

        # Ajouter un sélecteur multiple pour la colonne 'Nom'
        noms = df_perte_temps['Nom'].unique()
        noms_selectionnes = st.multiselect('Sélectionnez un ou plusieurs noms :', noms)

        # Ajouter un sélecteur de plage de dates
        min_date = df_perte_temps['DateDebut'].min().date()
        max_date = df_perte_temps['DateDebut'].max().date()
        date_range = st.slider(
            "Sélectionnez une plage de dates :",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="YYYY-MM-DD"
        )

        # Filtrer les données en fonction des sélections
        df_filtre = df_perte_temps[
            (df_perte_temps['Secteur'].isin(secteurs_selectionnes) if secteurs_selectionnes else True) &
            (df_perte_temps['Nom'].isin(noms_selectionnes) if noms_selectionnes else True) &
            (df_perte_temps['DateDebut'].dt.date >= date_range[0]) &
            (df_perte_temps['DateDebut'].dt.date <= date_range[1])
        ]

        if not df_filtre.empty:
            # Préparer les données pour le graphique en aires empilées
            df_pivot = df_filtre.pivot_table(
                index='DateDebut',
                columns='Nom',
                values='DureeSecondaire',
                aggfunc='sum'
            ).fillna(0)

            # Afficher le graphique en aires empilées
            st.area_chart(df_pivot)
        else:
            st.write("Aucune donnée disponible pour les sélections effectuées.")
 
# Section 4: Données de Perte de Temps Non planifié
st.header("4. Données de Perte de Temps Non planifiées")
st.write(
    "Cette section présente les enregistrements de la vue 'vPerteTempsExtraction' de la base de données 'PROD_PDTAzure'."
)

# Exécuter la requête pour obtenir les données
query_perte_temps = "SELECT * FROM dbo.vPerteTempsExtraction;"
df_perte_tempsnonplan = execute_query(query_perte_temps, connection_string_prod_pdtazure)

# Vérifier si des données ont été récupérées
if df_perte_tempsnonplan is not None and not df_perte_tempsnonplan.empty:
    # Filtrer les données pour 'Non-Planifié'
    df_non_planifie = df_perte_tempsnonplan[df_perte_tempsnonplan['Nom'] == 'Non-Planifié']

    # Convertir 'DateDebut' en datetime
    df_non_planifie['DateDebut'] = pd.to_datetime(df_non_planifie['DateDebut'], errors='coerce')

    # Supprimer les lignes avec des dates invalides
    df_non_planifie = df_non_planifie.dropna(subset=['DateDebut'])

    # Ajouter un filtre multisélection pour la colonne 'Secteur'
    secteurs_disponibles = df_non_planifie['Secteur'].unique()
    secteurs_selectionnes = st.multiselect(
        "Sélectionnez un ou plusieurs secteurs :",
        options=secteurs_disponibles,
        default=secteurs_disponibles  # Par défaut, tous les secteurs sont sélectionnés
    )

    # Appliquer le filtre sur les secteurs sélectionnés
    df_filtre = df_non_planifie[df_non_planifie['Secteur'].isin(secteurs_selectionnes)]

    # Définir les dates limites
    date_aujourdhui = datetime.now().date()
    date_max_retour = date_aujourdhui - timedelta(days=730)  # Deux ans en arrière

    # Slider pour sélectionner le nombre de jours à afficher
    nb_jours = st.slider(
        "Sélectionnez le nombre de jours à afficher",
        min_value=1,
        max_value=730,
        value=30,  # Valeur par défaut : 30 jours
        step=1,
        help="Déplacez le curseur pour sélectionner la période à afficher, jusqu'à un maximum de deux ans."
    )

    # Calculer la date de début en fonction du nombre de jours sélectionné
    date_debut = date_aujourdhui - timedelta(days=nb_jours)

    # Filtrer les données en fonction de la période sélectionnée
    df_filtre = df_filtre[
        (df_filtre['DateDebut'].dt.date >= date_debut) &
        (df_filtre['DateDebut'].dt.date <= date_aujourdhui)
    ]

    # Vérifier si le DataFrame filtré n'est pas vide
    if not df_filtre.empty:
        # Calculer la somme des durées en secondes et le nombre d'occurrences par 'TypeCauses'
        df_aggregated = df_filtre.groupby('TypeCauses').agg(
            Nombre_Occurrences=('TypeCauses', 'size'),
            Somme_Duree_Minutes=('DureeSecondaire', 'sum')
        ).reset_index()

        # Calculer la durée par occurrence en minutes
        df_aggregated['Duree_Par_Occurrence_Minutes'] = df_aggregated['Somme_Duree_Minutes'] / df_aggregated['Nombre_Occurrences'] 

        # Ajouter une colonne pour les durées sous forme de liste (pour le graphique)
        df_aggregated['Durees'] = df_filtre.groupby('TypeCauses')['DureeSecondaire'].apply(list).reset_index(drop=True)

        # Afficher le DataFrame avec la configuration des colonnes
        st.dataframe(
            df_aggregated,
            column_config={
                'TypeCauses': 'Type de Causes',
                'Nombre_Occurrences': st.column_config.NumberColumn(
                    'Nombre d\'Occurrences',
                    help='Nombre total d\'occurrences pour chaque type de cause',
                    format='%d'
                ),
                'Duree_Par_Occurrence_Minutes': st.column_config.NumberColumn(
                    'Durée par Occurrence (Minutes)',
                    help='Durée moyenne par occurrence en minutes pour chaque type de cause',
                    format='%.2f'
                ),
                'Durees': st.column_config.LineChartColumn(
                    'Distribution des Durées (Minutes)',
                    help='Graphique montrant la distribution des durées en Minutes pour chaque type de cause',
                    y_min=0
                )
            },
            hide_index=True
        )
        
        # Calculer la somme des durées en secondes et le nombre d'occurrences par 'TypeCauses'
        df_aggregated = df_filtre.groupby('ActionsInterventions').agg(
            Nombre_Occurrences=('ActionsInterventions', 'size'),
            Somme_Duree_Minutes=('DureeSecondaire', 'sum')
        ).reset_index()

        # Calculer la durée par occurrence en minutes
        df_aggregated['Duree_Par_Occurrence_Minutes'] = df_aggregated['Somme_Duree_Minutes'] / df_aggregated['Nombre_Occurrences'] 

        # Ajouter une colonne pour les durées sous forme de liste (pour le graphique)
        df_aggregated['Durees'] = df_filtre.groupby('ActionsInterventions')['DureeSecondaire'].apply(list).reset_index(drop=True)

        # Afficher le DataFrame avec la configuration des colonnes
        st.dataframe(
            df_aggregated,
            column_config={
                'ActionsInterventions': 'Actions',
                'Nombre_Occurrences': st.column_config.NumberColumn(
                    'Nombre d\'Occurrences',
                    help="Nombre total d\'occurrences pour chaque type d'action",
                    format='%d'
                ),
                'Duree_Par_Occurrence_Minutes': st.column_config.NumberColumn(
                    'Durée par Occurrence (Minutes)',
                    help="Durée moyenne par occurrence en minutes pour chaque type d'action",
                    format='%.2f'
                ),
                'Durees': st.column_config.LineChartColumn(
                    'Distribution des Durées (Minutes)',
                    help="Graphique montrant la distribution des durées en Minutes pour chaque type d'action",
                    y_min=0
                )
            },
            hide_index=True
        )
        
    else:
        st.write("Aucune donnée 'Non-Planifié' disponible pour les critères sélectionnés.")
else:
    st.write("Aucune donnée disponible.")
    
    
# Section 5: Données de la production
st.header("5. Données de la production")
st.write(
    "Cette section présente les enregistrements de la table 'Daily Production' de la base de données 'BI_PREP'."
)

query_prod = (
    "SELECT * FROM [dbo].[Daily Production] "
    "WHERE Secteur = 'Usine' AND Date >= '2022-01-01';"
)
df_prod = execute_query(query_prod, connection_string_bi_prep)


if df_prod is not None:
    st.write("Aperçu des données de production :")
    st.dataframe(df_prod)

    # Convertir la colonne 'Date' en datetime
    df_prod['Date'] = pd.to_datetime(df_prod['Date'], errors='coerce')

    # Supprimer les lignes avec des dates invalides
    df_prod = df_prod.dropna(subset=['Date'])

    # Définir 'Date' comme index
    df_prod.set_index('Date', inplace=True)

    # Sélectionner les colonnes pertinentes
    df_prod_selected = df_prod[['Production Nette (lb)', 'Production Brute (lb)']]

    # Définir la date minimale et maximale pour le date_input
    min_date = df_prod.index.min().date()
    max_date = df_prod.index.max().date()

    # Afficher le champ de saisie de dates pour sélectionner la plage
    start_date, end_date = st.date_input(
        "Sélectionnez la plage de dates",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        format="YYYY-MM-DD"
    )

    # Vérifier que les dates sont dans le bon ordre
    if start_date > end_date:
        st.error("La date de début doit être antérieure ou égale à la date de fin.")
    else:
        # Filtrer les données en fonction de la plage de dates sélectionnée
        df_filtered = df_prod_selected.loc[start_date:end_date]

        # Afficher le graphique linéaire des données filtrées
        st.line_chart(df_filtered)


# Section 6: Données de la production
st.header("6. Données de consommation d'eau")
st.write(
    "Cette section présente les enregistrements de la table 'Water Consumption' de la base de données 'BI_PREP'."
)

query_eau = (
    "SELECT * FROM [dbo].[Water Consumption] "
    "WHERE Secteur = 'Usine' AND Date >= '2022-01-01';"
)
df_prod = execute_query(query_eau, connection_string_bi_prep)


if df_prod is not None:
    st.write("Aperçu des données de consommation d'eau:")
    st.dataframe(df_prod)

    # Convertir la colonne 'Date' en datetime
    df_prod['Date'] = pd.to_datetime(df_prod['Date'], errors='coerce')

    # Supprimer les lignes avec des dates invalides
    df_prod = df_prod.dropna(subset=['Date'])

    # Définir 'Date' comme index
    df_prod.set_index('Date', inplace=True)


    # Définir la date minimale et maximale pour le date_input
    min_date = df_prod.index.min().date()
    max_date = df_prod.index.max().date()

    # Afficher le champ de saisie de dates pour sélectionner la plage
    start_date, end_date = st.date_input(
        "Sélectionnez la plage de dates",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        format="YYYY-MM-DD"
    )

    # Vérifier que les dates sont dans le bon ordre
    if start_date > end_date:
        st.error("La date de début doit être antérieure ou égale à la date de fin.")
    else:
        # Filtrer les données en fonction de la plage de dates sélectionnée
        df_filtered = df_prod_selected.loc[start_date:end_date]

        # Afficher le graphique linéaire des données filtrées
        st.line_chart(df_filtered)
   