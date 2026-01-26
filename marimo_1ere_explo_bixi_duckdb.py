# td_bixi_marimo.py
# ============================================================
# TD Marimo — Analyse des données Bixi avec DuckDB
# M1 MIAGE UT Capitole — Antoine Giraud
# ============================================================

import marimo

__generated_with = "0.19.6"
app = marimo.App(app_title="1ère explo DuckDB")


@app.cell(hide_code=True)
def intro(mo):
    mo.md(r"""
    # TD — Analyse des stations **bixi** 🚲

    Ce TD vous guide dans l’exploration de données réelles :
    - Stations Bixi (JSON)
    - Secteurs municipaux (GeoJSON)
    - Locations journalières (Parquet)

    Vous utiliserez **DuckDB** et son extension **spatial**.

    👉 Certaines cellules contiennent des `TODO` à compléter.


    ### **GBFS** : le remplissage temps réel des stations

    L'offre des stations bixi & leur remplissange en vélos [est publiée](https://gbfs.velobixi.com/gbfs/2-2/gbfs.json) dans le format GBFS

    Chaque minutes l'état des stations est mis à jour !

    Les 2 fichiers principaux sont [station_information](https://gbfs.velobixi.com/gbfs/2-2/fr/station_information.json) & [station_status](https://gbfs.velobixi.com/gbfs/2-2/fr/station_status.json)
    """)
    return


@app.cell
def imports():
    import marimo as mo
    import duckdb

    # Create a DuckDB connection
    conn = duckdb.connect("explo_bixi.db")
    # on va travailler avec des coordonnées
    conn.sql('INSTALL spatial; LOAD spatial;')
    return conn, mo


@app.cell(hide_code=True)
def stations_intro(mo):
    mo.md("""
    ## 📦 Exploration des stations Bixi ()

    ### 🚲 Création de la table `station_info`

    Les 2 requêtes suivantes servent d'exemple pour apprendre à manipuler les json avec DuckDB.

    L'enjeu est d'avoir une belle table avec 1 station par ligne & leurs principales infos en colonne (nom, code, coordonnées)

    n'hésitez pas à ouvrir le [station_information.json](https://gbfs.velobixi.com/gbfs/fr/station_information.json) brut dans votre navigateur avec l'option "impression élégante" pour comprendre la structure des données
    """)
    return


@app.cell
def _(conn, mo, station_info_raw):
    _ = mo.sql(
        f"""
        -- on enregistre la table en mémoire
        create or replace table station_info_raw as 
        SELECT unnest("data".stations::json[]) AS station
        FROM read_json_auto('https://gbfs.velobixi.com/gbfs/fr/station_information.json');
        -- affichons les résultats
        from station_info_raw;
        -- huuum super 1 ligne 1 station
        -- MAIS on va vouloir les avoir en colonnes !
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo, station_info_raw):
    station_info = mo.sql(
        f"""
        -- on va extraire des json les colonnes qui nous intéressent
        create or replace table station_info as 
        SELECT
            station->>'name' AS nom,
            (station->>'capacity')::INT AS capacity,
            ST_Point(
                (station->>'lon')::DOUBLE,
                (station->>'lat')::DOUBLE
            ) AS station_geom,
            ST_AsGeoJSON(station_geom) AS geom_json,
        FROM station_info_raw
        WHERE (station->>'lon')::DOUBLE != 0
        ORDER BY 1;
        -- affichons les résultats
        from station_info;
        """,
        engine=conn
    )
    return (station_info,)


@app.cell(hide_code=True)
def exo1_intro(mo):
    mo.md(r"""
    ### 🧪 Exercice 1 — Ajouter une colonne 👆🏻

    dans la requête précédente
    - remonter la colonne permetant de savoir s'il y a une borne de paiement !
    - remonter le code de la station (appelé nom court)
    """)
    return


@app.cell(hide_code=True)
def sectors_intro(mo):
    mo.md("""
    ### 🗺️ Import des secteurs municipaux (OD 2013)

    Des stations seules c'est dommage, il est bon de faire un récap par arrondissement !
    """)
    return


@app.cell
def sectors_table(conn, mo):
    sectors = mo.sql(
        f"""
        CREATE TABLE if not exists sectors AS
        WITH t AS (
            SELECT unnest(features) AS feat
            FROM read_json_auto(
                'https://www.donneesquebec.ca/recherche/dataset/b57cdeb1-98e7-4db7-bb84-32530f0367eb/resource/95ab084b-727e-4322-9433-0fed7baa690d/download/artm-sm-od13.geojson',
                sample_size=-1
            )
        )
        SELECT
            feat.properties.SM13::INT AS sector_id,
            feat.properties.SM13_nom AS sector_name,
            ST_GeomFromGeoJSON(feat.geometry::json) AS sector_geom,
            ST_AsText(sector_geom) AS geom_wkb,
            ST_Centroid(sector_geom) AS sector_centroid
        FROM t;
        -- on les affiche de suite
        from sectors
        """,
        engine=conn
    )
    return


@app.cell(hide_code=True)
def join_intro(mo):
    mo.md("""
    ### 🧭 Stations par secteur (jointure spatiale)

    🧪 **Exercice 2** - à vous de compléter `nb_station` & `capacity`
    """)
    return


@app.cell
def _(conn, mo, sectors, station_info):
    sector_has_stations = mo.sql(
        f"""
        select
        	sector_name,
        	42 as nb_station, --> 🧪 à compléter
        	404 as capacity, --> 🧪 à compléter
            any_value(ST_AsGeoJSON(sector_geom)) AS geom_json,
        from station_info
          left join sectors
          	on ST_Within(station_geom, sector_geom)
        group by 1
        order by 1
        """,
        engine=conn
    )
    return (sector_has_stations,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 🗺️ Carte récap
    """)
    return


@app.cell
def viz_stations(mo, sector_has_stations, station_info):
    import json
    import folium


    # 1) Définir la carte
    m = folium.Map(
        location=[45.54, -73.6],
        zoom_start=12,
        tiles="CartoDB Positron"   # fond clair, parfait pour superposer
    )

    # 2) Ajouter les secteurs en fond
    for row in sector_has_stations.iter_rows(named=True):
        folium.GeoJson(
            json.loads(row['geom_json']),
            name=row['sector_name'],
            popup=folium.Popup(f"{row['sector_name']}<ul><li>{row['nb_station']} stations</li><li>{row['capacity']} ancrages</li></ul>", max_width=200),
            style_function=lambda x: {
                "fillColor": "#cccccc",
                "color": "#555555",
                "weight": 1,
                "fillOpacity": 0.15,   # léger fond transparent
            }
        ).add_to(m)

    # 3) Palette de couleurs par capacité
    bins = [0, 10, 20, 30, 40, 999]
    colors = ["#fee5d9", "#fcae91", "#fb6a4a", "#de2d26", "#a50f15"]

    def color_for_capacity(cap):
        for i in range(len(bins) - 1):
            if bins[i] <= cap < bins[i+1]:
                return colors[i]
        return colors[-1]

    # 4) Ajouter les stations par-dessus
    # Ajouter les stations (en GeoJSON Point)
    for row in station_info.iter_rows(named=True): 
        folium.GeoJson( json.loads(row['geom_json']), popup=folium.Popup(f"{row['nom']}<ul><li>{row['capacity']} ancrages</li></ul>", max_width=200), marker=folium.CircleMarker( 
            radius=6,
            fill=True,
            fill_color=color_for_capacity(row['capacity']),
            fill_opacity=1.0,
            weight=0) ).add_to(m)

    # 7) Affichage
    mo.md("## 🗺️ Carte interactive — Secteurs + Stations")
    mo.md("Cliquez sur les secteurs & stations pour + d'infos")

    m
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 🧪 Aller plus loin & s'entrainer chez soi

    En s'inspirant des requêtes précédentes, allez chercher les données de statut de station et faites une 2nde carte montrant le taux de remplissage des stations avec une échèle de couleur allant du bleu foncé (vide) au rouge foncé (plein)

    -----
    """)
    return


@app.cell(hide_code=True)
def rentals_intro(mo):
    mo.md("""
    ## 📅 Analyse des locations vélo de l'année 2020

    On récupère un fichier .parquet contenant toutes les locations de vélo pour 2020.
    """)
    return


@app.cell
def rentals_load(conn, mo):
    _ = mo.sql(
        f"""
        CREATE TABLE if not exists rentals_2020 AS
        FROM 'hf://datasets/antoinegiraud/bixi_opendata/rentals_2020.parquet';
        -- afficher les résultats
        from rentals_2020 where start_date='2020-04-27';
        """,
        engine=conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Récap des locations bixi par mois de 2020
    """)
    return


@app.cell
def _(conn, mo, rentals_2020):
    _df = mo.sql(
        f"""
        select
            start_date_month as mois,
            count(1) nb_rentals,
            count(distinct start_date) nb_jours,
            --> hé oui, on n'était pas ouvert tous les mois de l'année
        from rentals_2020
        group by start_date_month
        order by 1
        """,
        engine=conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### `row_number` vs `rank` vs `dense_rank`

    Petite apparté sur ces fonctions hautement importantes en ingénierie de données.

    Elles très utilisées dans la déduplications de données, c'est à dire **retirer les doublons** (cf. [article d'Axel Thevenaut](https://medium.com/google-cloud/deduplication-in-bigquery-tables-a-comparative-study-of-7-approaches-f48966eeea2b))

    On les réutilisera dans le TD hypermarché à venir.

    D'ici là, voici une petite requête SQL pour vous y repérer 🕵🏻

    _Question souvent demandé en entretien d'embauche_
    """)
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        --------------------------------------------------------------------
        -- 🦆 DuckDB - Top player -> 🥇🥈🥉 row_number, rank & dense_rank
        --------------------------------------------------------------------
        with players(player, score) as (
          values
        	('Grégoire', 20), ('Corentin', 18), ('Antoine', 30),
        	('bob', 12), ('kevin', 12), ('dylan', 12)
        )
        select
        	*,
        	row_number() over(order by score) rg_rownumber,
        	rank() over(order by score) rg_rank,
        	dense_rank() over(order by score) rg_denserank,
        	row_number() over(partition by score order by player) rg_un_1er_par_score,
        from players
        -- pour filtrer sur le 1er
        -- qualify 1 = rg_rownumber
        -- qualify 1 = rg_rank
        -- qualify 1 = rg_denserank
        -- qualify 1 = rg_un_1er_par_score
        order by score
        """,
        engine=conn
    )
    return


@app.cell(hide_code=True)
def exo3_intro(mo):
    mo.md("""
    ### 🧪 Exercice 3 — Top 3 journées de 2020

    **Point d'attention** : quelle est la granularité de la table rentals_2020 ?
    """)
    return


app._unparsable_cell(
    r"""
    select ...
    from rentals_2020
    group by start_date
    qualify ...
    """,
    name="exo3_query"
)


@app.cell(hide_code=True)
def daily_intro(mo):
    mo.md("""
    ____

    ## 📅 Analyse des locations **journalières** de vélos _(2014 à 2025)_
    """)
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE if not exists rentals_daily AS
        SELECT
            year,
            date,
            SUM(nb_rentals_starts) AS rentals
        FROM 'hf://datasets/antoinegiraud/bixi_opendata/recap_stations_daily/**/*.parquet'
        GROUP BY all;
        -- afficher les données
        from rentals_daily limit 100
        """,
        output=False,
        engine=conn
    )
    return


@app.cell
def daily_load(conn, mo, rentals_daily):
    _ = mo.sql(
        f"""
        -- récap par année
        select
            year,
            sum(rentals) rentals,
            count(1) nb_jours_ouverts,
        from rentals_daily
        group by all
        """,
        engine=conn
    )
    return


@app.cell(hide_code=True)
def exo4_intro(mo):
    mo.md("""
    ## 🧪 Exercice 4 — Year-over-Year **par mois**
    🧪 à vous de compléter `rentals_202x`

    Astuce : regarder la doc duckdb du côté de `nullif` & `count_if`
    """)
    return


@app.cell
def _(conn, mo, rentals_daily):
    _df = mo.sql(
        f"""
        -- YearOverYear evolution
        select
            month(date) as mois,
            1e6 rentals_2022, --> 🧪 à compléter
            2e6 rentals_2023, --> 🧪 à compléter
            3e6 rentals_2024, --> 🧪 à compléter
            (rentals_2024 / nullif(rentals_2023, 0) * 100)::int -100 tx_23_24,
        from rentals_daily
        where year(date)>=2022
        group by 1 order by 1;
        """,
        engine=conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    _____

    ## Apprécier l'écart entre `.parquet`, `.csv`, `.json`
    """)
    return


@app.cell
def _(conn, mo, rentals_2020):
    _df = mo.sql(
        f"""
        copy rentals_2020 to 'data/rentals_2020.parquet';
        copy rentals_2020 to 'data/rentals_2020.csv';
        copy rentals_2020 to 'data/rentals_2020.json';
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT 
            filename,
            (size/1024/1024)::int AS size_mb,
        FROM read_text('data/*')
        order by 2
        """,
        engine=conn
    )
    return


if __name__ == "__main__":
    app.run()
