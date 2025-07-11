# Exam_IA1
🌤️ Climat & Tourisme — Analyse des périodes idéales de voyage
🎯 Objectif

Ce projet vise à déterminer les meilleures périodes pour visiter une ville en fonction de critères météorologiques tels que :

    🌡️ Températures agréables (entre 22°C et 28°C)

    🌧️ Faible précipitation

    💨 Vent modéré

    🌟 Score météo agrégé (personnalisé)

📊 Dashboard Power BI

Le rapport Power BI permet d’explorer les tendances météo mensuelles par ville, à travers 3 visuels principaux :
1. 🌧️ Moyenne de précipitation par mois

    Visualise la quantité moyenne de pluie pour chaque mois.

    Permet d’identifier les saisons sèches.

2. 🌡️ Moyenne de température par mois

    Permet de repérer les mois où la température est dans la zone “idéale” (22-28°C).

3. 🌟 Score météo (agrégé)

    Score personnalisé calculé selon plusieurs critères météo.

    Représenté par ville et par mois pour comparer les destinations.

🧱 Données utilisées

Le modèle repose sur des données issues de :

    🔄 API OpenWeather pour les données en temps réel

    🕰️ Fichier CSV historique pour enrichir l’analyse sur plusieurs jours

    🔄 Données consolidées et nettoyées dans une structure en étoile (dim_ville, dim_date, fact_weather)