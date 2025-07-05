# Villes disponibles dans vos données
CITIES = [
    "Paris",
    "London",
    "Tokyo",
    "New York",
    "Sydney"
]

# Paramètres des scores
SCORE_PARAMS = {
    'temp_range': (22, 28),  # Température idéale en °C
    'temp_weight': 0.5,       # Pondération température
    'rain_weight': 0.3,       # Pondération pluie
    'wind_weight': 0.2        # Pondération vent
}

# Noms des mois
MONTH_NAMES = {
    1: "Janvier", 2: "Février", 3: "Mars",
    4: "Avril", 5: "Mai", 6: "Juin",
    7: "Juillet", 8: "Août", 9: "Septembre",
    10: "Octobre", 11: "Novembre", 12: "Décembre"
}