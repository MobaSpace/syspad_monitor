# This can be moved to some other places
[    {
        'id': 0,
        'name': 'douleur',
        'description': 'Semble être ou dit être douloureux?',
        'score': (('oui', 0), ('non', 4)),
        'weight': 30,
        'prob': (0.25, 0.75),
        'type': 'mandatory'
    },
    {
        'id': 1,
        'name': 'tristesse',
        'description': 'Semble être ou dit être triste?',
        'score': (('oui', 2), ('non', 4)),
        'weight': 30,
        'prob': (0.15, 0.85),
        'type': 'mandatory'
    },
    {
        'id': 2,
        'name': 'fièvre',
        'description': 'Semble être ou dit être fiévreux?',
        'score': (('oui', 0), ('non', 4)),
        'weight': 50,
        'prob': (0.05, 0.95),
        'type': 'mandatory'
    },
    {
        'id': 3,
        'name': 'agitation',
        'description': 'Agité et/ou agressif?',
        'score': (('oui', 0), ('non', 4)),
        'weight': 60,
        'prob': (0.25, 0.75),
        'type': 'mandatory'
    },
    {
        'id': 4,
        'name': 'fatigue',
        'description': 'Semble être ou dit être fatigué?',
        'score': (('fatigue intense', 0), ('fatigue moyenne', 2), ('fatigue légère', 3), ('pas de fatigue', 4)),
        'weight': 20,
        'prob': (0.05, 0.15, 0.55, 0.25),
        'type': 'optional'
    },
    {
        'id': 5,
        'name': 'déplacement',
        'description': 'Déplacement dans la journée qui vient de passer?',
        'score': (('0 pas', 0), ('Quelques pas en chambre', 1), ('A marché hors de la chambre', 3)),
        'weight': 40,
        'prob': (0.05, 0.25, 0.7),
        'type': 'optional'
    },
    {
        'id': 6,
        'name': 'selles_quantité',
        'description': 'Quantité de selles?',
        'score': (('0 ou Traces', 0), ('Peu', 1), ('Normales', 2), ('Beaucoup', 3)),
        'weight': 40,
        'prob': (0.05, 0.15, 0.45, 0.35),
        'type': 'optional'
    },
    {
        'id': 7,
        'name': 'selles_texture',
        'description': 'Texture des selles?',
        'score': (('Dures', 0), ('Liquides', 0), ('Normales/Molles', 4)),
        'weight': 30,
        'prob': (0.2, 0.05, 0.75),
        'type': 'optional'
    },
    {
        'id': 8,
        'name': 'sommeil',
        'description': 'Sommeil déclaré par le résident ou constaté par soignant?',
        'score': (('Mauvais ', 1), ('Moyen ', 2), ('Bon', 3)),
        'weight': 50,
        'prob': (0.05, 0.75, 0.2),
        'type': 'optional'
    },
    {
        'id': 9,
        'name': 'appétit',
        'description': 'Moyenne des part de repas pris ce jour (tout repas confondu)?',
        'score': (('0 ', 0), ('1/2', 2), ('3/4', 3), ('tout', 4)),
        'weight': 60,
        'prob': (0.0, 0.1, 0.55, 0.35),
        'type': 'optional'
    },
    {
        'id': 10,
        'name': 'hydratation',
        'description': 'Hydratation totale du jour (toute boisson confondue)?',
        'score': (('0 ', 0), ('6 ou 7 verres', 1), ('8 ou 9 verres', 3), ('10 verres ou plus', 4)),
        'weight': 80,
        'prob': (0.01, 0.1, 0.75, 0.14),
        'type': 'optional'
    },
    {
        'id': 11,
        'name': 'fall',
        'description': 'Chute au cours des 7 derniers jours (récolté dans netsoins)',
        'score': (('oui', 0), ('non', 4)),
        'weight': 100,
        'prob': (0.01, 0.99),
        'type': 'mandatory'
    }
]
