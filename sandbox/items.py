# This can be moved to some other places
[    {
        'id': 0,
        'name': 'douleur',
        'description': 'semble être ou dit être douloureux',
        'score': (('oui', 0), ('non', 4)),
        'weight': 30,
        'prob': (0.25, 0.75)
    },
    {
        'id': 1,
        'name': 'tristesse',
        'description': 'semble être ou dit être triste',
        'score': (('oui', 2), ('non', 4)),
        'weight': 30,
        'prob': (0.15, 0.85)
    },
    {
        'id': 2,
        'name': 'fièvre',
        'description': 'semble être ou dit être fiévreux',
        'score': (('oui', 0), ('non', 4)),
        'weight': 50,
        'prob': (0.05, 0.95)
    },
    {
        'id': 3,
        'name': 'agitation',
        'description': 'Agité et /ou agressif',
        'score': (('oui', 0), ('non', 4)),
        'weight': 60,
        'prob': (0.25, 0.75)
    },
    {
        'id': 4,
        'name': 'fatigue',
        'description': 'semble être ou dit être fatigué',
        'score': (('fatigue intense', 0), ('fatigue moyenne', 2), ('fatigue légère', 3), ('pas de fatigue', 4)),
        'weight': 20,
        'prob': (0.05, 0.15, 0.55, 0.25)
    },
    {
        'id': 5,
        'name': 'déplacement',
        'description': 'déplacement dans la journée qui vient de passer',
        'score': (('0 pas', 0), ('quelques pas en chambre', 1), ('a marché hors de la chambre', 3)),
        'weight': 40,
        'prob': (0.05, 0.25, 0.7)
    },
    {
        'id': 6,
        'name': 'selles_quantité',
        'description': 'selles quantité',
        'score': (('0 croix', 0), ('1 croix', 1), ('2 croix', 2), ('3 croix', 3)),
        'weight': 40,
        'prob': (0.05, 0.15, 0.45, 0.35)
    },
    {
        'id': 7,
        'name': 'selles_texture',
        'description': 'selles texture',
        'score': (('dures', 0), ('liquides', 0), ('normales/molles', 4)),
        'weight': 30,
        'prob': (0.2, 0.05, 0.75)
    },
    {
        'id': 8,
        'name': 'sommeil',
        'description': 'sommeil déclaré par le résident ou constaté par soignant',
        'score': (('mauvais ', 1), ('moyen ', 2), ('bon', 3)),
        'weight': 50,
        'prob': (0.05, 0.75, 0.2)
    },
    {
        'id': 9,
        'name': 'appétit',
        'description': 'appétit : part des repas du jour (récolté dans netsoins si on compte sur la RV)',
        'score': (('0 ', 0), ('1/2', 2), ('3/4', 3), ('tout', 4)),
        'weight': 60,
        'prob': (0.0, 0.1, 0.55, 0.35)
    },
    {
        'id': 10,
        'name': 'hydratation',
        'description': 'hydratation orale du jour (récolté ds netsoins si on compte sur la RV)',
        'score': (('0 ', 0), ('6 ou 7 verres', 1), ('8 ou 9 verres', 3), ('10 verres ou plus', 4)),
        'weight': 80,
        'prob': (0.01, 0.1, 0.75, 0.14)
    },
    {
        'id': 11,
        'name': 'fall',
        'description': 'chute au cours des 7 derniers jours (récolté dans netsoins)',
        'score': (('oui', 0), ('non', 4)),
        'weight': 100,
        'prob': (0.01, 0.99)
    }
]
