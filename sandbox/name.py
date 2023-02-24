class Name:
    """
    Class Name
    Represent a name in fr/en
    """

    def __init__(self, fr=None, en=None):
        if (fr is not None) != (en is not None):
            if fr is None:
                self.en = en
                self.fr = translate(en)
            else:
                self.fr = fr
                self.en = translate(fr)

    def __str__(self):
        return self.en

    def __repr__(self):
        return f'Name: {self.fr} (fr) / {self.en} (en)'


en_fr_list = (('sadness', 'tristesse'),
              ('pain', 'douleur'),
              ('fever', 'fièvre'),
              ('sadness', 'tristesse'),
              ('agitation', 'agitation'),
              ('tiredness', 'fatigue'),
              ('walking', 'déplacement'))



def translate(word):
    en, fr = list(zip(*en_fr_list))
    found_word = None
    try:
        found_word = fr[en.index(word)]
    except ValueError as ve:
        pass
    try:
        found_word = en[fr.index(word)]
    except ValueError as ve:
        pass
    if found_word is not None:
        return found_word
    else:
        raise ValueError(f'No translation found for word {word}')



