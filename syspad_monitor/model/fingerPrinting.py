import pandas as pd
import numpy as np
import json

from sklearn.neighbors import KNeighborsClassifier as kNN

class loc_fingerprinting:
    """
    Cette classe permet de calculer la localisation par la méthode KNN
    a partir d'une BD construite au préalable
    """
    def __init__(self, loc_db: str, label_pos:str, list_restricted_nets=None):
        #self.__dataset = pd.read_csv("mean_positions.csv",header = [0,1,2])
        dataset = pd.read_csv(loc_db,header = [0,1,2])
        features = np.asarray(dataset.iloc[:,3:])
        posIndex = np.asarray(dataset["Relative Position"].transpose())[0]
        self.__networks = [z for (_,_,z) in list(dataset)[3:]]
        if list_restricted_nets:
            # If we want only selected wifis in dataset:
            # network_names = ['ca-access-point','node14ap','node19ap','node20ap']
            self.__networks = [z for (x,_,z) in list(dataset)[3:] if x in list_restricted_nets]
            features = np.asarray(dataset[list_restricted_nets])

        self.__clf = kNN(n_neighbors=2)
        self.__clf.fit(features, posIndex)

        with open(label_pos) as json_file:
            self.__labels = json.load(json_file)

    def perform_loc(self, wifi_list:[]) -> {}:
        # form of the wifi_list
        # [{'bssid': '00:0b:6b:de:ea:36', 'frequency': '2437',
        # 'signal level': '-37', 'flags': '[WPA-PSK-TKIP][WPA2-PSK-TKIP][ESS]',
        # 'ssid': 'node14ap', 'distance': '0.858'},

        # Current networks found in scan
        # Must be of length and order of original networks in dataset
        found_networks = [0]*len(self.__networks)
        cells = wifi_list

        for i in range(len(found_networks)): found_networks[i] = 100
        for cell in cells:
            mac = cell['bssid']
            if mac not in self.__networks: continue
            rssi = cell['signal level']
            found_networks[self.__networks.index(mac)] = rssi

        position = self.__clf.predict([found_networks])[0]
        pos = self.__labels[str(position)]
        print("Position: {}".format(position))
        print(f"Label: {pos['Label']}")
        return pos
