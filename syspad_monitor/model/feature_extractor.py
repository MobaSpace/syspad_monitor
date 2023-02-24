import statistics


class FeatureExtractor:

    def __init__(self, lmt, umt, it, fs):
        self.__it = it
        self.__umt = umt
        self.__lmt = lmt
        self.__freq = fs
        self.__peaks = []
        self.__valleys = []
        self.__myAcc = []

    def getFeaturesOnString(self, acc: []) -> str:
        my_features = self.getFeatures(acc)
        out = ""
        out += "AAMV              = " + str(my_features[0]) + "\n"
        out += "ImpDuration       = " + str(my_features[1]) + "\n"
        out += "ImpPeakValue      = " + str(my_features[2]) + "\n"
        out += "ImpPeakDur        = " + str(my_features[3]) + "\n"
        out += "LongValleyVal     = " + str(my_features[4]) + "\n"
        out += "LongValleyDur     = " + str(my_features[5]) + "\n"
        out += "PeaksPriorImp     = " + str(my_features[6]) + "\n"
        out += "ValleysPriorImp   = " + str(my_features[7]) + "\n"
        out += "STDevAfterImp     = " + str(my_features[8]) + "\n"
        out += "AreaPeakDur       = " + str(my_features[9]) + "\n"
        out += "StepCounterBI     = " + str(my_features[10]) + "\n"
        out += "PrevValleyVal     = " + str(my_features[11]) + "\n"
        out += "PrevValleyDur     = " + str(my_features[12]) + "\n"
        return out

    def getFeatures(self, acc: []) -> []:
        self.__myAcc = acc
        self.__peaksIndexes()
        self.__valleysIndexes()

        features = []
        features.append(self.__getAverageAccVar())
        features.append(self.__getImpactDuration())
        features.append(self.__getImpactPeakValue())
        features.append(self.__getImpactPeakDuration())
        tmp = self.__getLongestValeeyValueAndDuration()
        features.append(tmp[0])
        features.append(tmp[1])
        features.append(len(self.__peaks) - 1)
        features.append(len(self.__valleys))
        features.append(self.__getSTD_ATI())
        features.append(self.__getArea())
        features.append(self.__getStepIndex())
        tmp = self.__getPrevValleyValueAndDuration()
        features.append(tmp[0])
        features.append(tmp[1])

        return features

    def __getStepIndex(self) -> int:
        # evaluation from window start to iniImpac - 200ms
        # freq is 50Hz (20ms) so 10 samples
        step_counter_index = 0
        index_ini_impact = self.__getIndexIniImpact()
        last_step = -999
        for ii in range(index_ini_impact - 10):
            is_valley = True
            for jj in range(ii, ii + 4):
                if self.__myAcc[jj] >= 1:
                    is_valley = False
                    break
            is_peak = False
            for jj in range(ii + 4, ii + 10):
                if self.__myAcc[jj] >= 1.6:
                    is_peak = True
                    break
            if is_peak and is_valley and ii > last_step + 10:
                step_counter_index += 1
                last_step = ii
        return step_counter_index

    def __getSTD_ATI(self) -> float:
        ini_eval = self.__getIndexEndImpact()
        tmp = []
        for ii in range(ini_eval, len(self.__myAcc)):
            tmp.append(self.__myAcc[ii])
        return statistics.stdev(tmp)

    def __getSTD_BFI(self) -> float:
        ini_eval = self.__getIndexEndImpact()
        tmp = []
        for ii in range(ini_eval):
            tmp.append(self.__myAcc[ii])
        return statistics.stdev(tmp)

    def __getArea(self) -> float:
        peak_center = (self.__getIndexIniImpact() + self.__getIndexEndImpact()) // 2
        ini_win = peak_center - int(self.__freq // 2)
        end_win = peak_center + int(self.__freq // 2)

        area = 0.0
        for ii in range(ini_win, end_win):
            area += self.__myAcc[ii]
        return area

    def __getIndexIniImpact(self) -> int:
        peak_index = self.__peaks[-1]
        index = peak_index - 1
        ii_range = range(peak_index - 1, -1, -1)
        for ii in ii_range:
            if self.__myAcc[ii] < self.__it:
                index = ii + 1
                break
        return index

    def __getIndexEndImpact(self) -> int:
        peak_index = self.__peaks[-1]
        index = peak_index + 1
        ii_range = range(peak_index + 1, len(self.__myAcc))
        for ii in ii_range:
            if self.__myAcc[ii] < self.__lmt or self.__myAcc[ii] > self.__umt:
                index = ii
        return index

    def __getAverageAccVar(self) -> float:
        peak_center = (self.__getIndexIniImpact() + self.__getIndexEndImpact()) // 2
        ini_win = peak_center - int(self.__freq // 2)
        end_win = peak_center + int(self.__freq // 2)
        aamv = 0.0
        for ii in range(ini_win, end_win + 1):
            aamv += abs(self.__myAcc[ii + 1] - self.__myAcc[ii])
        return aamv / self.__freq

    def __getImpactDuration(self):
        return (self.__getIndexEndImpact() - self.__getIndexIniImpact() + 1) / self.__freq * 1000.

    def __peaksIndexes_orig(self):
        for ii in range(1, len(self.__myAcc)):
            if (self.__myAcc[ii] > self.__it and
                    self.__myAcc[ii - 1] < self.__myAcc[ii] and
                    self.__myAcc[ii + 1] < self.__myAcc[ii]):
                self.__peaks.append(ii)

    def __peaksIndexes(self):
        ii = 1
        while ii < len(self.__myAcc):
            max_val = -999.9
            peak_index = -1
            while self.__myAcc[ii] > self.__it:
                if self.__myAcc[ii] > max_val:
                    max_val = self.__myAcc[ii]
                    peak_index = ii
                ii = ii + 1
                if ii == len(self.__myAcc):
                    break
            if peak_index >= 0:
                self.__peaks.append(peak_index)
            ii = ii + 1

    def __valleysIndexes(self):
        peak_index = self.__peaks[-1]
        ii = peak_index - 1
        while ii >= 0:
            # for ii in range(peak_index-1, -1, -1):
            min_val = 999.9
            valley_index = -1
            while self.__myAcc[ii] < self.__lmt:
                if self.__myAcc[ii] < min_val:
                    min_val = self.__myAcc[ii]
                    valley_index = ii
                ii = ii - 1
                if ii == 0:
                    break
            if valley_index >= 0:
                self.__valleys.append(valley_index)
            ii = ii - 1

    def __getImpactPeakValue(self) -> float:
        peak_index = self.__peaks[-1]
        return self.__myAcc[peak_index]

    def __getImpactPeakDuration(self) -> float:
        peak_index = self.__peaks[-1] - 1
        ini = peak_index
        for ii in range(peak_index, -1, -1):
            if self.__myAcc[ii] < self.__it:
                ini = ii + 1
                break

        peak_index = self.__peaks[-1] + 1
        end = peak_index
        for jj in range(peak_index, len(self.__myAcc)):
            if self.__myAcc[jj] < self.__it:
                end = jj - 1
                break
        return (end - ini + 1) / self.__freq * 1000

    def __getPrevValleyValueAndDuration(self) -> []:
        prevValley = [0.0, -9999]
        currentValleyIndex = self.__valleys[-1]
        ini = currentValleyIndex - 1
        for ii in range(currentValleyIndex - 1, -1, -1):
            if self.__myAcc[ii] > self.__lmt:
                ini = ii + 1
                break
        end = currentValleyIndex + 1
        for jj in range(currentValleyIndex + 1, len(self.__myAcc)):
            if self.__myAcc[jj] > self.__lmt:
                end = jj - 1
                break

        currenValleyDuration = (end - ini + 1) / self.__freq * 1000.
        if currenValleyDuration > prevValley[1]:
            prevValley[0] = self.__myAcc[currentValleyIndex]
            prevValley[1] = currenValleyDuration
        return prevValley

    def __getLongestValeeyValueAndDuration(self) -> []:
        longestValley = [0.0, -9999]
        for vv in range(0, len(self.__valleys)):
            currentValleyIndex = self.__valleys[vv]
            ini = currentValleyIndex - 1
            for ii in range(currentValleyIndex - 1, -1, -1):
                if self.__myAcc[ii] > self.__lmt:
                    ini = ii + 1
                    break
            end = currentValleyIndex + 1
            for jj in range(currentValleyIndex + 1, len(self.__myAcc)):
                if self.__myAcc[jj] > self.__lmt:
                    end = jj - 1
                    break

            currenValleyDuration = (end - ini + 1) / self.__freq * 1000.
            if currenValleyDuration > longestValley[1]:
                longestValley[0] = self.__myAcc[currentValleyIndex]
                longestValley[1] = currenValleyDuration

        return longestValley
