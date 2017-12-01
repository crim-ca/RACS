# coding: utf-8

class Interval:

    def __init__(self,begin,end, openBegin=False,openEnd=False,isFullyInclusif=True):
        """
        Creates a new interval. Begin must be < end.
        Example:
        open,open = (a,b) = {a < x < b}
        close,close = [a,b] = {a <= x <= b}
        Here some example for better understanding:
        isFullyInclusif = true, Interval (3,6). The following itervals would be valid to fit inside this interval:
        (3,6),[2,5],(3,4] but the following will not [3,6], (2,5)
        isFullyInclusif = False Interval (3,6). Main would consider edges. The following will fit:
        (3,6), (2,5), but the following will not (2,3). An intersting example would be that for an interval of
        bounds [1,4], the interval [4,5] would be valid.


        :param begin:   start of the interval
        :param end:     end of the interval.
        :param isFullInclusif:  If true, elements should be completly included in the interval.
        :param openBegin: If true, the begin part should be considered Open by mathematical definition.
        :param openEnd:   If true, the end part should be considered Open by mathematical definition.
        """
        self.begin = begin
        self.end = end
        self.openBegin = openBegin
        self.openEnd = openEnd
        self.isFullyInclusif = isFullyInclusif