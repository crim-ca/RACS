import re

class LanguageNotSupported(Exception):
    pass

class LanguageManager:
    """
    Manages languages by making sure there is an associated analyser.
    """
    def __init__(self,es_analyser_by_language:dict):
        """
        :param es_analyser_by_language: Dictionary which maps a custom language field to an
            es analyser. wildcards can be used at the end of language. Example en_* will match
            with en_EN and en_UK and en_whatever.
        """
        self.es_analyser_by_language_static = {}
        self.es_analyser_by_language_regex = {}
        for language in es_analyser_by_language.keys():
            if "*" in language:
                reg = language.replace("*","(.*)")
                self.es_analyser_by_language_regex[reg] = es_analyser_by_language[language]
            else:
                self.es_analyser_by_language_static[language] = es_analyser_by_language[language]

    def has_es_analyser(self,language):
        """
        Returns true if the language passed has an analyser.

        :param language:
        :return:
        """

        if language in self.es_analyser_by_language_static:
            return True

        for reg in self.es_analyser_by_language_regex.keys():
            if re.match(reg,language):
                return True

        return False

    def get_es_analyser(self,language):
        """
        returns es analyser associated with the language

        :param language:
        :return:
        """
        if language in self.es_analyser_by_language_static:
            return self.es_analyser_by_language_static[language]

        for reg in self.es_analyser_by_language_regex.keys():
            if re.match(reg, language):
                return self.es_analyser_by_language_regex[reg]

        raise LanguageNotSupported(language)