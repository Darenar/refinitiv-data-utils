from typing import List, Union
import json

from itertools import chain


class ESGTaxonomy(dict):
    @classmethod
    def from_json(cls, path_to_json: str) -> 'ESGTaxonomy':
        with open(path_to_json, 'r') as f:
            return cls(json.load(f))

    def get_pillars(self) -> List[str]:
        return list(self)
    
    def get_categories(self, pillars: Union[str, List[str]] = None) -> List[str]:
        pillars = self.get_pillars() if pillars is None else pillars
        if not isinstance(pillars, list):
            pillars = [pillars]
        return list(chain(*[list(self[p]) for p in pillars]))
    
    def get_scores(self, pillars: Union[str, List[str]] = None, 
                   categories: Union[str, List[str]] = None, 
                   filter_controversy_value: str = None) -> List[str]:
        if filter_controversy_value is None:
            filter_controversy_value = ['Y', 'N']
        if not isinstance(filter_controversy_value, list):
            filter_controversy_value = [filter_controversy_value]

        pillars = self.get_pillars() if pillars is None else pillars
        if not isinstance(pillars, list):
            pillars = [pillars]

        categories = self.get_categories() if categories is None else categories
        if not isinstance(categories, list):
            categories = [categories]
        
        scores = list()
        for p in pillars:
            for c in categories:
                if c in self[p]:
                    for score in self[p][c]:
                        if self[p][c][score]['controversy'] in filter_controversy_value:
                            scores.append(score)
        return scores
