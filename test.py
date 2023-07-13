"""
# Project:
# Author: Eddie
# Date: 
"""
import re
def extract_rating_number(string):
    rating = re.search(r"\d+", string).group()
    return int(rating)

a = "klantbeoordeling: 6 van de 5 sterren"
s = extract_rating_number(a)

print(s)