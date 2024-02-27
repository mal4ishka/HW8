from models import *
import connect
import json

with open('authors.json', 'r', encoding='utf-8') as file:
    authors_json = json.load(file)

with open('quotes.json', 'r', encoding='utf-8') as file:
    quotes_json = json.load(file)

authors_dict = {}

for author_json in authors_json:
    fullname = FullName(fullname=author_json['fullname'])
    born_date = BornDate(born_date=author_json['born_date'])
    born_location = BornLocation(born_location=author_json['born_location'])
    description = Description(description=author_json['description'])

    authors = Authors(fullname=fullname, born_date=born_date, born_location=born_location, description=description)
    authors.save()

    authors_dict[authors.fullname.fullname] = authors

for quote_json in quotes_json:
    tags = Tags(tags=quote_json['tags'])

    author = authors_dict.get(quote_json['author'])
    author = AuthorRef(author=author)

    quote_text = QuoteText(quote=quote_json['quote'])

    Quotes(tags=tags, author=author, quote=quote_text).save()
