from models import *
import connect
import sys


# Встановлюємо кодування виводу на utf-8
sys.stdout.reconfigure(encoding='utf-8')

quotes = Quotes.objects()

while True:
    prompt = input('>')

    if prompt == 'exit':
        break

    command, value = prompt.split(':')

    if command.strip() == 'name':
        for quote in quotes:
            if value.strip() == quote.author.author.fullname.fullname:
                print(quote.quote.quote)
    elif command.strip() == 'tag':
        for quote in quotes:
            if value.strip() in quote.tags.tags:
                print(quote.quote.quote)
    elif command.strip() == 'tags':
        tags_list = [v.strip() for v in value.split(',')]
        for quote in quotes:
            for tag in tags_list:
                if tag in quote.tags.tags:
                    print(quote.quote.quote)
