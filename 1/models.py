from mongoengine import EmbeddedDocument, Document
from mongoengine.fields import EmbeddedDocumentField, ListField, StringField, ReferenceField, ObjectIdField


# Author Classes
class FullName(EmbeddedDocument):
    fullname = StringField()


class BornDate(EmbeddedDocument):
    born_date = StringField()


class BornLocation(EmbeddedDocument):
    born_location = StringField()


class Description(EmbeddedDocument):
    description = StringField()


class Authors(Document):
    fullname = EmbeddedDocumentField(FullName)
    born_date = EmbeddedDocumentField(BornDate)
    born_location = EmbeddedDocumentField(BornLocation)
    description = EmbeddedDocumentField(Description)

    author_id = ObjectIdField()


# Quotes Classes
class Tags(EmbeddedDocument):
    tags = ListField()


class AuthorRef(EmbeddedDocument):
    author = ReferenceField(Authors)


class QuoteText(EmbeddedDocument):
    quote = StringField()


class Quotes(Document):
    tags = EmbeddedDocumentField(Tags)
    author = EmbeddedDocumentField(AuthorRef)
    quote = EmbeddedDocumentField(QuoteText)
