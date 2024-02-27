from mongoengine import EmbeddedDocument, Document
from mongoengine.fields import StringField, BooleanField, EmbeddedDocumentField
import faker
import connect
import pika
import sys
import json


# Models
class FullName(EmbeddedDocument):
    fullname = StringField()


class EmailAddress(EmbeddedDocument):
    email_address = StringField()


class EmailBody(EmbeddedDocument):
    email_body = StringField()


class Contacts(Document):
    fullname = EmbeddedDocumentField(FullName)
    email_address = EmbeddedDocumentField(EmailAddress)
    email_body = EmbeddedDocumentField(EmailBody)
    is_sent = BooleanField(default=False)


NUMBER_OF_EMAILS = 100
fake_data = faker.Faker()


def generate_fake_data():
    for _ in range(NUMBER_OF_EMAILS):
        fullname = FullName(fullname=fake_data.name())
        email_address = EmailAddress(email_address=fake_data.email())
        email_body = EmailBody(email_body=fake_data.text())
        Contacts(fullname=fullname, email_address=email_address, email_body=email_body, is_sent=False).save()


if __name__ == "__main__":
    generate_fake_data()

    # Отримуємо перелік ObjectId з MongoBD
    contacts = Contacts.objects()
    ids = contacts.distinct('_id')

    # Кладемо повідомлення в чергу RabbitMQ
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.exchange_declare(exchange='task_mock', exchange_type='direct')
    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_bind(exchange='task_mock', queue='task_queue')


    def send_to_rabbitmq():
        for id in ids:
            channel.basic_publish(
                exchange='task_mock',
                routing_key='task_queue',
                body=json.dumps(str(id)).encode(),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ))
            print(f'Id {id} sent to RabbitMQ')
        connection.close()

    send_to_rabbitmq()
