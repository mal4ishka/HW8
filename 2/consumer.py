from producer import Contacts
import pika
import json
from bson import ObjectId

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)

contacts = Contacts.objects()

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    message = json.loads(body.decode())
    print(f" [x] Received {message}")

    object_id = ObjectId(message)
    row = contacts.get(id=object_id)

    fullname = row.fullname.fullname
    email_address = row.email_address.email_address

    # Встановлюємо is_sent=True
    contacts.filter(id=object_id).update_one(set__is_sent=True)
    print(f'Fullname: {fullname}, Email address: {email_address}, message sent')

    print(f" [x] Done: {method.delivery_tag}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)


if __name__ == '__main__':
    channel.start_consuming()
