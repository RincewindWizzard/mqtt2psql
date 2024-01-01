import toml
import psycopg2
from datetime import datetime
import paho.mqtt.client as mqtt
from loguru import logger


def run_mqtt(host='localhost', port=1883, username=None, password=None, on_message=lambda x, y, z: None):
    def on_connect(client: mqtt.Client, userdata, flags, rc):
        logger.debug(f'Connected to mqtt://{host}/ with rc = {rc}')
        if not rc == 0:
            raise IOError(f"RC is {rc}")
        client.subscribe("#")

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(username, password)
    client.connect(
        host,
        port=port
    )
    client.loop_forever()


def main():
    with open('mqtt2psql.toml') as f:
        config = toml.load(f)

        with psycopg2.connect(**config['postgres']) as connection:
            logger.debug(f'Connected to postgres://{config["postgres"]["host"]}')
            with connection.cursor() as cursor:
                def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
                    # timestamp = datetime.utcfromtimestamp(msg.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f %z')
                    # print(f"[{msg.timestamp} = {timestamp}] {msg.topic}: {msg.payload.decode()}")
                    cursor.execute(
                        "INSERT INTO public.mqtt (id, timestamp, topic, message) VALUES (DEFAULT, %s, %s, %s)",
                        (
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f %z'),
                            msg.topic,
                            msg.payload.decode()
                        )
                    )
                    connection.commit()

                run_mqtt(**config['mqtt'], on_message=on_message)


if __name__ == '__main__':
    main()
