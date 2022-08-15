import inject
import paho.mqtt.client as mqtt
import rospy

from .bridge import create_bridge
from .mqtt_client import create_private_path_extractor
from .util import lookup_object


bridges = []


def create_config(mqtt_client, serializer, deserializer, mqtt_private_path):
    if isinstance(serializer, str):
        serializer = lookup_object(serializer)
    if isinstance(deserializer, str):
        deserializer = lookup_object(deserializer)
    private_path_extractor = create_private_path_extractor(mqtt_private_path)

    def config(binder):
        binder.bind("serializer", serializer)
        binder.bind("deserializer", deserializer)
        binder.bind(mqtt.Client, mqtt_client)
        binder.bind("mqtt_private_path_extractor", private_path_extractor)

    return config


def mqtt_bridge_node():
    global bridges

    # init node
    rospy.init_node("mqtt_bridge_node")

    # load parameters
    params = rospy.get_param("~", {})
    mqtt_params = params.pop("mqtt", {})
    conn_params = mqtt_params.pop("connection")
    mqtt_private_path = mqtt_params.pop("private_path", "")

    # create mqtt client
    mqtt_client_factory_name = rospy.get_param(
        "~mqtt_client_factory", ".mqtt_client:default_mqtt_client_factory"
    )
    mqtt_client_factory = lookup_object(mqtt_client_factory_name)
    mqtt_client = mqtt_client_factory(mqtt_params)

    # load serializer and deserializer
    serializer = params.get("serializer", "msgpack:dumps")
    deserializer = params.get("deserializer", "msgpack:loads")

    # dependency injection
    config = create_config(mqtt_client, serializer, deserializer, mqtt_private_path)
    inject.configure(config)

    # configure and connect to MQTT broker
    mqtt_client.on_connect = _on_connect
    mqtt_client.on_disconnect = _on_disconnect
    mqtt_client.connect(**conn_params)
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=2)

    # start MQTT loop
    mqtt_client.loop_start()

    # register shutdown callback and spin
    rospy.on_shutdown(mqtt_client.disconnect)
    rospy.on_shutdown(mqtt_client.loop_stop)
    rospy.spin()


def subscribe():
    global bridges
    rospy.loginfo("Subscribing to bridges")
    bridges = []
    params = rospy.get_param("~", {})
    bridge_params = params.get("bridge", [])
    for bridge_args in bridge_params:
        bridges.append(create_bridge(**bridge_args))


def _on_connect(client, userdata, flags, response_code):
    rospy.loginfo("MQTT connected")
    subscribe()


def _on_disconnect(client, userdata, response_code):
    rospy.loginfo("MQTT disconnected")


__all__ = ["mqtt_bridge_node"]
